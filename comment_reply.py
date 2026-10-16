"""B站视频评论区自动回复：轮询『收到评论』消息流 + 限速回复（统一由Y账号发出）

实现参考 bilibili-API-collect（msgfeed/reply、reply/add、wbi签名文档）与
BiliCommentBot、bilibili-ai-bot 等成熟同类项目的防风控实践：
首轮轮询只建立基准不回复、已读位置持久化（跨重启不重复回复）、每轮限量、
发送限速+随机延迟、忽略本插件两个账号自身的消息、回复失败不重试。

v3.0.0 新增：X账号『仅回复自己视频』过滤（owner/staff 判定）、tag 人设路由
（Route 数据类，由宿主注入的 resolve_route 判定）、回复中@指定用户
（at_name_to_mid 拼装 + 发送后校验）。
"""
import asyncio
import hashlib
import json
import random
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path

import aiohttp

from astrbot.api import logger

from .blivedm.clients.ws_base import USER_AGENT

NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
REPLY_FEED_URL = "https://api.bilibili.com/x/msgfeed/reply"
REPLY_ADD_URL = "https://api.bilibili.com/x/v2/reply/add"
# at 目标昵称解析用接口（按序尝试，任一成功即用）：
# - 空间详情（现行官方地址；旧地址 x/space/acc/info 已被B站废弃，稳定返回-799）
SPACE_ACC_INFO_URL = "https://api.bilibili.com/x/space/wbi/acc/info"
# - 用户名片（兜底；实测无需登录即可返回昵称，容错性最好）
CARD_INFO_URL = "https://api.bilibili.com/x/web-interface/card"

# wbi 混合密钥索引表（bilibili-API-collect wbi.md，全表64项取前32项即可）
MIXIN_KEY_ENC_TAB = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3,
    45, 35, 27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39,
    12, 38, 41, 13,
]

# wbi 密钥按日更替，缓存12小时
WBI_KEY_TTL = 12 * 3600

# 每轮轮询拉取的通知条数（只关心最新一页）
FEED_PAGE_SIZE = 20

# 评论回复队列上限
REPLY_QUEUE_MAX = 100

# 频率/风控类错误码（BiliCommentBot 实战汇总），触发时应调大发送间隔
RATE_LIMIT_CODES = {-412, -509, 412, 509, 799, 10403}

# at 目标昵称缓存TTL（秒）：mid→昵称落 comment_state.json，跨重启复用
AT_UNAME_TTL = 24 * 3600

# at 昵称查询失败后的冷却时长（秒）：冷却期内不再请求B站（防限频恶化与日志
# 刷屏），到期后随新评论自然重试
AT_UNAME_FAIL_COOLDOWN = 5 * 60


def _parse_cookie_str(cookie_str: str) -> dict:
    cookies = {}
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            key, value = item.split("=", 1)
            cookies[key.strip()] = value.strip()
    return cookies


@dataclass
class Route:
    """一条人设路由的运行时形态（tag 路由或默认路由）。

    at_uname 由 manager 在路由判定后（D7）解析并挂载：为空且 at_enable
    为真时视为本轮 at 关闭。Route 实例必须每次判定新建（宿主的 resolve_route
    保证），禁止跨评论共享，否则 at_uname 会污染后续判定并在双轮询并发下竞态。
    """

    name: str
    persona: str
    at_enable: bool
    at_mid: str
    at_position: str          # "prefix" | "suffix"
    at_guide: str
    at_uname: str = ""        # 由 manager 在路由判定后填充


class BiliCommentClient:
    """单个B站账号的评论区客户端：维护wbi密钥、轮询收到评论、发布回复

    :param label: 账号标签（"X"/"Y"），仅用于日志区分
    :param get_cookie: 获取当前cookie字符串的回调，每个请求实时调用，
        Cookie自动刷新写回配置后无需重建客户端即可生效
    """

    def __init__(self, label: str, get_cookie):
        self.label = label
        self._get_cookie = get_cookie
        self._session: aiohttp.ClientSession | None = None
        self._mixin_key = ""
        self._mixin_key_fetched_at = 0.0
        self.mid = ""
        self.uname = ""

    async def start(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )

    async def stop(self):
        if self._session:
            await self._session.close()
            self._session = None

    def _headers(self) -> dict:
        return {
            "User-Agent": USER_AGENT,
            "Cookie": self._get_cookie(),
            "Referer": "https://www.bilibili.com/",
        }

    async def ensure_identity(self) -> bool:
        """校验登录态并记录账号mid/昵称（nav接口），失败返回False"""
        async with self._session.get(NAV_URL, headers=self._headers()) as resp:
            result = await resp.json()
        if result.get("code") != 0:
            logger.error(
                f"[{self.label}] 登录态校验失败: code={result.get('code')}, "
                f"message={result.get('message')}，请检查该账号的Cookie"
            )
            return False
        data = result.get("data") or {}
        self.mid = str(data.get("mid") or "")
        self.uname = data.get("uname") or ""
        if not self.mid:
            logger.error(f"[{self.label}] 登录态校验失败: 响应缺少账号mid")
            return False
        return True

    async def signed_params(self, params: dict) -> dict | None:
        """对请求参数做 wbi 签名（供宿主的元数据层等复用），密钥获取失败返回 None"""
        if not await self._ensure_mixin_key():
            return None
        return self._wbi_sign(params)

    async def _ensure_mixin_key(self) -> bool:
        """获取并缓存wbi混合密钥（nav的wbi_img，未登录也会返回）"""
        if self._mixin_key and time.time() - self._mixin_key_fetched_at < WBI_KEY_TTL:
            return True
        try:
            async with self._session.get(
                NAV_URL, headers={"User-Agent": USER_AGENT}
            ) as resp:
                result = await resp.json()
            wbi_img = result["data"]["wbi_img"]
            img_key = wbi_img["img_url"].rpartition("/")[2].partition(".")[0]
            sub_key = wbi_img["sub_url"].rpartition("/")[2].partition(".")[0]
        except Exception as e:
            logger.warning(f"[{self.label}] 获取wbi密钥失败: {e}")
            return False
        raw_key = img_key + sub_key
        self._mixin_key = "".join(
            raw_key[i] for i in MIXIN_KEY_ENC_TAB if i < len(raw_key)
        )
        self._mixin_key_fetched_at = time.time()
        return True

    def _reset_mixin_key(self):
        self._mixin_key = ""
        self._mixin_key_fetched_at = 0.0

    def _wbi_sign(self, params: dict) -> dict:
        """wbi签名：加wts→按key升序→值中删除 !'()* →urlencode→md5(query+mixin_key)"""
        params = {**params, "wts": str(int(time.time()))}
        params = {
            key: "".join(ch for ch in str(value) if ch not in "!'()*")
            for key, value in params.items()
        }
        query = urllib.parse.urlencode(sorted(params.items()))
        params["w_rid"] = hashlib.md5(
            (query + self._mixin_key).encode("utf-8")
        ).hexdigest()
        return params

    async def fetch_reply_feed(self) -> list[dict]:
        """轮询『收到评论』消息流（最新一页，新→旧），失败返回空列表"""
        if not await self._ensure_mixin_key():
            return []
        params = self._wbi_sign(
            {"platform": "web", "web_location": "1315875", "ps": str(FEED_PAGE_SIZE)}
        )
        try:
            async with self._session.get(
                REPLY_FEED_URL, params=params, headers=self._headers()
            ) as resp:
                result = await resp.json()
        except Exception as e:
            logger.warning(f"[{self.label}] 收到评论轮询请求异常: {e}")
            return []
        # v_voucher 是wbi缺失/被风控的标志：重置密钥重试一次
        if result.get("v_voucher") or result.get("code") == -403:
            logger.debug(f"[{self.label}] 收到评论轮询遭遇风控校验，重置wbi密钥重试")
            self._reset_mixin_key()
            if await self._ensure_mixin_key():
                params = self._wbi_sign(
                    {"platform": "web", "web_location": "1315875", "ps": str(FEED_PAGE_SIZE)}
                )
                try:
                    async with self._session.get(
                        REPLY_FEED_URL, params=params, headers=self._headers()
                    ) as resp:
                        result = await resp.json()
                except Exception as e:
                    logger.warning(f"[{self.label}] 收到评论轮询重试请求异常: {e}")
                    return []
        if result.get("v_voucher") or result.get("code") == -403:
            logger.warning(f"[{self.label}] 收到评论轮询被风控，本轮跳过")
            return []
        code = result.get("code")
        if code == 0:
            return (result.get("data") or {}).get("items") or []
        if code == -101:
            logger.error(f"[{self.label}] 收到评论轮询失败: Cookie已失效(code=-101)，请重新获取")
        else:
            logger.warning(
                f"[{self.label}] 收到评论轮询失败: code={code}, message={result.get('message')}"
            )
        return []

    async def resolve_uname(self, mid: str) -> str | None:
        """查询用户昵称（供 at 目标解析；wbi签名+Cookie头）。按序尝试两个接口，
        任一成功即返回；全部失败返回None，空白昵称视为失败。接口与字段路径：
        1. space/wbi/acc/info 取 data.name
        2. web-interface/card 取 data.card.name（兜底）
        注：旧接口 x/space/acc/info 已被B站废弃（对旧路径稳定返回 -799
        「请求过于频繁」，实测复现），不再使用。
        缓存由 CommentReplyManager 负责（纯查询）"""
        if not await self._ensure_mixin_key():
            return None
        headers = {
            **self._headers(),
            "Referer": f"https://space.bilibili.com/{mid}",
        }
        # (日志用接口名, URL, 附加参数, 昵称字段在响应中的路径)
        attempts = (
            (
                "space/wbi/acc/info",
                SPACE_ACC_INFO_URL,
                {"mid": mid, "token": "", "platform": "web", "web_location": "333.999"},
                ("data", "name"),
            ),
            (
                "web-interface/card",
                CARD_INFO_URL,
                {"mid": mid, "photo": "false"},
                ("data", "card", "name"),
            ),
        )
        failures: list[str] = []
        for api_name, url, params, name_path in attempts:
            try:
                async with self._session.get(
                    url, params=self._wbi_sign(params), headers=headers
                ) as resp:
                    result = await resp.json()
            except Exception as e:
                failures.append(f"{api_name}: {e}")
                continue
            if result.get("code") != 0:
                failures.append(
                    f"{api_name}: code={result.get('code')}, "
                    f"message={result.get('message')}"
                )
                continue
            node = result
            for key in name_path:
                node = node.get(key) if isinstance(node, dict) else None
            name = str(node or "").strip()
            if name:
                if failures:
                    logger.debug(
                        f"[{self.label}] at昵称查询兜底成功(mid={mid})，"
                        f"前置接口失败原因: {'；'.join(failures)}"
                    )
                return name
            failures.append(f"{api_name}: 响应缺少昵称字段")
        logger.warning(
            f"[{self.label}] 查询用户昵称失败(mid={mid}): " + "；".join(failures)
        )
        return None

    async def send_reply(
        self,
        oid: str,
        root: str,
        parent: str,
        message: str,
        reply_type: str = "1",
        at_map: dict[str, int] | None = None,
    ) -> tuple[bool, bool]:
        """发布一条视频评论回复（POST reply/add，仅需Cookie+CSRF，无需wbi）。
        返回（发送成功, at验证通过）；无 at_map 时第二返回值恒 True。

        at_map: {"昵称": mid}，非空时 POST 体携带 at_name_to_mid（JSON字符串），
        且 message 文本中的 `@昵称 ` 须与映射键字面一致（纯文本@无效、不触发
        通知，已查证）。发送成功后校验响应 data.reply.content 的 at 映射 /
        members 是否含目标 mid；未生效返回 False（昵称变更/风控吞at，不重试）"""
        csrf = _parse_cookie_str(self._get_cookie()).get("bili_jct", "")
        if not csrf:
            logger.error(f"[{self.label}] 缺少bili_jct，无法发送评论回复")
            return False, True
        data = {
            "type": reply_type,
            "oid": oid,
            "root": root,
            "parent": parent,
            "message": message,
            "csrf": csrf,
            "csrf_token": csrf,
            "plat": "1",
        }
        if at_map:
            data["at_name_to_mid"] = json.dumps(at_map, ensure_ascii=False)
        headers = {
            **self._headers(),
            "Origin": "https://www.bilibili.com",
            "Referer": f"https://www.bilibili.com/video/av{oid}/",
        }
        try:
            async with self._session.post(
                REPLY_ADD_URL, data=data, headers=headers
            ) as resp:
                result = await resp.json()
        except Exception as e:
            logger.error(f"[{self.label}] 评论回复请求异常: {e}")
            return False, True
        code = result.get("code")
        if code == 0:
            if at_map and not self._verify_at_result(result, at_map):
                content = (
                    ((result.get("data") or {}).get("reply") or {}).get("content")
                    or {}
                )
                logger.warning(
                    f"[{self.label}] 评论at未生效: "
                    f"at_name_to_mid={content.get('at_name_to_mid')}, "
                    f"members={[m.get('mid') for m in (content.get('members') or []) if isinstance(m, dict)]}"
                )
                return True, False
            return True, True
        if code in RATE_LIMIT_CODES or code == 12015:
            logger.warning(
                f"[{self.label}] 评论回复触发风控: code={code}, "
                f"message={result.get('message')}，建议调大发送间隔"
            )
        elif code == -101:
            logger.error(f"[{self.label}] 评论回复失败: Cookie已失效(code=-101)，请重新获取")
        elif code == -111:
            logger.error(f"[{self.label}] 评论回复失败: CSRF校验失败(code=-111)，请检查bili_jct")
        else:
            logger.warning(
                f"[{self.label}] 评论回复失败: code={code}, message={result.get('message')}"
            )
        return False, True

    @staticmethod
    def _verify_at_result(result: dict, at_map: dict[str, int]) -> bool:
        """校验 reply/add 响应中 at 是否真正生效：at_name_to_mid 为主，members 兜底"""
        content = ((result.get("data") or {}).get("reply") or {}).get("content") or {}
        mapping = content.get("at_name_to_mid")
        if isinstance(mapping, dict):
            mids = {str(v) for v in mapping.values() if v is not None}
            if any(str(target) in mids for target in at_map.values()):
                return True
        members = content.get("members")
        if isinstance(members, list):
            mids = {
                str(m.get("mid"))
                for m in members
                if isinstance(m, dict) and m.get("mid") is not None
            }
            if any(str(target) in mids for target in at_map.values()):
                return True
        return False


class CommentReplyManager:
    """视频评论区自动回复编排器

    - X账号（可选）：仅轮询『收到评论』，新评论转发进统一回复队列
    - Y账号：轮询自己的『收到评论』，并作为唯一发送账号回复两个账号的新评论
    """

    def __init__(
        self,
        y_client: BiliCommentClient,
        x_client: BiliCommentClient | None,
        state_path: Path,
        poll_interval: float,
        min_interval: float,
        random_delay_max: float,
        max_replies_per_cycle: int,
        max_length: int,
        max_reply_depth: int,
        context_max_chars: int,
        on_reply_needed,
        get_video_context=None,
        poll_gate=None,
        x_owner_only=False,
        get_video_meta=None,
        resolve_route=None,
        has_routes=False,
    ):
        """
        :param on_reply_needed: async (account_label, prompt_text, oid, root_id, route)
            -> tuple[str | None, bool]，由宿主完成LLM回复生成（人设取自route），
            返回（正文，是否at）；正文None涵盖"不回复"与"被拦截取消"两种情形，
            manager 无需区分。root_id为评论所在楼层的根评论id（"0"表示对视频的
            直接评论），供宿主按楼层维护上下文
        :param get_video_context: async (oid) -> str | None，由宿主识别评论所属视频
            内容（【当前视频信息】文本），置于prompt最前；None表示不识别
        :param poll_gate: 可选的同步门控 callable，返回False时跳过本轮轮询
            （如宿主LLM熔断中：不请求B站、不推进已读位置，恢复后自然补回）
        :param x_owner_only: 仅回复X账号自己发布的视频（含联合投稿中X为staff）
            下的评论；元数据获取失败时X账号通知fail-closed跳过（宁缺毋滥）
        :param get_video_meta: async (oid) -> dict | None，元数据层入口
            （owner_mid/staff_mids/tags），供owner过滤与tag路由判定；None表示不可用
        :param resolve_route: (tags: list[str] | None) -> Route，宿主注入的纯同步
            路由判定（默认路由也在其中返回）；每次调用须返回新建的 Route 实例
        :param has_routes: 路由表是否非空（惰性元数据获取的条件之一：
            仅当 X账号+owner过滤 或 路由表非空 时才请求元数据）
        """
        self._y_client = y_client
        self._x_client = x_client
        self._state_path = state_path
        self._poll_interval = poll_interval
        self._min_interval = min_interval
        self._random_delay_max = random_delay_max
        self._max_replies_per_cycle = max_replies_per_cycle
        self._max_length = max_length
        self._max_reply_depth = max_reply_depth
        self._context_max_chars = context_max_chars
        self._on_reply_needed = on_reply_needed
        self._get_video_context = get_video_context
        self._poll_gate = poll_gate
        self._x_owner_only = x_owner_only
        self._get_video_meta = get_video_meta
        self._resolve_route = resolve_route
        self._has_routes = has_routes
        self._tasks: list[asyncio.Task] = []
        self._reply_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=REPLY_QUEUE_MAX)
        self._own_mids: set[str] = set()
        # 账号标签→mid（owner 过滤需要单独取X账号的mid）
        self._account_mids: dict[str, str] = {}
        self._state = self._load_state()
        # at昵称解析串行化锁：防X/Y双轮询并发对同一mid重复查询/重复写缓存
        self._at_uname_lock = asyncio.Lock()

    @property
    def y_client(self) -> BiliCommentClient:
        """Y账号客户端（宿主注入元数据层 wbi 签名等用途）"""
        return self._y_client

    async def start(self):
        """启动轮询与回复任务。Y账号登录态必需，X账号登录态失败仅禁用X轮询"""
        await self._y_client.start()
        if not await self._y_client.ensure_identity():
            logger.error(
                "[Y] 登录态校验失败，视频评论区自动回复功能不可用"
                "（回复统一依赖Y账号，请检查Y账号Cookie）"
            )
            await self._y_client.stop()
            return
        logger.info(f"视频评论区回复使用Y账号: {self._y_client.uname}({self._y_client.mid})")
        self._own_mids.add(self._y_client.mid)
        self._account_mids["Y"] = self._y_client.mid
        if self._x_client:
            await self._x_client.start()
            if await self._x_client.ensure_identity():
                logger.info(
                    f"X账号轮询已启用: {self._x_client.uname}({self._x_client.mid})，"
                    "其收到的新评论将转发给Y账号统一回复"
                )
                self._own_mids.add(self._x_client.mid)
                self._account_mids["X"] = self._x_client.mid
                self._tasks.append(asyncio.create_task(self._poll_loop(self._x_client)))
            else:
                logger.error("[X] 登录态校验失败，X账号轮询已禁用（不影响Y账号）")
                await self._x_client.stop()
                self._x_client = None
        self._tasks.append(asyncio.create_task(self._poll_loop(self._y_client)))
        self._tasks.append(asyncio.create_task(self._reply_worker()))

    async def stop(self):
        """停止所有任务并释放资源"""
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await asyncio.wait_for(task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
            except Exception as e:
                logger.warning(f"停止评论区任务时出现异常: {e}")
        self._tasks.clear()
        await self._y_client.stop()
        if self._x_client:
            await self._x_client.stop()

    async def _poll_loop(self, client: BiliCommentClient):
        # 启动即轮询一轮，之后按间隔周期轮询（外加少量随机抖动）
        while True:
            try:
                if self._poll_gate is not None and not self._poll_gate():
                    logger.debug(
                        f"[{client.label}] 轮询门控关闭（如LLM熔断中），本轮跳过"
                    )
                else:
                    await self._poll_once(client)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[{client.label}] 收到评论处理异常: {e}")
            await asyncio.sleep(self._poll_interval + random.uniform(0, 15))

    async def _poll_once(self, client: BiliCommentClient):
        items = await client.fetch_reply_feed()
        if not items:
            return
        account = client.label
        newest_id = str(items[0].get("id") or "")
        last_id = self._state.get("last_id", {}).get(account)
        if last_id is None or not newest_id:
            # 首次轮询建立基准：只记录位置不回复，避免把历史通知翻出来批量回复
            if newest_id:
                self._set_last_id(account, newest_id)
                self._save_state()
                logger.info(
                    f"[{account}] 评论区已建立初始基准（最新通知id={newest_id}），"
                    "从下一轮开始处理新评论"
                )
            return
        last_id_num = self._to_int(last_id)
        fresh = []
        for item in items:
            nid = str(item.get("id") or "")
            if not nid:
                continue
            nid_num = self._to_int(nid)
            if last_id_num is not None and nid_num is not None:
                if nid_num <= last_id_num:
                    continue
            elif nid == last_id:
                continue
            fresh.append(item)
        # 已读位置直接推进到最新：超额的评论也视为已读不再回复（防积压，参考BiliCommentBot的max_process）
        self._set_last_id(account, newest_id)
        self._save_state()
        if not fresh:
            return
        fresh.reverse()  # 旧→新，先回较早的评论
        handled = 0
        for item in fresh:
            if handled >= self._max_replies_per_cycle:
                logger.debug(
                    f"[{account}] 本轮新评论超过上限 {self._max_replies_per_cycle} 条，"
                    "其余已标记为已读"
                )
                break
            if await self._process_item(account, item):
                handled += 1

    async def _process_item(self, account: str, item: dict) -> bool:
        """处理单条通知：过滤→元数据/owner过滤→路由判定→at昵称解析→LLM生成回复
        →at拼装→入队（由Y账号发送）。返回是否入队"""
        it = item.get("item") or {}
        user = item.get("user") or {}
        nid = str(item.get("id") or "")
        mid = str(user.get("mid") or "")
        nickname = user.get("nickname") or mid or "未知用户"
        content = (it.get("source_content") or it.get("root_reply_content") or "").strip()
        # 只处理视频评论（business_id=1 即视频，亦可用作reply/add的type）
        if str(it.get("business_id") or "") != "1" and it.get("business") != "视频":
            logger.debug(
                f"[{account}] 忽略非视频评论通知(id={nid}, business={it.get('business')})"
            )
            return False
        # 忽略本插件两个账号自身的消息：防自我回复套娃，尤其防Y回复X视频评论后
        # X回复Y、Y再回X的无限循环
        if mid in self._own_mids:
            logger.debug(f"[{account}] 忽略本插件账号自身的消息(id={nid}, mid={mid})")
            return False
        if not content:
            logger.debug(f"[{account}] 忽略空内容评论通知(id={nid})")
            return False
        oid = str(it.get("subject_id") or "")
        source_id = str(it.get("source_id") or "")
        if not oid or not source_id:
            logger.debug(f"[{account}] 评论通知缺少关键字段(id={nid})，跳过")
            return False
        root_id = str(it.get("root_id") or "0")
        is_thread = root_id not in ("", "0")
        root = root_id if is_thread else source_id
        rtype = str(it.get("business_id") or "1")
        # 楼中楼深度限制：同一楼层下机器人已回复达上限则不再介入，防无限套娃。
        # 计数在回复发送成功后累加（见 _reply_worker），跨重启持久化；
        # 同一楼层多分支追问会高估深度，对防套娃而言是安全方向的误差。
        # 注：B站通知只发给被回复评论的作者（楼主收不到楼中楼通知，已实测确认），
        # 因此插件收到的楼中楼必然回复的是机器人自己的评论，无需再校验楼层归属
        if is_thread and self._max_reply_depth > 0:
            replied = int(
                (self._state.get("thread_replies") or {}).get(root, 0)
            )
            if replied >= self._max_reply_depth:
                logger.debug(
                    f"[{account}] 楼层r{root}已达最大回复深度"
                    f"({self._max_reply_depth})，不再回复(id={nid})"
                )
                return False
        # [1.1/1.2] 元数据获取（惰性）：仅 owner 过滤或路由表需要时才请求 view/detail；
        # 元数据层带进程内10分钟缓存，同一视频的连续评论不会重复请求
        meta = None
        need_meta = (account == "X" and self._x_owner_only) or self._has_routes
        if need_meta and self._get_video_meta is not None:
            try:
                meta = await self._get_video_meta(oid)
            except Exception as e:
                logger.warning(f"[{account}] 视频元数据获取失败(id={nid}): {e}")
        if account == "X" and self._x_owner_only:
            # 仅回复X自己发布的视频下的评论；元数据取不到时fail-closed跳过（宁缺毋滥）
            if meta is None:
                logger.info(
                    f"[{account}] 视频元数据未获取到，按『仅回复X自己视频』策略跳过(id={nid})"
                )
                return False
            owner_mids = {meta.get("owner_mid") or ""} | set(
                meta.get("staff_mids") or []
            )
            if self._account_mids.get("X", "") not in owner_mids:
                logger.info(
                    f"[{account}] 视频av{oid}非X账号(或其联合投稿staff)发布，跳过回复(id={nid})"
                )
                return False
        # [1.2] 路由判定：tags来自元数据；未获取/取不到(tags=None)时走默认路由，
        # 不做 tag 判定（missing_tags 在 tags 为 None 时会误判命中）
        if self._resolve_route is not None:
            route = self._resolve_route(
                meta.get("tags") if meta is not None else None
            )
        else:
            route = Route(
                name="默认", persona="", at_enable=False, at_mid="",
                at_position="prefix", at_guide="",
            )
        # [1.3·D7] at昵称解析在路由判定后立即定型（24h缓存，落状态文件）：
        # 失败则本轮at关闭（按普通路由形态生成），此后链路（prompt注入、JSON
        # 协议、at拼装）一律以 route.at_uname 为准，不再二次查询
        if route.at_enable:
            try:
                uname = await self._resolve_at_uname(route.at_mid)
            except Exception as e:
                logger.warning(
                    f"[{account}] at昵称解析异常(mid={route.at_mid}, id={nid}): {e}"
                )
                uname = None
            if uname:
                route.at_uname = uname
            else:
                route.at_enable = False
                logger.warning(
                    f"[{account}] at目标昵称解析失败(mid={route.at_mid})，"
                    f"本轮at关闭(id={nid})"
                )
        prompt_text = f"[视频评论] {nickname}({mid})说: {content}"
        if is_thread:
            # 通知自带楼层上下文（msgfeed/reply的root/target字段，零额外请求）：
            # 主评论与被回复评论（通常即机器人的上一条回复），注入prompt供AI理解语境。
            # 冷启动（如重启后记忆丢失）时这两行是楼层语境的唯一来源，故始终注入
            context_lines = []
            root_content = (it.get("root_reply_content") or "").strip()
            target_content = (it.get("target_reply_content") or "").strip()
            if root_content and root_content != content:
                one_line = " ".join(root_content.split())
                if len(one_line) > self._context_max_chars:
                    one_line = one_line[: self._context_max_chars] + "…"
                context_lines.append(f"[评论楼层] 主评论: {one_line}")
            if (
                target_content
                and target_content != root_content
                and target_content != content
            ):
                one_line = " ".join(target_content.split())
                if len(one_line) > self._context_max_chars:
                    one_line = one_line[: self._context_max_chars] + "…"
                context_lines.append(f"[回复对象] {one_line}")
            if context_lines:
                prompt_text = "\n".join(context_lines) + "\n" + prompt_text
        if self._get_video_context is not None:
            # 识别所属视频内容并置于prompt最前，让AI了解讨论范围再回复
            try:
                video_context = await self._get_video_context(oid)
            except Exception as e:
                logger.warning(f"[{account}] 视频内容识别失败(id={nid}): {e}")
                video_context = None
            if video_context:
                prompt_text = f"{video_context}\n\n{prompt_text}"
        try:
            text, at_flag = await self._on_reply_needed(
                account, prompt_text, oid, root_id, route
            )
        except Exception as e:
            logger.warning(f"[{account}] 评论LLM回复生成失败(id={nid}): {e}")
            return False
        text = " ".join((text or "").split()).strip()
        if not text:
            logger.debug(f"[{account}] 评论(id={nid})无有效回复内容，跳过")
            return False
        # at拼装须晚于截断与复读校验（顺序约束）：截断上限预留 @昵称 片段长度，
        # 避免 prefix 拼装后超出 max_length、或 suffix 位置截出半个昵称
        use_at = bool(at_flag and route.at_uname and route.at_mid.isdigit())
        if use_at:
            if route.at_position == "suffix":
                at_snippet = f" @{route.at_uname}"
            else:
                at_snippet = f"@{route.at_uname} "
            limit = self._max_length - len(at_snippet)
            if limit < 1:
                logger.warning(
                    f"[{account}] @昵称片段过长导致截断上限异常(id={nid})，按1字截断"
                )
                limit = 1
        else:
            at_snippet = ""
            limit = self._max_length
        if len(text) > limit:
            text = text[:limit]
        # AI复读评论原文会被B站判为重复内容秒删（reply/add仍返回成功），直接跳过；
        # at前缀不参与比较（否则@前缀会让复读判定永久失效）
        normalized_content = " ".join(content.split())
        if text == normalized_content or (
            len(text) >= 6 and text in normalized_content
        ):
            logger.warning(
                f"[{account}] 评论(id={nid})的AI回复与评论原文重复（复读），跳过发送"
            )
            return False
        # at拼装（仅at_flag为真且昵称已解析）：message在入队前完成拼装，
        # worker只做机械发送与验证；队列条目只扩展 at_map（不含位置信息）
        at_map = None
        if use_at:
            if route.at_position == "suffix":
                text = f"{text}{at_snippet}"
            else:
                text = f"{at_snippet}{text}"
            at_map = {route.at_uname: int(route.at_mid)}
        try:
            self._reply_queue.put_nowait(
                {
                    "nid": nid,
                    "oid": oid,
                    "root": root,
                    "parent": source_id,
                    "rtype": rtype,
                    "message": text,
                    "nickname": nickname,
                    "source_account": account,
                    "at_map": at_map,
                }
            )
        except asyncio.QueueFull:
            logger.warning(f"评论回复队列已满，丢弃: {text[:20]}")
            return False
        return True

    async def _resolve_at_uname(self, mid: str) -> str | None:
        """查询并缓存 at 目标昵称（TTL 24小时，落 comment_state.json）。
        统一用Y客户端查询（at由Y发出，Y登录态是功能必需项）；返回None表示失败。
        查询串行化（防X/Y双轮询并发重复查询）；失败后进入冷却期
        （AT_UNAME_FAIL_COOLDOWN），冷却内直接返回None、不再请求B站（防限频
        期间反复查询恶化风控），成功自动清除冷却。
        注意 mid 为空/非法时不上游调用（配置校验已保证纯数字）"""
        async with self._at_uname_lock:
            cache = self._state.setdefault("at_uname_cache", {})
            now = time.time()
            entry = cache.get(mid)
            if (
                isinstance(entry, dict)
                and entry.get("name")
                and now - float(entry.get("ts") or 0) < AT_UNAME_TTL
            ):
                return str(entry["name"])
            fail_cache = self._state.setdefault("at_uname_fail", {})
            fail_entry = fail_cache.get(mid)
            if (
                isinstance(fail_entry, dict)
                and now - float(fail_entry.get("ts") or 0) < AT_UNAME_FAIL_COOLDOWN
            ):
                logger.debug(
                    f"at昵称(mid={mid})处于失败冷却期"
                    f"（{AT_UNAME_FAIL_COOLDOWN}秒），本轮不再查询"
                )
                return None
            uname = await self._y_client.resolve_uname(mid)
            if uname:
                cache[mid] = {"name": uname, "ts": now}
                fail_cache.pop(mid, None)
                # 防状态无限膨胀：超限丢弃最早的一半（缓存丢失仅多一次查询，可接受）
                if len(cache) > 200:
                    for old_mid in list(cache)[:100]:
                        del cache[old_mid]
                self._save_state()
                return uname
            fail_cache[mid] = {"ts": now}
            # 冷却记录同样限容（丢失仅多一次查询，可接受）
            if len(fail_cache) > 200:
                for old_mid in list(fail_cache)[:100]:
                    del fail_cache[old_mid]
            self._save_state()
            return None

    async def _reply_worker(self):
        """回复发送循环：逐条取出队列中的回复，用Y账号发送并限速+随机延迟"""
        while True:
            req = await self._reply_queue.get()
            try:
                ok, at_ok = await self._y_client.send_reply(
                    req["oid"],
                    req["root"],
                    req["parent"],
                    req["message"],
                    req["rtype"],
                    req.get("at_map"),
                )
                if ok:
                    at_map = req.get("at_map") or {}
                    if at_map and not at_ok:
                        # at未生效（昵称变更/风控吞at）：记warning不重试，不阻断后续发送
                        logger.warning(
                            f"[at] nid={req['nid']} at验证未通过："
                            f"目标mid={list(at_map.values())}"
                            "（响应详情见Y客户端日志），不重试"
                        )
                    prefix = (
                        "已回复X账号视频" if req["source_account"] == "X" else "已回复视频"
                    )
                    # 楼中楼回复（root≠parent）发送成功后累加楼层深度计数，直接评论不计
                    if req["root"] != req["parent"]:
                        self._bump_thread_replies(req["root"])
                    logger.info(
                        f"{prefix}av{req['oid']}下{req['nickname']}的评论: "
                        f"{req['message'][:30]}"
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"评论回复发送异常: {e}")
            await asyncio.sleep(
                self._min_interval + random.uniform(0, self._random_delay_max)
            )

    @staticmethod
    def _to_int(value: str):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _set_last_id(self, account: str, nid: str):
        self._state.setdefault("last_id", {})[account] = nid

    def _bump_thread_replies(self, root: str):
        """楼中楼回复发送成功后累加楼层深度计数并落盘（跨重启持久化）"""
        counts = self._state.setdefault("thread_replies", {})
        counts[root] = int(counts.get(root, 0)) + 1
        # 防状态无限膨胀：超限丢弃最早的一半（深度计数丢失仅使防套娃暂时失效，可接受）
        if len(counts) > 500:
            for old_root in list(counts)[:250]:
                del counts[old_root]
        self._save_state()

    def _load_state(self) -> dict:
        try:
            if self._state_path.exists():
                state = json.loads(self._state_path.read_text(encoding="utf-8"))
                if isinstance(state, dict):
                    return state
        except Exception as e:
            logger.warning(f"评论区状态文件读取失败，将从空白状态开始: {e}")
        return {"last_id": {}}

    def _save_state(self):
        try:
            tmp_path = self._state_path.with_name(self._state_path.name + ".tmp")
            tmp_path.write_text(
                json.dumps(self._state, ensure_ascii=False), encoding="utf-8"
            )
            tmp_path.replace(self._state_path)
        except Exception as e:
            logger.warning(f"评论区状态文件写入失败: {e}")
