"""B站视频评论区自动回复：轮询『收到评论』消息流 + 限速回复（统一由Y账号发出）

实现参考 bilibili-API-collect（msgfeed/reply、reply/add、wbi签名文档）与
BiliCommentBot、bilibili-ai-bot 等成熟同类项目的防风控实践：
首轮轮询只建立基准不回复、已读位置持久化（跨重启不重复回复）、每轮限量、
发送限速+随机延迟、忽略本插件两个账号自身的消息、回复失败不重试。
"""
import asyncio
import hashlib
import json
import random
import time
import urllib.parse
from pathlib import Path

import aiohttp

from astrbot.api import logger

from .blivedm.clients.ws_base import USER_AGENT

NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
REPLY_FEED_URL = "https://api.bilibili.com/x/msgfeed/reply"
REPLY_ADD_URL = "https://api.bilibili.com/x/v2/reply/add"

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


def _parse_cookie_str(cookie_str: str) -> dict:
    cookies = {}
    for item in cookie_str.split(";"):
        item = item.strip()
        if "=" in item:
            key, value = item.split("=", 1)
            cookies[key.strip()] = value.strip()
    return cookies


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

    async def send_reply(
        self, oid: str, root: str, parent: str, message: str, reply_type: str = "1"
    ) -> bool:
        """发布一条视频评论回复（POST reply/add，仅需Cookie+CSRF，无需wbi）。
        成功返回True，失败返回False（不重试）"""
        csrf = _parse_cookie_str(self._get_cookie()).get("bili_jct", "")
        if not csrf:
            logger.error(f"[{self.label}] 缺少bili_jct，无法发送评论回复")
            return False
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
            return False
        code = result.get("code")
        if code == 0:
            return True
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
    ):
        """
        :param on_reply_needed: async (account_label, prompt_text, oid, root_id) -> str | None，
            由宿主完成LLM回复生成（含人设prompt），返回None表示不回复；
            root_id为评论所在楼层的根评论id（"0"表示对视频的直接评论），供宿主按楼层维护上下文
        :param get_video_context: async (oid) -> str | None，由宿主识别评论所属视频
            内容（【当前视频信息】文本），置于prompt最前；None表示不识别
        :param poll_gate: 可选的同步门控 callable，返回False时跳过本轮轮询
            （如宿主LLM熔断中：不请求B站、不推进已读位置，恢复后自然补回）
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
        self._tasks: list[asyncio.Task] = []
        self._reply_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=REPLY_QUEUE_MAX)
        self._own_mids: set[str] = set()
        self._state = self._load_state()

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
        if self._x_client:
            await self._x_client.start()
            if await self._x_client.ensure_identity():
                logger.info(
                    f"X账号轮询已启用: {self._x_client.uname}({self._x_client.mid})，"
                    "其收到的新评论将转发给Y账号统一回复"
                )
                self._own_mids.add(self._x_client.mid)
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
        """处理单条通知：过滤→LLM生成回复→入队（由Y账号发送）。返回是否入队"""
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
            text = await self._on_reply_needed(account, prompt_text, oid, root_id)
        except Exception as e:
            logger.warning(f"[{account}] 评论LLM回复生成失败(id={nid}): {e}")
            return False
        text = " ".join((text or "").split()).strip()
        if not text:
            logger.debug(f"[{account}] 评论(id={nid})无有效回复内容，跳过")
            return False
        if len(text) > self._max_length:
            text = text[: self._max_length]
        # AI复读评论原文会被B站判为重复内容秒删（reply/add仍返回成功），直接跳过
        normalized_content = " ".join(content.split())
        if text == normalized_content or (
            len(text) >= 6 and text in normalized_content
        ):
            logger.warning(
                f"[{account}] 评论(id={nid})的AI回复与评论原文重复（复读），跳过发送"
            )
            return False
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
                }
            )
        except asyncio.QueueFull:
            logger.warning(f"评论回复队列已满，丢弃: {text[:20]}")
            return False
        return True

    async def _reply_worker(self):
        """回复发送循环：逐条取出队列中的回复，用Y账号发送并限速+随机延迟"""
        while True:
            req = await self._reply_queue.get()
            try:
                if await self._y_client.send_reply(
                    req["oid"], req["root"], req["parent"], req["message"], req["rtype"]
                ):
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
