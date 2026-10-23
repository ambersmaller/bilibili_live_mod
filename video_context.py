"""视频内容识别：拉取评论所属视频的公开元数据，分两层使用。

参考 bilibili-ai-bot 的视频识别管线。元数据层（get_meta）：`view/detail`
一次取回 View（owner.mid/staff[]）与 Tags[]，供「仅回复X自己视频」过滤与
tag 人设路由使用；不依赖 LLM、不受 video_context 开关影响，带两层缓存：
进程内 aid 缓存（TTL 10 分钟）→ 长期归属缓存 video_owners.json（30 天，跨
重启，由 get_owner 提供 0 请求的本地查询）。概括层（get_context）：
LLM 一句话概括后按视频(bvid)落盘缓存，以【当前视频信息】注入回复提示词最前。

风控加固（v3.3.0 起）：
1. 元数据请求携带 Y 账号 Cookie（本插件唯一曾匿名直连的接口），并显式检查
   HTTP 状态码——此前 412/HTML 被伪装成「JSON 解析失败」，无从排查。
2. 失败写负缓存（META_FAIL_TTL）并计入 FailureGuard，连续失败即熔断，
   避免一次风控被「每条评论重打一次请求」放大成持续封禁。
3. 请求失败时降级用本地归属缓存判定 owner（get_owner 全程 0 请求），
   见过的视频不再因网络问题被静默丢弃。
"""
import asyncio
import json
import time
from pathlib import Path

import aiohttp

from astrbot.api import logger

from .blivedm.clients.ws_base import USER_AGENT
from .failure_guard import FailureGuard

# 一次请求同时取回 View（owner/staff）与 Tags 的接口（tag 路由与 owner 过滤的共用数据源）
VIEW_DETAIL_URL = "https://api.bilibili.com/x/web-interface/view/detail"

# 注入prompt的简介上限（字）：视频信息模板与降级拼接共用
DESC_MAX = 250
# LLM概括的长度上限（字）
ANALYSIS_MAX = 80
# 进程内元数据缓存TTL（秒）：同一视频连续评论只发一次 view/detail，进程重启清空（可接受）
META_CACHE_TTL = 600
# 元数据失败负缓存TTL（秒）：期内不再重试同一视频，防一次风控触发请求风暴
META_FAIL_TTL = 120
# 元数据熔断：连续失败达阈值后暂停请求，冷却到期以真实请求探测恢复
META_GUARD_THRESHOLD = 3
META_GUARD_COOLDOWN = 300
# 长期归属缓存：视频归属不会变，成功取到一次即长期复用（跨重启）
OWNER_TTL = 30 * 86400
# tags 新鲜度：超期的缓存仍可判 owner，但不再参与 tag 路由（避免用过期标签误路由）
OWNER_TAGS_TTL = 86400
# 落盘归属缓存的最大条目数（超出按时间淘汰最早的一半）
OWNER_MAX = 2000

CONTEXT_TEMPLATE = (
    "【当前视频信息】\n"
    "标题：{title}\n"
    "UP主：{owner_name}\n"
    "分区：{tname}\n"
    "简介：{desc}\n"
    "内容概括：{analysis}"
)

# 落盘缓存条目的必备字段：旧版缓存缺任一字段即视为过期重建（重建代价仅为补一次LLM概括）
CACHE_FIELDS = (
    "bvid", "title", "owner_name", "owner_mid", "staff_mids", "tname",
    "desc", "tags", "analysis",
)


class VideoContextManager:
    """视频元数据与内容概括管理器：元数据层（多级缓存 + 熔断）+ 概括层（按bvid落盘缓存）"""

    def __init__(
        self,
        state_path: Path,
        summarize,
        wbi_sign=None,
        get_cookie=None,
    ):
        """
        :param summarize: async (metadata: dict) -> str | None，
            由宿主调用LLM生成概括；返回None时降级为原始元数据
        :param wbi_sign: 可选 async (params: dict) -> dict | None，宿主注入的
            wbi 签名回调（通常为评论客户端的 signed_params，避免本模块反向
            持有客户端实例）；None 或签名失败时无签名直连并记 debug 日志
        :param get_cookie: 可选 () -> str，宿主注入的 Cookie 回调（Y账号）。
            元数据接口曾因匿名直连被 WAF 拦截（HTTP 412），带登录态后稳定；
            回调每次请求实时取值，Cookie 自动刷新后无需重建本管理器
        """
        self._state_path = state_path
        self._summarize = summarize
        self._wbi_sign = wbi_sign
        self._get_cookie = get_cookie
        self._session: aiohttp.ClientSession | None = None
        # 元数据锁：仅保护网络请求与内存字典（LLM概括另用 _summary_lock，防长时间阻塞）
        self._lock = asyncio.Lock()
        self._summary_lock = asyncio.Lock()
        self._guard = FailureGuard(
            threshold=META_GUARD_THRESHOLD,
            cooldown=META_GUARD_COOLDOWN,
            name="B站元数据",
        )
        self._cache = self._load_state()
        # 进程内元数据缓存：aid -> (写入时间, meta字典)
        self._meta_cache: dict[str, tuple[float, dict]] = {}
        # 元数据负缓存：aid -> 失败时间（期内不再请求同一视频）
        self._meta_fail: dict[str, float] = {}
        # 长期归属缓存（aid -> entry）
        self._owners, _ = self._load_nested("video_owners.json")

    async def start(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )

    async def stop(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def get_meta(self, oid: str) -> dict | None:
        """元数据层：取视频(oid=aid)的元数据（owner_mid/staff_mids/tags等），
        供 owner 过滤与 tag 路由使用。网络失败时降级返回本地归属缓存，
        全无缓存才返回 None（调用方按 fail-closed 跳过）"""
        async with self._lock:
            return await self._get_meta_cached(oid)

    async def get_owner(self, oid: str) -> dict | None:
        """纯本地归属查询（0 请求）：进程缓存 → 长期归属缓存。
        命中即可判定 owner（含联合投稿staff），未命中返回 None 由调用方决定是否联网"""
        async with self._lock:
            return self._owner_view(oid)

    async def get_context(self, oid: str) -> str | None:
        """概括层：基于元数据生成视频内容概括文本（【当前视频信息】），
        失败返回None（不阻断回复流程）。已有落盘概括时直接渲染，0请求0 LLM"""
        try:
            bvid = self._lookup_bvid(oid)
            cached = self._cache.get(bvid) if bvid else None
            if cached is not None and all(f in cached for f in CACHE_FIELDS):
                return self._render(cached)
            meta = await self.get_meta(oid)
            if meta is None:
                return self._render(cached) if cached else None
            if not meta.get("title"):
                # 仅本地归属兜底（无标题/简介），不足以生成概括
                return self._render(cached) if cached else None
            return await self._build_context(oid, meta)
        except Exception as e:
            logger.warning(f"视频内容识别失败(oid={oid}): {e}")
            return None

    async def _get_meta_cached(self, oid: str) -> dict | None:
        """取元数据（须持有 self._lock）：缓存/负缓存/熔断 → 网络 → 落盘归属"""
        cached = self._meta_cache.get(oid)
        if cached is not None and time.time() - cached[0] < META_CACHE_TTL:
            return cached[1]
        failed_at = self._meta_fail.get(oid)
        if failed_at is not None:
            if time.time() - failed_at < META_FAIL_TTL:
                return self._local_owner(oid)
            self._meta_fail.pop(oid, None)
        if not self._guard.allow():
            return self._local_owner(oid)
        meta = await self._fetch_metadata(oid)
        if meta is None:
            self._meta_fail[oid] = time.time()
            self._guard.record_failure()
            return self._local_owner(oid)
        self._guard.record_success()
        self._meta_cache[oid] = (time.time(), meta)
        self._store_owner(oid, meta)
        return meta

    def _owner_view(self, oid: str) -> dict | None:
        """纯本地归属视图（须持有 self._lock）：进程缓存 → 长期归属缓存"""
        cached = self._meta_cache.get(oid)
        if cached is not None:
            return dict(cached[1], source="meta")
        entry = self._owners.get(oid)
        if isinstance(entry, dict) and self._owner_fresh(entry):
            return self._owner_meta(entry, "owner_cache")
        return None

    def _local_owner(self, oid: str) -> dict | None:
        """网络不可用时的降级视图：过期进程缓存 > 长期归属缓存"""
        cached = self._meta_cache.get(oid)
        if cached is not None:
            return dict(cached[1], from_cache=True, source="stale_meta")
        return self._owner_view(oid)

    def _lookup_bvid(self, oid: str) -> str:
        """aid → bvid（仅查本地，用于概括缓存命中；查不到返回空串）"""
        cached = self._meta_cache.get(oid)
        if cached is not None:
            return str(cached[1].get("bvid") or "")
        entry = self._owners.get(oid)
        if isinstance(entry, dict):
            return str(entry.get("bvid") or "")
        return ""

    @staticmethod
    def _owner_fresh(entry: dict) -> bool:
        return time.time() - float(entry.get("ts") or 0) <= OWNER_TTL

    @staticmethod
    def _owner_meta(entry: dict, source: str) -> dict:
        """长期归属缓存 → 元数据形态。tags 超期后置 None，避免用过期标签误路由"""
        tags = list(entry.get("tags") or [])
        if time.time() - float(entry.get("ts") or 0) > OWNER_TAGS_TTL:
            tags = None
        return {
            "bvid": entry.get("bvid") or "",
            "title": entry.get("title") or "",
            "owner_name": entry.get("owner_name") or "",
            "owner_mid": str(entry.get("owner_mid") or ""),
            "staff_mids": list(entry.get("staff_mids") or []),
            "tname": entry.get("tname") or "",
            "desc": "",
            "duration": 0,
            "tags": tags,
            "created": int(entry.get("created") or 0),
            "from_cache": True,
            "source": source,
        }

    def _store_owner(self, oid: str, meta: dict):
        """元数据成功后写入长期归属缓存（视频归属不变，一次成功长期有效）"""
        owner_mid = str(meta.get("owner_mid") or "")
        if not owner_mid:
            return
        self._owners[oid] = {
            "bvid": meta.get("bvid") or "",
            "title": meta.get("title") or "",
            "owner_name": meta.get("owner_name") or "",
            "owner_mid": owner_mid,
            "staff_mids": list(meta.get("staff_mids") or []),
            "tname": meta.get("tname") or "",
            "tags": list(meta.get("tags") or []),
            "created": int(meta.get("created") or 0),
            "ts": int(time.time()),
        }
        if len(self._owners) > OWNER_MAX:
            ordered = sorted(
                self._owners.items(),
                key=lambda kv: float((kv[1] or {}).get("ts") or 0),
            )
            for old_oid, _ in ordered[: len(ordered) - OWNER_MAX]:
                self._owners.pop(old_oid, None)
        self._save_nested(
            "video_owners.json",
            self._owners,
            {"ver": 1, "updated_at": int(time.time())},
        )

    async def _build_context(self, oid: str, meta: dict) -> str:
        """构造【当前视频信息】文本：LLM 调用移出元数据锁，仅串行化概括本身"""
        bvid = meta.get("bvid") or oid
        async with self._summary_lock:
            cached = self._cache.get(bvid)
            if cached is not None and not all(f in cached for f in CACHE_FIELDS):
                # 旧版缓存缺少新字段（owner_mid/tags等），视为过期重新生成（每个视频仅多一次概括调用）
                cached = None
            if cached is None:
                analysis = await self._summarize(meta)
                if not analysis:
                    analysis = self._raw_summary(meta)
                cached = self._make_entry(bvid, meta, analysis)
                self._cache[bvid] = cached
                self._save_state()
                logger.info(f"视频内容概括已生成并缓存: {bvid} 《{cached['title']}》")
            return self._render(cached)

    @staticmethod
    def _make_entry(bvid: str, meta: dict, analysis: str) -> dict:
        return {
            "bvid": bvid,
            "title": meta.get("title") or "",
            "owner_name": meta.get("owner_name") or "",
            "owner_mid": meta.get("owner_mid") or "",
            "staff_mids": list(meta.get("staff_mids") or []),
            "tname": meta.get("tname") or "",
            "desc": (meta.get("desc") or "").strip()[:DESC_MAX] or "无",
            "tags": list(meta.get("tags") or []),
            "analysis": analysis,
            "updated_at": int(time.time()),
        }

    @staticmethod
    def _render(cached: dict) -> str:
        return CONTEXT_TEMPLATE.format(
            title=cached.get("title") or "未知",
            owner_name=cached.get("owner_name") or "未知",
            tname=cached.get("tname") or "未知",
            desc=cached.get("desc") or "无",
            analysis=cached.get("analysis") or "",
        )

    def _headers(self) -> dict:
        """元数据请求头：UA + Referer + Cookie。
        匿名直连会被 WAF 拦截（HTTP 412/HTML），故复用 Y 账号登录态"""
        headers = {"User-Agent": USER_AGENT, "Referer": "https://www.bilibili.com/"}
        if self._get_cookie is not None:
            try:
                cookie = self._get_cookie() or ""
            except Exception as e:
                logger.debug(f"取Cookie失败(元数据请求将匿名直连): {e}")
                cookie = ""
            if cookie:
                headers["Cookie"] = cookie
        return headers

    async def _fetch_metadata(self, oid: str) -> dict | None:
        """请求 view/detail 取回 View+Tags 并拍平为元数据字典（不含LLM概括）。
        按『需要 wbi 签名』预设实现（view 系接口 2023 年后普遍要求签名），
        签名回调不可用时无签名直连并记 debug 日志"""
        if self._session is None:
            return None
        params = {"aid": oid}
        if self._wbi_sign is not None:
            try:
                signed = await self._wbi_sign(params)
            except Exception as e:
                logger.debug(f"wbi签名回调异常(oid={oid})，无签名直连: {e}")
                signed = None
            if signed is not None:
                params = signed
            else:
                logger.debug("wbi签名不可用(密钥获取失败)，view/detail无签名直连")
        try:
            async with self._session.get(
                VIEW_DETAIL_URL,
                params=params,
                headers=self._headers(),
            ) as resp:
                if resp.status != 200:
                    # 风控拦截返回 HTML(412)，此前会被 resp.json() 吞成「解析失败」
                    body = (await resp.text())[:120].replace("\n", " ")
                    logger.warning(
                        f"获取视频元数据失败(oid={oid}): HTTP {resp.status}, body={body!r}"
                    )
                    return None
                result = await resp.json()
        except Exception as e:
            logger.warning(f"获取视频元数据失败(oid={oid}): {e}")
            return None
        if not isinstance(result, dict):
            logger.warning(f"视频元数据响应格式异常(oid={oid}): {type(result).__name__}")
            return None
        if result.get("code") != 0:
            logger.warning(
                f"获取视频元数据失败(oid={oid}): code={result.get('code')}, "
                f"message={result.get('message')}"
            )
            return None
        data = result.get("data") or {}
        view = data.get("View") or {}
        if not view:
            logger.warning(f"视频元数据响应缺少View(oid={oid})")
            return None
        owner = view.get("owner") or {}
        staff_mids = []
        for staff in view.get("staff") or []:
            staff_mid = str((staff or {}).get("mid") or "").strip()
            if staff_mid:
                staff_mids.append(staff_mid)
        tags = []
        for tag in data.get("Tags") or []:
            tag_name = str((tag or {}).get("tag_name") or "").strip()
            if tag_name:
                tags.append(tag_name)
        return {
            "bvid": str(view.get("bvid") or ""),
            "title": view.get("title") or "",
            "owner_name": owner.get("name") or "",
            "owner_mid": str(owner.get("mid") or ""),
            "staff_mids": staff_mids,
            "tname": view.get("tname") or "",
            "desc": (view.get("desc") or "").strip(),
            "duration": view.get("duration") or 0,
            "tags": tags,
            "created": int(view.get("pubdate") or 0),
        }

    @staticmethod
    def _raw_summary(data: dict) -> str:
        """降级方案：直接拼接原始元数据（标题+UP主+分区+简介前100字）"""
        parts = [
            f"《{data.get('title') or '未知'}》",
            f"UP主：{data.get('owner_name') or '未知'}",
            f"分区：{data.get('tname') or '未知'}",
        ]
        desc = (data.get("desc") or "").strip()
        if desc:
            parts.append(f"简介：{desc[:DESC_MAX]}")
        return "，".join(parts)

    def _load_state(self) -> dict:
        try:
            if self._state_path.exists():
                state = json.loads(self._state_path.read_text(encoding="utf-8"))
                if isinstance(state, dict):
                    return state
        except Exception as e:
            logger.warning(f"视频内容缓存读取失败，将从空白缓存开始: {e}")
        return {}

    def _save_state(self):
        try:
            tmp_path = self._state_path.with_name(self._state_path.name + ".tmp")
            tmp_path.write_text(
                json.dumps(self._cache, ensure_ascii=False), encoding="utf-8"
            )
            tmp_path.replace(self._state_path)
        except Exception as e:
            logger.warning(f"视频内容缓存写入失败: {e}")

    def _load_nested(self, filename: str) -> tuple[dict, dict]:
        """读取 {_meta:{...}, videos:{aid: entry}} 结构的落盘文件"""
        path = self._state_path.with_name(filename)
        try:
            if path.exists():
                data = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(data, dict):
                    videos = data.get("videos")
                    meta = data.get("_meta")
                    return (
                        videos if isinstance(videos, dict) else {},
                        meta if isinstance(meta, dict) else {},
                    )
        except Exception as e:
            logger.warning(f"{filename} 读取失败，将从空白缓存开始: {e}")
        return {}, {}

    def _save_nested(self, filename: str, videos: dict, meta: dict):
        path = self._state_path.with_name(filename)
        try:
            payload = {"_meta": meta, "videos": videos}
            tmp_path = path.with_name(path.name + ".tmp")
            tmp_path.write_text(
                json.dumps(payload, ensure_ascii=False), encoding="utf-8"
            )
            tmp_path.replace(path)
        except Exception as e:
            logger.warning(f"{filename} 写入失败: {e}")
