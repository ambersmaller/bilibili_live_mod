"""视频内容识别：拉取评论所属视频的公开元数据，分两层使用。

参考 bilibili-ai-bot 的视频识别管线。元数据层（get_meta）：`view/detail`
一次取回 View（owner.mid/staff[]）与 Tags[]，供「仅回复X自己视频」过滤与
tag 人设路由使用；不依赖 LLM、不受 video_context 开关影响，带进程内 aid
缓存（TTL 10 分钟），避免同一视频的连续评论重复请求（风控）。概括层
（get_context）：LLM 一句话概括后按视频(bvid)落盘缓存，以【当前视频信息】
注入回复提示词最前，让AI了解所评视频的大概讨论范围。识别失败时降级为
拼接原始元数据，不中断回复流程。
"""
import asyncio
import json
import time
from pathlib import Path

import aiohttp

from astrbot.api import logger

from .blivedm.clients.ws_base import USER_AGENT

# 一次请求同时取回 View（owner/staff）与 Tags 的接口（tag 路由与 owner 过滤的共用数据源）
VIEW_DETAIL_URL = "https://api.bilibili.com/x/web-interface/view/detail"

# 注入prompt的简介上限（字）：视频信息模板与降级拼接共用
DESC_MAX = 250
# LLM概括的长度上限（字）
ANALYSIS_MAX = 80
# 进程内元数据缓存TTL（秒）：同一视频连续评论只发一次 view/detail，进程重启清空（可接受）
META_CACHE_TTL = 600

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
    """视频元数据与内容概括管理器：元数据层（进程内缓存）+ 概括层（按bvid落盘缓存）"""

    def __init__(self, state_path: Path, summarize, wbi_sign=None):
        """
        :param summarize: async (metadata: dict) -> str | None，
            由宿主调用LLM生成概括；返回None时降级为原始元数据
        :param wbi_sign: 可选 async (params: dict) -> dict | None，宿主注入的
            wbi 签名回调（通常为评论客户端的 signed_params，避免本模块反向
            持有客户端实例）；None 或签名失败时无签名直连并记 debug 日志
        """
        self._state_path = state_path
        self._summarize = summarize
        self._wbi_sign = wbi_sign
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()
        self._cache = self._load_state()
        # 进程内元数据缓存：aid -> (写入时间, meta字典)
        self._meta_cache: dict[str, tuple[float, dict]] = {}

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
        供 owner 过滤与 tag 路由使用；失败返回 None（不阻断流程，调用方自行决策）"""
        async with self._lock:
            return await self._get_meta_cached(oid)

    async def get_context(self, oid: str) -> str | None:
        """概括层：基于元数据生成视频内容概括文本（【当前视频信息】），
        失败返回None（不阻断回复流程）。签名与输出模板与旧版保持一致"""
        async with self._lock:
            try:
                meta = await self._get_meta_cached(oid)
            except Exception as e:
                logger.warning(f"视频内容识别失败(oid={oid}): {e}")
                return None
            if meta is None:
                return None
            try:
                return await self._build_context(oid, meta)
            except Exception as e:
                logger.warning(f"视频内容识别失败(oid={oid}): {e}")
                return None

    async def _get_meta_cached(self, oid: str) -> dict | None:
        """取元数据（须持有 self._lock）：命中进程内缓存直接返回，否则请求 view/detail"""
        cached = self._meta_cache.get(oid)
        if cached is not None and time.time() - cached[0] < META_CACHE_TTL:
            return cached[1]
        meta = await self._fetch_metadata(oid)
        if meta is None:
            return None
        self._meta_cache[oid] = (time.time(), meta)
        return meta

    async def _build_context(self, oid: str, meta: dict) -> str:
        """构造【当前视频信息】文本（须持有 self._lock）：缺概括时补一次LLM调用"""
        bvid = meta.get("bvid") or oid
        cached = self._cache.get(bvid)
        if cached is not None and not all(f in cached for f in CACHE_FIELDS):
            # 旧版缓存缺少新字段（owner_mid/tags等），视为过期重新生成（每个视频仅多一次概括调用）
            cached = None
        if cached is None:
            analysis = await self._summarize(meta)
            if not analysis:
                analysis = self._raw_summary(meta)
            cached = {
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
            self._cache[bvid] = cached
            self._save_state()
            logger.info(f"视频内容概括已生成并缓存: {bvid} 《{cached['title']}》")
        return CONTEXT_TEMPLATE.format(
            title=cached.get("title") or "未知",
            owner_name=cached.get("owner_name") or "未知",
            tname=cached.get("tname") or "未知",
            desc=cached.get("desc") or "无",
            analysis=cached.get("analysis") or "",
        )

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
                headers={
                    "User-Agent": USER_AGENT,
                    "Referer": "https://www.bilibili.com/",
                },
            ) as resp:
                result = await resp.json()
        except Exception as e:
            logger.warning(f"获取视频元数据失败(oid={oid}): {e}")
            return None
        if result.get("code") != 0:
            logger.debug(
                f"获取视频元数据失败(oid={oid}): code={result.get('code')}, "
                f"message={result.get('message')}"
            )
            return None
        data = result.get("data") or {}
        view = data.get("View") or {}
        if not view:
            logger.debug(f"视频元数据响应缺少View(oid={oid})")
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
