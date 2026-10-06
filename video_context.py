"""视频内容识别：拉取评论所属视频的公开元数据，LLM一句话概括后按视频(bvid)缓存。

参考 bilibili-ai-bot 的视频识别管线：评论通知自带视频aid → 查元数据 → 概括
→ 按视频粒度缓存 → 以【当前视频信息】注入回复提示词最前，让AI了解所评视频
的大概讨论范围。识别失败时降级为拼接原始元数据，不中断回复流程。
"""
import asyncio
import json
from pathlib import Path

import aiohttp

from astrbot.api import logger

from .blivedm.clients.ws_base import USER_AGENT

VIEW_API_URL = "https://api.bilibili.com/x/web-interface/view"

# 注入prompt的简介上限（字）：视频信息模板与降级拼接共用
DESC_MAX = 250
# LLM概括的长度上限（字）
ANALYSIS_MAX = 80

CONTEXT_TEMPLATE = (
    "【当前视频信息】\n"
    "标题：{title}\n"
    "UP主：{owner}\n"
    "分区：{tname}\n"
    "简介：{desc}\n"
    "内容概括：{analysis}"
)


class VideoContextManager:
    """按视频(bvid)粒度缓存内容概括，供评论区回复提示词注入"""

    def __init__(self, state_path: Path, summarize):
        """
        :param summarize: async (metadata: dict) -> str | None，
            由宿主调用LLM生成概括；返回None时降级为原始元数据
        """
        self._state_path = state_path
        self._summarize = summarize
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()
        self._cache = self._load_state()

    async def start(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15)
            )

    async def stop(self):
        if self._session:
            await self._session.close()
            self._session = None

    async def get_context(self, oid: str) -> str | None:
        """获取视频(oid=aid)的内容概括文本，失败返回None（不阻断回复流程）"""
        async with self._lock:
            try:
                return await self._get_context_locked(oid)
            except Exception as e:
                logger.warning(f"视频内容识别失败(oid={oid}): {e}")
                return None

    async def _get_context_locked(self, oid: str) -> str | None:
        data = await self._fetch_metadata(oid)
        if data is None:
            return None
        bvid = str(data.get("bvid") or oid)
        cached = self._cache.get(bvid)
        if cached is not None and "desc" not in cached:
            # 旧版缓存缺少简介字段，视为过期重新生成（每个视频仅多一次概括调用）
            cached = None
        if cached is None:
            analysis = await self._summarize(data)
            if not analysis:
                analysis = self._raw_summary(data)
            cached = {
                "title": data.get("title") or "",
                "owner": (data.get("owner") or {}).get("name") or "",
                "tname": data.get("tname") or "",
                "desc": (data.get("desc") or "").strip()[:DESC_MAX] or "无",
                "analysis": analysis,
            }
            self._cache[bvid] = cached
            self._save_state()
            logger.info(f"视频内容概括已生成并缓存: {bvid} 《{cached['title']}》")
        return CONTEXT_TEMPLATE.format(**cached)

    async def _fetch_metadata(self, oid: str) -> dict | None:
        if self._session is None:
            return None
        try:
            async with self._session.get(
                VIEW_API_URL,
                params={"aid": oid},
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
        return result.get("data") or None

    @staticmethod
    def _raw_summary(data: dict) -> str:
        """降级方案：直接拼接原始元数据（标题+UP主+分区+简介前100字）"""
        parts = [
            f"《{data.get('title') or '未知'}》",
            f"UP主：{(data.get('owner') or {}).get('name') or '未知'}",
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
