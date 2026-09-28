"""直播消息攒批缓冲：聚合多条消息后按数量阈值或时间窗口一次性冲刷，打包交给LLM回复一条"""

import asyncio
from collections.abc import Awaitable, Callable

from astrbot.api import logger


class BatchBuffer:
    """攒批缓冲：push() 收集消息文本，达到数量阈值或时间窗口到期时经 on_flush 一次性吐出全部。

    时间窗口从缓冲清空后的第一条消息开始计时，flush 后重新计时。
    只在弹幕机器人模式下由主插件启用，单事件循环内使用。
    """

    def __init__(
        self,
        max_batch: int,
        max_wait: float,
        on_flush: Callable[[list[str]], Awaitable[None]],
    ):
        self._max_batch = max(1, int(max_batch))
        self._max_wait = max(0.5, float(max_wait))
        self._on_flush = on_flush
        self._items: list[str] = []
        self._timer: asyncio.TimerHandle | None = None

    def push(self, text: str):
        """追加一条消息；首条启动时间窗口，达到数量阈值立即冲刷"""
        self._items.append(text)
        if self._timer is None:
            self._timer = asyncio.get_running_loop().call_later(
                self._max_wait, self._flush
            )
        if len(self._items) >= self._max_batch:
            self._flush()

    def reset(self) -> int:
        """清空缓冲并停止计时（下播/插件卸载时调用），返回丢弃的消息条数"""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        dropped = len(self._items)
        self._items = []
        return dropped

    def _flush(self):
        """到点/到量的同步入口：取出全部条目转交异步回调（回调异常不扩散）"""
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        if not self._items:
            return
        items, self._items = self._items, []
        logger.info(f"攒批冲刷：聚合 {len(items)} 条直播消息，打包请求LLM")
        asyncio.create_task(self._drain(items))

    async def _drain(self, items: list[str]):
        try:
            await self._on_flush(items)
        except Exception as e:
            logger.warning(f"攒批回复失败: {e}")
