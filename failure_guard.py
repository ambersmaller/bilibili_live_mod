"""连续失败守卫：单LLM供应商 + 单调用入口场景下的最简熔断。

经典熔断器（滑动窗口/失败率/半开放配额）为高频多实例调用设计；
本插件全插件LLM调用汇聚于 main.LlmChatService._chat 一个入口、
供应商唯一且调用量低，只需最朴素形态：

    连续失败达阈值 → 打开（冷却期内直接拒绝调用，降级为不回复）
    → 冷却到期放行一条真实请求作为探针 → 成功闭合 / 失败重新计时。

探针即下一条真实消息的正常调用，成功则回复照常发出，无额外探测成本。
"""
import time

from astrbot.api import logger


class FailureGuard:
    """连续失败计数 + 冷却熔断（CLOSED/OPEN 双态，到期惰性放行单探针）

    :param threshold: 连续失败多少次后打开（异常/超时/空响应均计为失败）
    :param cooldown: 打开后的冷却时长（秒），期内 allow() 返回 False
    :param name: 日志前缀（区分多个守卫实例）
    """

    def __init__(self, threshold: int = 3, cooldown: float = 120.0, name: str = "LLM"):
        self._threshold = max(1, int(threshold))
        self._cooldown = max(10.0, float(cooldown))
        self._name = name
        self._consecutive_failures = 0
        self._opened_at: float | None = None

    @property
    def is_open(self) -> bool:
        """当前是否处于冷却期内（True 表示应拒绝调用）"""
        return self._opened_at is not None and (
            time.monotonic() - self._opened_at < self._cooldown
        )

    def allow(self) -> bool:
        """调用前检查。False 表示跳过本次调用（不发起、不计费）；
        冷却到期时放行本次作为恢复探针，成败由 record_* 决定"""
        return self._opened_at is None or not self.is_open

    def record_success(self):
        """记录一次成功：清零计数；若处于打开态则闭合（探针成功，恢复调用）"""
        self._consecutive_failures = 0
        if self._opened_at is not None:
            self._opened_at = None
            logger.info(f"[{self._name}] 熔断恢复，恢复正常调用")

    def record_failure(self):
        """记录一次失败：连续失败达阈值时打开并进入冷却；
        打开期间的失败（含探针失败）重新计时冷却"""
        self._consecutive_failures += 1
        if self._opened_at is not None:
            self._opened_at = time.monotonic()  # 探针失败，冷却重新计时
            return
        if self._consecutive_failures >= self._threshold:
            self._opened_at = time.monotonic()
            logger.warning(
                f"[{self._name}] 连续失败 {self._consecutive_failures} 次，"
                f"暂停调用 {self._cooldown:.0f} 秒"
                "（期间自动跳过，到期以真实请求探测恢复）"
            )
