class ContextRecord:
    """上下文记录器（普通类，每次构造都是独立实例）

    按发送者键维护短期对话记忆；get_messages 返回拷贝，
    调用方（含 LLM provider）拿到的不是内部活引用，避免被外部改写。
    """

    def __init__(self, max_messages: int = 15):
        self.max_messages = max_messages * 2
        self.message_dict = {}

    def put_message(self, sender: str, message: str, is_ai: bool):
        """插入消息"""
        if sender not in self.message_dict:
            self.message_dict[sender] = []

        if len(self.message_dict[sender]) >= self.max_messages:
            self.message_dict[sender].pop(0)

        self.message_dict[sender].append(
            {"role": "assistant" if is_ai else "user", "content": f"{message}"}
        )

    def get_messages(self, sender: str) -> list[dict]:
        """获取消息（返回拷贝，防止外部就地修改内部记录）"""
        return [dict(m) for m in self.message_dict.get(sender, [])]
