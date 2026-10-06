# astrbot_plugin_bilibili_live_mod

<img width="696" height="394" alt="QQ20260922-021133" src="https://github.com/user-attachments/assets/fde98151-6b3c-4802-9266-f478016adede" />

**B站回复机器人**：视频、直播间的AI回复助手。
**采用双账号制**：大号收到消息并转发，小号负责所有回复行为，既可防止账号人设被AI影响，又可防风控。


## 安装

1. 在AstrBot管理面板或插件市场安装本插件（zip或插件目录均可），依赖自动安装。
2. 打开插件配置，按下文填写，保存后自动重载。

## 接入方式（二选一，同时启用时以Web为准）

**Web接入（弹幕机器人必选）**：只需B站账号Cookie，无需任何资质。

- 直播间ID：地址 `live.bilibili.com/12345678` 中的数字。
- 三项Cookie：浏览器登录bilibili.com → F12 → 应用/Application → Cookie → `https://www.bilibili.com`，分别复制 `SESSDATA`、`buvid3`、`bili_jct` 的值（勿勾选"显示已解码的网址"）。
  - 只收弹幕：可全不填（用户名打码、ID显示为0）。
  - 发弹幕/回复评论：`SESSDATA` + `bili_jct` 必填。

> **SESSDATA等于账号登录凭证，请妥善保管，不要发给任何人。**

**开放平台接入**：B站直播[开放平台](https://open-live.bilibili.com/)官方API，需申请开发者资质，创建项目后填入 `access_key_id`、`access_key_secret`、`app_id`、`room_owner_auth_code` 四项。此方式只能收弹幕，不能发弹幕。

## 功能与配置

### 弹幕机器人（主模式，`work_mode=danmaku_bot`，均在"插件设置"组）

弹幕 → LLM生成回复 → 发回直播间。

| 配置项 | 说明 |
| --- | --- |
| `trigger_prefix` | 触发前缀，如 `#问 `（注意末尾空格）。强烈建议设置，留空则所有弹幕都触发，活跃直播间会积压 |
| `live_persona_prompt` | 直播间人设：机器人的身份、性格、说话风格。自动附加内置输出规则，无需手写，越短越省token |
| `llm_provider_id` | 本插件使用的模型供应商，留空跟随AstrBot当前供应商 |
| `llm_breaker` | LLM连续失败熔断（默认开启）：异常/超时/空响应累计达阈值（默认3次）后暂停调用，冷却（默认120秒）到期自动探测恢复；熔断期间评论区轮询也暂停，恢复后自然补回 |
| `llm_chat_max_context` | 记忆轮数（保存条数=该值×2）。弹幕互动短平快，建议3~5 |
| `allow_message_type` | 处理哪些消息，逗号分隔：`danmaku, gift, guard_buy, super_chat, like, enter_room`。弹幕机器人建议只留 `danmaku` |

机器人会自动以账号身份进入直播间（在线列表可见），并忽略自己发的弹幕，无需额外配置。

### 视频评论区自动回复（`comment_reply` 组）

轮询Y账号（及启用的X账号）消息中心的「收到评论」，由LLM按评论区人设（`comment_persona_prompt`）生成回复，统一用Y账号发布。不直播时也在运行。

| 配置项 | 说明 |
| --- | --- |
| `enable` | 是否启用，默认关闭 |
| `video_context` | 回复前识别所属视频内容：按视频标题/简介生成一句话概括注入提示词，按视频缓存，每个视频仅一次额外LLM调用。默认开启 |
| `poll_interval` | 轮询间隔（秒），默认180，建议不低于120 |
| `min_interval` | 两条回复的最小发送间隔（秒），默认45，勿调低 |
| `random_delay_max` | 每条回复前的额外随机延迟（秒），默认20，0为关闭 |
| `max_replies_per_cycle` | 每轮最多回复条数，默认3，超出顺延 |
| `max_length` | 单条回复最大长度（字），默认120，超出截断 |

注意：

- 首轮轮询只建立基准、不回复历史通知，重启不重复回复；删除数据目录的 `comment_state.json`（`data/plugin_data/astrbot_plugin_bilibili_live_mod/`）可从头再来。
- 「收到评论」**不含**自己视频下无人回复过的全新顶层评论。

### X账号（`account_x` 组，可选）

X账号**只读**：轮询本账号收到的评论并转发给Y统一回复，自身不发送任何内容，风控风险极低。典型用法：X=大号/UP主号，Y=小号承担全部发送。Cookie获取方法与Y账号相同，`bili_jct`/refresh_token可不填（不填则过期后需手动更换）。

### 开播监控（`live_monitor` 组，仅Web接入）

开启后插件不常驻直播间：开播才连接弹幕，下播自动断开。`notify_destinations`填umo（在目标会话发送 `/sid` 获取），开播/下播会推送纯文本通知；QQ官方机器人通道可用，主动推送有平台配额限制。主播"轮播中"视为未开播。

### Cookie自动刷新（`cookie_refresh` 组）

开启后按间隔自动续期Cookie并即时生效，无需手动更换。需在各账号组填写 `refresh_token`：浏览器登录bilibili.com → F12 → 控制台 → 输入 `copy(localStorage.ac_time_value)` 回车，剪贴板中的值即是；也可发送 `/bililogin` 扫码登录自动写入。

### 扫码登录（`qr_login` 组）

Cookie和refresh_token彻底失效时，自动生成B站扫码二维码并推送到「插件设置→转发目标(umo)」，手机扫码后新Cookie自动写回配置并即时生效。需已开启Cookie自动刷新。也可随时发送 `/bililogin`（Y账号）或 `/bililogin x`（X账号）手动触发，首次部署无Cookie时也可用此指令直接登录。

> 刷新或扫码成功后，浏览器/手机端同账号的旧登录态会失效（B站机制），请给机器人专用小号。

### 其他工作模式（测试用）

| 模式 | 行为 |
| --- | --- |
| `forward_only` | 直播间消息原样转发到AstrBot侧，不调用LLM |
| `llm_chat_forward` | LLM回复只转发到AstrBot侧，不发弹幕。适合观察回复效果 |
| `llm_chat_callback` | LLM回复POST/GET到自定义回调地址 |

## 风控要点

1. **一律使用小号**，频率失控或内容违规会被禁言封号。
2. 务必设置 `trigger_prefix`。
3. 不要调小发送间隔（弹幕默认1.5秒、评论默认45秒）；日志出现12015/-412/509说明已被风控盯上，调大间隔、暂停隔天再试。
4. 低等级账号把 `max_length` 调小（弹幕20字）。
5. AI回复内容不可控：先用 `llm_chat_forward` 模式观察几天，满意后再切 `danmaku_bot`；评论区同理，先把 `max_replies_per_cycle` 设为1观察。

## 常见问题

**没有任何消息转发**
- 确认已启用一种接入方式；`allow_message_type` 包含期待的消息类型。
- 开启开播监控时主播没开播属正常。

**弹幕用户名打码 / 用户ID是0**
- `SESSDATA` 缺失或过期，重新复制。

**LLM模式报错"没有可用的模型供应商"**
- 检查AstrBot的模型供应商配置（至少一个启用且密钥有效）。

**弹幕/评论发不出去**
- 看日志错误码：`-101` Cookie过期；`-111` 缺`bili_jct`；发送过快调大 `min_interval`；超长调小 `max_length`。

**评论区自动回复没生效**
- 确认 `comment_reply.enable` 已保存并重启；启动日志应有"视频评论区回复使用Y账号: 昵称(mid)"。
- 首轮轮询只建基准不回复，属正常；等新的收到评论或删除数据目录（`data/plugin_data/astrbot_plugin_bilibili_live_mod/`）下的 `comment_state.json` 重启。

**X账号的评论没被回复**
- 确认 `account_x.enable` 且 `cookie_SESSDATA` 有效；启动日志应有"X账号轮询已启用"。

**开播通知没推到群里**
- umo需在目标群里用 `/sid` 获取（私聊和群聊不同）；查看日志"推送通知失败"报错。

**登录态彻底失效，不想手动换Cookie**
- 开启 `qr_login.enable`，下个检测周期会自动推送扫码二维码；等不及可发 `/bililogin` 手动触发。

---

本插件为 [astrbot_plugin_bilibili_live](https://github.com/Raven95676/astrbot_plugin_bilibili_live) 的功能拓展版本，更新blivedm版本至1.1.7，新增回复功能。
