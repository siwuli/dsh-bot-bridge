# dsh-bot-bridge

[![npm version](https://img.shields.io/npm/v/dsh-bot-bridge)](https://www.npmjs.com/package/dsh-bot-bridge)
[![license](https://img.shields.io/npm/l/dsh-bot-bridge)](https://github.com/siwuli/dsh-bot-bridge)

DSH (DeepSeek Harness) 宿主端插件：给机器人后端（如 AstrBot）提供一条**稳定、带令牌认证的 HTTP 桥**，让 QQ/微信等聊天机器人把用户消息送进 DSH 的 Agent 会话执行，并把**非思考输出**（模型正文、工具动态、提问/授权）实时流式返回。

- 📦 npm: [dsh-bot-bridge](https://www.npmjs.com/package/dsh-bot-bridge)
- 🐙 GitHub: [siwuli/dsh-bot-bridge](https://github.com/siwuli/dsh-bot-bridge)
- 🤖 配套 AstrBot 插件: [siwuli/AstrBot_siwu-dsh-harness](https://github.com/siwuli/AstrBot_siwu-dsh-harness)

## 解决的问题

DSH 的 Web GUI /api 走浏览器会话（Cookie），且内部 RPC（session.create / session.prompt / events.mux 等）属于实现细节。机器人后端直接对接这些接口会把两者耦合死。本插件在 DSH 内提供一层独立的稳定契约：

- **令牌认证**：Authorization: Bearer <token> 或 x-bot-token，常量时间比较，不依赖 Cookie/登录页；
- **SSE 流**：POST /api/bot/prompt 返回 Server-Sent Events，只转发本次会话事件；
- **非思考过滤**：默认只转发 text-delta（模型正文），reasoning/thinking 增量不输出（includeThinking 可开）；
- **交互回传**：DSH 的 ask_user_question 提问、工具授权请求会以 question/approval 帧推送，机器人可通过 answer/approve 端点回答，同一个 SSE 连接继续收流；
- **会话绑定**：clientId（如 qq:123456）自动绑定/复用 DSH 会话，重启不丢（存 ~/.dsh/storages/dsh-bot-bridge.json）。

## 架构

    QQ 用户
       │ 私聊消息
       ▼
    AstrBot (siwu-dsh-harness 插件)
       │ POST /api/bot/prompt  (Bearer token)
       ▼
    DSH 宿主插件 dsh-bot-bridge
       ├─ ctx.apiProxy.sessions.create/prompt  → 复用 DSH Agent 运行时
       ├─ ctx.apiProxy.events.mux             → 订阅会话事件流
       └─ 过滤/打包后以 SSE 帧回传: text / tool / question / approval / done

本插件只是"桥"：它不持有会话逻辑，全部复用 DSH 自身的会话存储、Agent 与工具运行时；关掉插件不影响 DSH Web GUI。AstrBot 插件只是"客户端"：bridge 不可用时它仍可独立加载、回复错误提示。两边通过上面这一纸 HTTP 契约解耦。

## 安装

    dsh plugin --profile <profile名> add dsh-bot-bridge
    # 本地开发
    dsh plugin --profile <profile名> add link:<插件源码绝对路径>

重启 DSH 生效（例如 dsh --profile first）。

## 配置 (cordis patch)

在 ~/.dsh/profiles/<profile>/cordis.patch.yml 中覆盖（id 与安装一致）：

    - id: bot-bridge
      config:
        botToken: '生成一个足够长的随机令牌'   # 必填
        defaultWorkspacePath: '/data/dsh-bot-workspace'  # 可选: 新会话的工作目录(不填则用 DSH 默认)
        defaultAgentPreset: ''                 # 可选: 新会话使用的 agent 预设 id
        includeThinking: false                 # 是否转发 reasoning/thinking 增量
        includeToolEvents: true                # 是否转发工具调用/结果帧
        doneQuietMs: 3000                      # Agent 空闲多少毫秒后判定本轮完成并关闭 SSE
        maxStreamMs: 1800000                   # 单条 SSE 最长存活时间
        maxPromptChars: 20000                  # 单条提示词长度上限
        maxHistoryItems: 50                    # 历史接口返回的最大条数
        defaultModelProvider: 'deepseek'       # 可选: 新建会话的模型 provider
        defaultModelName: 'deepseek-chat'      # 可选: 新建会话的模型 id
        defaultReasoningEffort: ''             # 可选: 思考强度 (如 low/medium/high)
        storageFile: '~/.dsh/storages/dsh-bot-bridge.json'
        loopbackOnly: false                    # true 时仅允许 127.0.0.1 访问

## API

全部端点都要求令牌头（Authorization: Bearer <token> 或 x-bot-token: <token>）。

### POST /api/bot/prompt

请求 (JSON)：

    {
      "clientId": "qq:123456",   // 可选: 绑定身份, 自动创建/复用会话
      "sessionId": "session-…",  // 可选: 显式指定会话(优先于 clientId)
      "text": "帮我看看仓库状态", // 必填
      "reset": false,            // 可选: true 时先解绑旧会话再新建
      "workspacePath": null,     // 可选: 新建会话时的工作目录
      "agentPreset": null        // 可选: 新建会话时的 agent 预设
    }

响应：SSE 流，每帧一行 JSON（data: {...}）：

    { "type": "status", "status": "starting|accepted|running|answered", "sessionId": "…" }
    { "type": "text",  "text": "模型正文增量" }        // 非思考输出
    { "type": "tool",  "status": "call|result", "name": "ls -la" }
    { "type": "question", "rpcId": "q1", "questions": [ { "id", "question", "options", "multiSelect" } ] }
    { "type": "approval", "rpcId": "a1", "approvalId": "ap1", "toolName": "write", "reason": "…" }
    { "type": "error",  "message": "…" }
    { "type": "done",   "sessionId": "…", "reason": "timeout|agent-error|prompt-rejected" }

请求还支持工作区/模型选择（只在**新建会话**时生效）：

    {
      "clientId": "qq:123456",
      "text": "帮我看看仓库状态",
      "workspacePath": "/data/workspace-a",   // 可选: 新建会话的工作目录
      "modelProvider": "deepseek",            // 可选: 新建会话的模型 provider
      "modelName": "deepseek-reasoner",       // 可选: 新建会话的模型 id
      "reasoningEffort": "high"               // 可选: 思考强度
    }

done 判定：本提示词引发的 turn 全部结束且 Agent 进入 idle，静默 doneQuietMs 后收尾；期间出现 question/approval 会挂起等待应答，Agent 重新 running 会取消倒计时。

### POST /api/bot/answer

    { "sessionId": "…", "rpcId": "q1",
      "answers": [ { "id": "q", "selected": [], "custom": "用户输入的答案" } ] }

回答 ask_user_question 提问（answers 与提问帧的 questions 一一对应）。应答后原 SSE 连接继续推流。

### POST /api/bot/cancel

    { "sessionId": "…" }

打断该会话当前正在执行的 turn。

### POST /api/bot/approve

    { "sessionId": "…", "rpcId": "a1", "approvalId": "ap1", "outcome": "allowed-once" }

outcome: allowed-once（放行一次）或 rejected（拒绝）。

### GET /api/bot/history?sessionId=…&limit=50

返回该会话最近的用户/助手文本（默认排除 thinking），供"查看最近结果"。

### GET /api/bot/workspaces

列出 DSH 当前注册的工作区：[{workspaceId, path, title}]，供机器人端"选择工作区"。

### GET /api/bot/models

列出模型目录：groups（provider → models[id/name]）与 failures，供机器人端"选择模型"。

### POST /api/bot/model

    { "sessionId": "…", "provider": "deepseek", "model": "deepseek-reasoner", "reasoningEffort": "high" }

给**现有会话**切换模型（reasoningEffort 可选）。

### GET /api/bot/session?clientId=…、POST /api/bot/reset、GET /api/bot/health


查询绑定会话 / 解绑（下次自动新建）/ 健康检查。

## 网络拓扑

**默认（推荐）：DSH 与机器人后端同机** —— 无需任何网络配置，机器人直接访问
http://127.0.0.1:3080，bridge 自带令牌认证。

**已有反向代理（Caddy/Nginx/Cloudflare 等）**：如果 DSH 已经通过反代暴露了域名
（如 https://dsh.example.com），机器人直接经该地址访问即可——bridge 自带令牌认证，
无需任何隧道或额外配置。

**跨机部署（可选）**：部分 DSH 版本的 CLI 出于安全禁止把 Web 服务绑定到
0.0.0.0（例如 dsh web --host 0.0.0.0 会被拒绝），此时不要把 DSH 直接暴露到
局域网，推荐用 SSH 反向隧道把 DSH 端口引到机器人所在机器：

    # 在运行 DSH 的机器上执行（断线可配合 autossh/启动脚本自动重连）
    ssh -N -R 13080:127.0.0.1:3080 <机器人机器用户>@<机器人机器IP> \
        -o ServerAliveInterval=30 -o ExitOnForwardFailure=yes

机器人侧即可用 http://127.0.0.1:13080 访问 bridge，全程走 SSH 加密通道、
DSH 仍只监听回环。也可以在机器人同机部署一套 DSH。公网暴露务必使用强令牌，
有条件就配合 dsh-auth-session 或代理层。

## 开发与测试

    npm test   # 核心纯函数测试 + mock DSH 集成测试(令牌/SSE流/提问/授权/历史)

## 版本历史

- v0.2.1: 工作区自动换绑——绑定会话的工作目录与请求 workspacePath 不一致时自动解绑并新建会话（改配置即生效，无需手动重置）。
- v0.2.0: 新增工作区列表 / 模型目录 / 会话切模型端点，prompt 新建会话支持 workspacePath + modelProvider/modelName/reasoningEffort。
- v0.1.0: 首个版本（/api/bot/* 桥接 API、SSE 非思考流、提问/授权/停止应答、clientId 会话绑定、mock 集成测试）。