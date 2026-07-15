# CTP 多账户交易服务

基于 CTP Trade + MD 接口实现的多 Client / 单 Server 交易服务。Server 统一管理 CTP 行情连接、交易账户连接、Client 权限、指令路由和回报推送；Client 作为交易终端发送登录、查询、下单、撤单、登出和行情订阅指令。

## 功能概览

- CTP Trade / MD 双接口接入。
- 1 个共享 MD 行情连接 + 多个按账户创建的 Trade 连接。
- 多 Client 同时连接同一个 Server。
- 账户登录、下单、撤单、资金查询、持仓查询、委托查询、成交查询、账户登出。
- 订单状态和成交回报主动推送到对应 Client。
- Server 端 TOML 白名单权限控制。
- Client TCP 自动重连，并重放注册、登录和订阅消息。

## 快速开始

准备配置：

```bash
cd /home/aaa/quantSystem/ctp_system
cp .env.example .env
```

编辑 `.env`，填写 SimNow 账号、密码、前置地址和 `CTP_CLIENT_ID`。Server 白名单配置在 `config/server.toml`。

编译：

```bash
cargo build -p ctp-server -p ctp-client
```

启动 Server：

```bash
./target/debug/ctp-server
```

启动 Client：

```bash
./target/debug/ctp-client
```

`ctp-client` 默认自动读取当前目录下的 `.env`。如需指定其他 env 文件：

```bash
CTP_CLIENT_ENV_FILE=.env.local ./target/debug/ctp-client
```

## 文档

- [项目架构文档](docs/项目架构与进度.md)
- [机考提交说明](docs/机考提交说明.md)
- [原始题目](机试题目-基于CTP接口的多账户交易服务.md)

## 目录结构

```text
ctp_system/
├── crates/
│   ├── common/      # 协议、网络、日志
│   ├── model/       # 领域模型和 CTP 映射
│   ├── server/      # Server、Actor、CTP Trade/MD adapter
│   └── client/      # Client API 和 demo
├── config/          # Server TOML 配置
├── docs/            # 架构与提交说明
└── .env.example     # 运行环境变量示例
```

## 注意事项

- `.env` 中包含账号密码，不应提交真实敏感信息。
- `flow/` 是 CTP 原生库运行时生成目录，不属于源码；停止程序后可以删除。
- 行情是否持续推送取决于交易时段，收盘后可能没有新 tick。
- 如果修改了 Rust 源码，需要重新 `cargo build` 后再运行 `./target/debug/*`。
