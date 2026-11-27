# Agent Observatory Go 项目入口说明

> **`.agent-forge/` 会话约定**
> - 每次新开会话，先读取 [.agent-forge/STATE.md](.agent-forge/STATE.md)、[.agent-forge/constitution.md](.agent-forge/constitution.md) 和 [.agent-forge/specs/architecture.md](.agent-forge/specs/architecture.md)（按修改模块按需读 `.agent-forge/specs/domains/`）。
> - **改动分级**：
>   - **L0（1–2 文件小修/单点 Bug）**：先写复现失败测试 → 最小修复 → 跑全量检查，无需建变更文档。
>   - **L1（常规增量功能/新适配器）**：先在 `.agent-forge/changes/active/` 建设计文档，完成后同步 `.agent-forge/specs/` 并归档至 `archive/`。
>   - **L2（架构/Schema/安全边界变更）**：先在 `.agent-forge/decisions/` 追加 ADR 并更新 `architecture.md` / `data-model.md`，待确认后再编码。

## 项目定位

`agent-observatory` 是**本地优先、只读、单人无登录**的多 coding agent 会话观测台（Go + SQLite 单文件单二进制架构，内嵌 Vue 3 SPA）。

- 默认以“单实例本地守护启动器 + 同类服务感知的端口顺延”模式运行：直接执行 `agent-observatory`（或短命令别名 `aobs`）时后台启动并打开浏览器；提供 `--foreground`、`status`、`stop`、`index --once` 与 `version` 子命令。
- 唯一持久化数据库为单文件 SQLite（`modernc.org/sqlite` 纯 Go 驱动，开启 `WAL` 模式，原始事件 `raw_json` 使用 `zstd` 压缩存储）。
- **本机 macOS 开发后交付约定**：在本机完成功能/修复开发并跑通下方全量验证后，执行 `./scripts/local-mac.sh`（重编前端与 macOS 二进制，安装 `agent-observatory` 和短命令 `aobs` 到 `~/.local/bin`，重启旧守护进程并自动在浏览器打开 Web 页面供查看效果）。

## 不可触碰边界

1. 严格只读各 coding agent 原始目录（`~/.codex`、`~/.claude`、`~/.gemini` 等），绝不修改或删除原文件。
2. 不修改相邻参考仓库 `/Users/xd/Developer/AgentObservatory`（仅作只读对照）。
3. 浏览器自动拉起仅允许经过校验的本机回环地址；默认仅监听 `127.0.0.1`，开启 Host 白名单与写请求同源检查。
4. 源码注释与 docstring 默认使用英文（只写反直觉 `Why`，不写复述代码的 `What` 或修改日志）。

## 常用验证命令（骨架建成后生效）

```bash
go vet ./...
go test -race ./...
cd frontend && LC_ALL=C.UTF-8 TZ=UTC npm run check
```
