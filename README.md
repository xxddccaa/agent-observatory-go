# agent-observatory-go (`aobs`)

本地优先、只读、单人无登录的多 coding agent 会话观测台。采用 Go + 纯 Go SQLite（`WAL` 模式 + `zstd` 压缩原始事件）单文件二进制交付，内嵌 Vue 3 SPA。

## 支持的 Agent 数据源（严格只读）

- **Codex**（`~/.codex/sessions`）
- **Claude Code**（`~/.claude/projects`）
- **Gemini CLI**（`~/.gemini/tmp`）
- **Antigravity CLI (`agy`)**（`~/.gemini/antigravity-cli/brain`）
- **XDCode**（`~/.xdcode/sessions`、`~/.xdcode/archived_sessions`）
- **Pi**（`~/.pi/agent/sessions`）
- **DSH**（`~/.dsh/sessions`，流式解压 `session.jsonl.zstd`）
- **Generic JSONL**：通过 `AGENT_OBS_EXTRA_ROOTS="agent:/path"` 追加任意自定义目录

绝不修改、重命名或删除任何原始会话文件。

## 安装

### macOS / Linux

```sh
curl -fsSL https://raw.githubusercontent.com/xxddccaa/agent-observatory-go/main/scripts/install.sh | sh
```

校验 `checksums.txt` SHA-256 后安装 `agent-observatory` 与短命令别名 `aobs` 至 `~/.local/bin`（可通过 `AGENT_OBS_INSTALL_DIR` 或 `--version` 覆盖）。

### Windows (PowerShell)

```powershell
irm https://raw.githubusercontent.com/xxddccaa/agent-observatory-go/main/scripts/install.ps1 | iex
```

校验 SHA-256 后安装 `agent-observatory.exe` 与 `aobs.cmd` 至 `%LocalAppData%\agent-observatory\bin` 并写入用户 `PATH`。

### 升级与卸载

- **升级**：重新执行上面的安装脚本即可原子替换二进制文件（不会删除 SQLite 数据目录 `~/.agent-observatory-go`）。
- **卸载**：先执行 `aobs stop`，再删除安装的可执行文件及数据目录：
  ```sh
  rm -f ~/.local/bin/agent-observatory ~/.local/bin/aobs
  # 如需同时清除本地索引库（不影响各 agent 原始文件）：rm -rf ~/.agent-observatory-go
  ```

## 常用指令

安装后可直接使用短命令 `aobs`（等价于 `agent-observatory`）：

```sh
aobs                        # 后台单实例启动并自动在浏览器打开 Web UI（默认 127.0.0.1:8125）
aobs --no-open              # 仅后台启动，不自动打开浏览器
aobs status                 # 查看 PID、监听地址、URL 与日志路径
aobs stop                   # 优雅停机
aobs index --once           # 单次全量增量扫描入库并退出
aobs --foreground --no-open # 前台调试运行
aobs version                # 打印当前版本号
```

若默认端口被其他程序占用，自动探测 `/api/meta` 与 `X-Agent-Observatory-Service` 头：同软件实例直接复用并打开浏览器，其他程序占用则自动 `+1` 顺延最多 20 个端口。

## 本地开发与校验

```sh
go vet ./...
go test -race ./...
cd frontend && npm ci && LC_ALL=C.UTF-8 TZ=UTC npm run check && npm run build
```

macOS 开发后一键重编、安装到 `~/.local/bin/{agent-observatory,aobs}`、重启旧守护进程并打开浏览器：

```sh
./scripts/local-mac.sh
```
