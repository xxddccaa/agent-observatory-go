#!/bin/sh
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REPO_ROOT="$(CDPATH= cd -- "${SCRIPT_DIR}/.." && pwd)"

INSTALL_DIR="${AGENT_OBS_INSTALL_DIR:-${HOME}/.local/bin}"
DATA_DIR="${AGENT_OBS_HOME:-}"
LISTEN_ADDR="${AGENT_OBS_LISTEN:-}"
SKIP_FRONTEND=0
NO_OPEN=0
INSTALL_ONLY=0
APP_NAME="agent-observatory"
SHORT_ALIAS="aobs"

usage() {
  cat <<EOF
Build ${APP_NAME} (and ${SHORT_ALIAS} short alias) locally for macOS, install into PATH, restart any old daemon, and open the Web UI.

Usage:
  scripts/local-mac.sh [options]

Options:
  --install-dir DIR  Install directory (default: \$HOME/.local/bin)
  --data-dir DIR     Override AGENT_OBS_HOME data directory
  --listen ADDR      Override listen address (default: 127.0.0.1:8125)
  --skip-frontend    Skip rebuilding frontend/ into internal/webui/dist
  --no-open          Restart daemon after install without opening a browser window
  --install-only     Only build and install binaries without starting the daemon
  -h, --help         Show this help message
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --install-dir)
      [ $# -ge 2 ] || { echo "missing value for --install-dir" >&2; exit 2; }
      INSTALL_DIR="$2"
      shift 2
      ;;
    --data-dir)
      [ $# -ge 2 ] || { echo "missing value for --data-dir" >&2; exit 2; }
      DATA_DIR="$2"
      shift 2
      ;;
    --listen)
      [ $# -ge 2 ] || { echo "missing value for --listen" >&2; exit 2; }
      LISTEN_ADDR="$2"
      shift 2
      ;;
    --skip-frontend)
      SKIP_FRONTEND=1
      shift
      ;;
    --no-open)
      NO_OPEN=1
      shift
      ;;
    --install-only)
      INSTALL_ONLY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

cd "$REPO_ROOT"

if [ "$SKIP_FRONTEND" -eq 0 ]; then
  echo "Building frontend SPA -> internal/webui/dist..."
  (
    cd "${REPO_ROOT}/frontend"
    if [ ! -d node_modules ]; then
      if [ -f package-lock.json ]; then
        npm ci
      else
        npm install
      fi
    fi
    npm run build
  )
fi

VERSION="${AGENT_OBS_BUILD_VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo 0.1.0-dev)}"
VERSION="${VERSION#v}"
LDFLAGS="-s -w -X github.com/agent-observatory/agent-observatory-go/internal/config.Version=${VERSION}"

mkdir -p "$INSTALL_DIR"
STAGE_BIN="${INSTALL_DIR}/.${APP_NAME}.tmp.$$"
cleanup() {
  rm -f "$STAGE_BIN"
}
trap cleanup EXIT INT TERM HUP

echo "Compiling ${APP_NAME} (${VERSION})..."
CGO_ENABLED=0 go build -trimpath -ldflags "$LDFLAGS" -o "$STAGE_BIN" ./cmd/agent-observatory
chmod 0755 "$STAGE_BIN"

# Atomic directory entry swap avoids corrupting code signatures if an old binary is running on macOS.
mv -f "$STAGE_BIN" "${INSTALL_DIR}/${APP_NAME}"
ln -sf "${APP_NAME}" "${INSTALL_DIR}/${SHORT_ALIAS}"

INSTALLED_VER="$("${INSTALL_DIR}/${SHORT_ALIAS}" version)"
echo "Installed ${INSTALLED_VER} as ${INSTALL_DIR}/${APP_NAME} and ${INSTALL_DIR}/${SHORT_ALIAS}"

if [ "$INSTALL_ONLY" -eq 1 ]; then
  exit 0
fi

# Stop any running daemon so the freshly built binary and embedded SPA take effect.
if [ -n "$DATA_DIR" ]; then
  if "${INSTALL_DIR}/${SHORT_ALIAS}" status --data-dir "$DATA_DIR" >/dev/null 2>&1; then
    echo "Stopping previous daemon for ${DATA_DIR}..."
    "${INSTALL_DIR}/${SHORT_ALIAS}" stop --data-dir "$DATA_DIR"
  fi
else
  if "${INSTALL_DIR}/${SHORT_ALIAS}" status >/dev/null 2>&1; then
    echo "Stopping previous daemon..."
    "${INSTALL_DIR}/${SHORT_ALIAS}" stop
  fi
fi

set --
if [ -n "$DATA_DIR" ]; then
  set -- "$@" --data-dir "$DATA_DIR"
fi
if [ -n "$LISTEN_ADDR" ]; then
  set -- "$@" --listen "$LISTEN_ADDR"
fi
if [ "$NO_OPEN" -eq 1 ]; then
  set -- "$@" --no-open
fi

echo "Starting ${SHORT_ALIAS}..."
"${INSTALL_DIR}/${SHORT_ALIAS}" "$@"

case ":${PATH:-}:" in
  *":${INSTALL_DIR}:"*) ;;
  *)
    echo "Add ${INSTALL_DIR} to your PATH to invoke '${SHORT_ALIAS}' directly:"
    echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
    ;;
esac
