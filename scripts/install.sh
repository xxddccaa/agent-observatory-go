#!/bin/sh
set -eu

REPO="${AGENT_OBS_REPO:-agent-observatory/agent-observatory-go}"
INSTALL_DIR="${AGENT_OBS_INSTALL_DIR:-${HOME}/.local/bin}"
VERSION="${AGENT_OBS_VERSION:-}"
BASE_URL="${AGENT_OBS_BASE_URL:-}"
DRY_RUN=0
APP_NAME="agent-observatory"

usage() {
  cat <<EOF
Install ${APP_NAME} from GitHub Releases with SHA-256 verification.

Usage:
  install.sh [--version vX.Y.Z] [--install-dir DIR] [--dry-run]

Environment variables:
  AGENT_OBS_VERSION      Version tag to install (defaults to latest release)
  AGENT_OBS_INSTALL_DIR  Destination bin directory (default: \$HOME/.local/bin)
  AGENT_OBS_REPO         GitHub owner/repo (default: agent-observatory/agent-observatory-go)
  AGENT_OBS_BASE_URL     Override archive base URL (e.g. local file:// or mirror)
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version)
      [ $# -ge 2 ] || { echo "missing value for --version" >&2; exit 2; }
      VERSION="$2"
      shift 2
      ;;
    --install-dir)
      [ $# -ge 2 ] || { echo "missing value for --install-dir" >&2; exit 2; }
      INSTALL_DIR="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
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

detect_os() {
  case "$(uname -s)" in
    Darwin) echo "darwin" ;;
    Linux) echo "linux" ;;
    *)
      echo "unsupported operating system: $(uname -s)" >&2
      exit 1
      ;;
  esac
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo "amd64" ;;
    arm64|aarch64) echo "arm64" ;;
    *)
      echo "unsupported CPU architecture: $(uname -m)" >&2
      exit 1
      ;;
  esac
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print tolower($1)}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print tolower($1)}'
  elif command -v openssl >/dev/null 2>&1; then
    openssl dgst -sha256 "$1" | awk '{print tolower($NF)}'
  else
    echo "no SHA-256 tool found (need sha256sum, shasum, or openssl)" >&2
    exit 1
  fi
}

fetch_text() {
  curl -fsSL "$1"
}

download_file() {
  curl -fsSL --output "$2" "$1"
}

resolve_latest_version() {
  api_url="https://api.github.com/repos/${REPO}/releases/latest"
  tag="$(fetch_text "$api_url" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)"
  if [ -z "$tag" ]; then
    echo "failed to resolve latest release tag from ${api_url}; pass --version explicitly" >&2
    exit 1
  fi
  echo "$tag"
}

OS="$(detect_os)"
ARCH="$(detect_arch)"

if [ -z "$VERSION" ]; then
  if [ -n "$BASE_URL" ]; then
    echo "AGENT_OBS_VERSION / --version is required when AGENT_OBS_BASE_URL is set" >&2
    exit 2
  fi
  VERSION="$(resolve_latest_version)"
fi

VERSION_NUM="${VERSION#v}"
ARCHIVE="${APP_NAME}_${VERSION_NUM}_${OS}_${ARCH}.tar.gz"

if [ -z "$BASE_URL" ]; then
  BASE_URL="https://github.com/${REPO}/releases/download/v${VERSION_NUM}"
fi
BASE_URL="${BASE_URL%/}"
ARCHIVE_URL="${BASE_URL}/${ARCHIVE}"
CHECKSUMS_URL="${BASE_URL}/checksums.txt"

if [ "$DRY_RUN" -eq 1 ]; then
  echo "dry-run: os=${OS} arch=${ARCH} version=v${VERSION_NUM}"
  echo "dry-run: archive=${ARCHIVE_URL}"
  echo "dry-run: checksums=${CHECKSUMS_URL}"
  echo "dry-run: install_path=${INSTALL_DIR}/${APP_NAME}"
  exit 0
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT INT TERM HUP

echo "Downloading ${ARCHIVE_URL}..."
download_file "$ARCHIVE_URL" "${TMP_DIR}/${ARCHIVE}"
download_file "$CHECKSUMS_URL" "${TMP_DIR}/checksums.txt"

EXPECTED_SHA="$(awk -v name="$ARCHIVE" '$2 == name { print tolower($1); exit }' "${TMP_DIR}/checksums.txt")"
if [ -z "$EXPECTED_SHA" ]; then
  echo "checksum entry missing for ${ARCHIVE} in checksums.txt" >&2
  exit 1
fi

ACTUAL_SHA="$(sha256_file "${TMP_DIR}/${ARCHIVE}")"
if [ "$ACTUAL_SHA" != "$EXPECTED_SHA" ]; then
  echo "SHA-256 verification failed for ${ARCHIVE}: expected ${EXPECTED_SHA}, got ${ACTUAL_SHA}" >&2
  exit 1
fi

tar -xzf "${TMP_DIR}/${ARCHIVE}" -C "$TMP_DIR"
if [ ! -f "${TMP_DIR}/${APP_NAME}" ]; then
  echo "archive did not contain expected binary ${APP_NAME}" >&2
  exit 1
fi

mkdir -p "$INSTALL_DIR"
STAGE_BIN="${INSTALL_DIR}/.${APP_NAME}.tmp.$$"
cp "${TMP_DIR}/${APP_NAME}" "$STAGE_BIN"
chmod 0755 "$STAGE_BIN"
# Stage inside destination directory so mv is an atomic same-filesystem replace.
mv -f "$STAGE_BIN" "${INSTALL_DIR}/${APP_NAME}"
ln -sf "${APP_NAME}" "${INSTALL_DIR}/aobs"

INSTALLED_VERSION="$("${INSTALL_DIR}/${APP_NAME}" --version 2>/dev/null || true)"
echo "Installed ${INSTALLED_VERSION:-${APP_NAME}} to ${INSTALL_DIR}/${APP_NAME} (short alias: ${INSTALL_DIR}/aobs)"

case ":${PATH:-}:" in
  *":${INSTALL_DIR}:"*) ;;
  *)
    echo "Add ${INSTALL_DIR} to your PATH:"
    echo "  export PATH=\"${INSTALL_DIR}:\$PATH\""
    ;;
esac
