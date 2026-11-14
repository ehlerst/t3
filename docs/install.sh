#!/usr/bin/env sh
# t3 (Turbo 3) One-Line Installer for Linux & macOS (amd64 / arm64)
# Usage:
#   curl -sSfL https://ehlerst.github.io/t3/install.sh | sh
#   curl -sSfL https://raw.githubusercontent.com/ehlerst/t3/main/install.sh | sh
#
# Environment Variables:
#   T3_VERSION      Release tag to install (default: "latest", e.g. "v0.1.2")
#   T3_INSTALL_DIR  Destination directory (default: /usr/local/bin or ~/.local/bin)

set -eu

REPO="ehlerst/t3"
VERSION="${T3_VERSION:-latest}"

uname_s="$(uname -s)"
case "${uname_s}" in
  Linux*)  OS="linux" ;;
  Darwin*) OS="darwin" ;;
  *)
    echo "Error: t3 only supports *nix operating systems (Linux, macOS). Detected: ${uname_s}" >&2
    exit 1
    ;;
esac

uname_m="$(uname -m)"
case "${uname_m}" in
  x86_64|amd64)  ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *)
    echo "Error: Unsupported architecture '${uname_m}'. Supported: amd64 (x86_64), arm64 (aarch64)." >&2
    exit 1
    ;;
esac

ARTIFACT="t3-${OS}-${ARCH}"
if [ "${VERSION}" = "latest" ]; then
  BASE_URL="https://github.com/${REPO}/releases/latest/download"
else
  BASE_URL="https://github.com/${REPO}/releases/download/${VERSION}"
fi

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT INT TERM

echo "==> Downloading ${ARTIFACT} (${VERSION}) from ${BASE_URL}..."
curl -sSfL "${BASE_URL}/${ARTIFACT}.tar.gz" -o "${TMP_DIR}/${ARTIFACT}.tar.gz"
curl -sSfL "${BASE_URL}/${ARTIFACT}.tar.gz.sha256" -o "${TMP_DIR}/${ARTIFACT}.tar.gz.sha256"

echo "==> Verifying SHA-256 checksum..."
(
  cd "${TMP_DIR}"
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum -c "${ARTIFACT}.tar.gz.sha256"
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 -c "${ARTIFACT}.tar.gz.sha256"
  else
    echo "Warning: neither sha256sum nor shasum found; skipping checksum verification." >&2
  fi
)

tar -xzf "${TMP_DIR}/${ARTIFACT}.tar.gz" -C "${TMP_DIR}"
chmod +x "${TMP_DIR}/t3"

if [ -n "${T3_INSTALL_DIR:-}" ]; then
  INSTALL_DIR="${T3_INSTALL_DIR}"
elif [ -w "/usr/local/bin" ]; then
  INSTALL_DIR="/usr/local/bin"
elif command -v sudo >/dev/null 2>&1; then
  INSTALL_DIR="/usr/local/bin"
  USE_SUDO=1
else
  INSTALL_DIR="${HOME}/.local/bin"
fi

mkdir -p "${INSTALL_DIR}" 2>/dev/null || true

if [ "${USE_SUDO:-0}" = "1" ] && [ ! -w "${INSTALL_DIR}" ]; then
  echo "==> Installing t3 to ${INSTALL_DIR}/t3 (using sudo)..."
  sudo mv "${TMP_DIR}/t3" "${INSTALL_DIR}/t3"
else
  echo "==> Installing t3 to ${INSTALL_DIR}/t3..."
  mv "${TMP_DIR}/t3" "${INSTALL_DIR}/t3"
fi

echo "==> Installed successfully: $(${INSTALL_DIR}/t3 version)"
