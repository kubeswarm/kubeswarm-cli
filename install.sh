#!/usr/bin/env bash
set -e

# Define colors for a professional look
BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

main() {
  REPO="kubeswarm/kubeswarm-cli"
  BINARY="swarm"
  INSTALL_DIR="$HOME/.local/bin"

  # --- Welcome Message ---
  echo -e "${BOLD}# Welcome to Swarm!${NC}"
  echo "This script will download and install the latest version of the Swarm CLI."
  echo -e "It will be placed in: ${GREEN}${INSTALL_DIR}${NC}"
  echo "---------------------------------------------------------"

  # --- Helpers ---
  need_cmd() {
    command -v "$1" >/dev/null 2>&1 || {
      echo -e "${RED}Error: '$1' is required but not installed.${NC}" >&2
      exit 1
    }
  }

  verify_checksum() {
    local file=$1
    local url=$2
    echo "Verifying checksum..."
    
    local expected_hash=$(curl -fsSL "${url}.sha256" | awk '{print $1}')
    if [ -z "$expected_hash" ]; then
      echo -e "${YELLOW}Warning: Could not fetch checksum, skipping verification.${NC}"
      return 0
    fi

    local actual_hash
    if command -v sha256sum >/dev/null 2>&1; then
      actual_hash=$(sha256sum "$file" | awk '{print $1}')
    elif command -v shasum >/dev/null 2>&1; then
      actual_hash=$(shasum -a 256 "$file" | awk '{print $1}')
    else
      echo -e "${YELLOW}Warning: No checksum tool available, skipping.${NC}"
      return 0
    fi

    if [ "$expected_hash" != "$actual_hash" ]; then
      echo -e "${RED}Error: Checksum verification failed!${NC}" >&2
      exit 1
    fi
    echo "Checksum verified successfully."
  }

  # --- Environment Setup ---
  need_cmd curl
  need_cmd uname
  need_cmd mktemp
  need_cmd awk
  need_cmd mkdir

  # Ensure local bin exists
  mkdir -p "$INSTALL_DIR"

  # Detect OS/Arch
  OS=$(uname -s | tr '[:upper:]' '[:lower:]')
  ARCH=$(uname -m)
  case "$ARCH" in
    x86_64) ARCH="amd64" ;;
    arm64|aarch64) ARCH="arm64" ;;
  esac

  # --- Version Discovery ---
  echo "Retrieving metadata..."
  VERSION=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep -m 1 '"tag_name"' \
    | cut -d '"' -f 4)

  if [ -z "$VERSION" ]; then
    echo -e "${RED}Failed to resolve latest version.${NC}"
    exit 1
  fi

  ASSET="${BINARY}-${OS}-${ARCH}"
  URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET}"

  # --- Download & Install ---
  echo -e "Installing ${BOLD}${BINARY} ${VERSION}${NC} (${OS}/${ARCH})..."
  
  TMP=$(mktemp 2>/dev/null || mktemp -t swarm)
  trap 'rm -f "$TMP"' EXIT

  if ! curl -fL "$URL" -o "$TMP"; then
    echo -e "${RED}Download failed: $URL${NC}"
    exit 1
  fi

  verify_checksum "$TMP" "$URL"

  # Move to destination (No sudo!)
  install -m 755 "$TMP" "${INSTALL_DIR}/${BINARY}"

  # --- Post-Install Path Check ---
  echo -e "---------------------------------------------------------"
  echo -e "${GREEN}${BOLD}Successfully installed ${BINARY}!${NC}"

  # Check if INSTALL_DIR is in the PATH
  if [[ ":$PATH:" != *":$INSTALL_DIR:"* ]]; then
    echo -e "\n${YELLOW}⚠️  Note: ${INSTALL_DIR} is not in your PATH.${NC}"
    echo "To run '${BINARY}' from anywhere, add this to your shell profile (.zshrc or .bashrc):"
    echo -e "  ${BOLD}export PATH=\"\$PATH:${INSTALL_DIR}\"${NC}"
    echo "Then run 'source ~/.zshrc' or restart your terminal."
  else
    echo "Run: ${BINARY} --help"
  fi
}

main "$@"