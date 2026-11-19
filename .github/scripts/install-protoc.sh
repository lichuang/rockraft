#!/usr/bin/env bash
#
# Install a pinned protoc release.
#
# The `arduino/setup-protoc` action resolves the requested version by paging
# through *every* release of protocolbuffers/protobuf over the GitHub REST API,
# even when `version` names an exact tag. That step is slow and fails whenever
# the API answers with a 5xx ("Error: Server Error"), which took down a nightly
# run. Release asset URLs are stable, so download the pinned archive directly.

set -euo pipefail

version="${PROTOC_VERSION:-36.2}"
version="${version#v}"

case "$(uname -s)" in
  Linux) os=linux ;;
  Darwin) os=osx ;;
  *)
    echo "install-protoc: unsupported OS '$(uname -s)'" >&2
    exit 1
    ;;
esac

case "$(uname -m)" in
  x86_64 | amd64) arch=x86_64 ;;
  aarch64 | arm64) arch=aarch_64 ;;
  *)
    echo "install-protoc: unsupported architecture '$(uname -m)'" >&2
    exit 1
    ;;
esac

dest="${HOME}/.local/protoc/${version}"
if [ -x "${dest}/bin/protoc" ]; then
  echo "install-protoc: protoc ${version} already installed at ${dest}"
else
  asset="protoc-${version}-${os}-${arch}.zip"
  url="https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${asset}"

  tmp="$(mktemp -d)"
  trap 'rm -rf "${tmp}"' EXIT

  echo "install-protoc: downloading ${url}"
  curl -fsSL --retry 5 --retry-delay 2 --retry-all-errors -o "${tmp}/${asset}" "${url}"
  unzip -q "${tmp}/${asset}" -d "${tmp}/out"

  mkdir -p "${dest}"
  cp -R "${tmp}/out/." "${dest}/"
  chmod +x "${dest}/bin/protoc"
fi

if [ -n "${GITHUB_PATH:-}" ]; then
  echo "${dest}/bin" >> "${GITHUB_PATH}"
else
  export PATH="${dest}/bin:${PATH}"
fi

"${dest}/bin/protoc" --version
