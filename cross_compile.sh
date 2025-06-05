set -e

echo $(uname -m)
echo $TARGETARCH
echo $TARGETOS

if [[ $(uname -m) = "amd64" ]]; then
  if [[ "${TARGETARCH}" = "aarch64" ]]; then
    rpm -i https://rpmfind.net/linux/opensuse/tumbleweed/repo/oss/x86_64/libisl23-0.27-1.2.x86_64.rpm
    zypper install -y cross-aarch64-gcc14
    export CC=aarch64-suse-linux-gcc-14
  fi
elif [[ $(uname -m) = "aarch64" ]]; then
  if [[ "${TARGETARCH}" = "amd64" ]]; then
    rpm -i https://rpmfind.net/linux/opensuse/ports/aarch64/tumbleweed/repo/oss/aarch64/libisl23-0.27-1.4.aarch64.rpm
    zypper install -y cross-x86_64-gcc14
    export CC=x86_64-suse-linux-gcc-14
  fi
fi

export C_INCLUDE_PATH=/usr/lib64/gcc/x86_64-suse-linux/14/include

CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} CC=$CC \
    go build -ldflags "-X 'main.projectVersion=${VERSION:-0.0.0}' -X 'main.projectBuild=${BUILD:-dev}'" -o bin/qdrant-migration main.go