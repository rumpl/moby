#!/bin/sh

set -e

errexit() {
	echo "$1"
	exit 1
}

[ "$(uname -s)" == "Darwin" ] || errexit "This script can only be used on a Mac"

[ $# -eq 1 ] || errexit "Usage: $0 install|undo"

BACKUP_PATH="$HOME/Desktop/services.tar.bak"
DOCKER_PATH="/Applications/Docker.app"
BUNDLE_PATH="$(pwd)/bundles"
TMP_PATH="$(mktemp -d)"
SERVICES_BIN_PATH="$TMP_PATH/containers/services/docker/lower/usr/bin"

BINARY_LIST="containerd containerd-shim-runc-v2 docker-init runc"
DYN_BINARY_LIST="docker-proxy dockerd"

[ -d "$DOCKER_PATH" ] || errexit "Docker for Mac must be installed for this script"

# Stop Docker Desktop
killall "Docker" 2> /dev/null || true
while pgrep -q "com.docker.driver*"; do
    echo "Waiting for Docker Desktop to stop..."
    sleep 1
done
killall "Docker Desktop" 2> /dev/null || true

replace_services() {
    cp "$1" "$DOCKER_PATH/Contents/Resources/linuxkit/services.tar"
}

case "$1" in
    "install")
        cp "$DOCKER_PATH/Contents/Resources/linuxkit/services.tar" "$TMP_PATH"
        tar -C "$TMP_PATH" -xf "$TMP_PATH/services.tar"
        mv "$TMP_PATH/services.tar" "$BACKUP_PATH"
        for f in $BINARY_LIST; do
            p="$BUNDLE_PATH/binary-daemon/$f"
            [ -f "$p" ] || errexit "You need to build \"$f\" using \`docker buildx bake binary\`"
            cp "$p" "$SERVICES_BIN_PATH/"
        done
        for f in $DYN_BINARY_LIST; do
            p="$BUNDLE_PATH/dynbinary-daemon/$f"
            [ -f "$p" ] || errexit "You need to build \"$f\" using \`docker buildx bake dynbinary\`"
            cp "$p" "$SERVICES_BIN_PATH/"
        done
        # Add dockerd that uses containerd
        cp "$BUNDLE_PATH/dynbinary-daemon/dockerd" "$SERVICES_BIN_PATH/dockerd-c8d"
        tar -C "$TMP_PATH" -cf "$TMP_PATH/services.tar" containers
        replace_services "$TMP_PATH/services.tar"
        echo "Replaced bundle"
        ;;
    "undo")
        replace_services "$BACKUP_PATH"
        echo "Restored backup bundle from $BACKUP_PATH"
        ;;
esac

echo "Restarting Docker Desktop..."
open /Applications/Docker.app
