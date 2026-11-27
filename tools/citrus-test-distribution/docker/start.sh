#!/bin/sh
#
# Copyright the original author or authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

if [ "$#" -ne 0 ]; then
    printf 'Usage: %s\n' "${0##*/}" >&2
    exit 2
fi

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd -P)
cd "$script_dir"
project_name=${COMPOSE_PROJECT_NAME:-citrus-test-distribution}

compose() {
    docker compose --project-name "$project_name" --file "$script_dir/compose.yaml" "$@"
}

# The socket belongs to the Docker engine host, which may be a Linux VM.
if [ -z "${CITRUS_DIST_DOCKER_GID:-}" ]; then
    engine_socket=${CITRUS_DIST_DOCKER_SOCKET:-/var/run/docker.sock}
    CITRUS_DIST_DOCKER_GID=$(docker run --rm \
        --mount "type=bind,source=$engine_socket,target=/engine.sock" \
        --entrypoint stat postgres:16-alpine -c %g /engine.sock)
    export CITRUS_DIST_DOCKER_GID
fi

compose up --detach --build --wait
coordinator_address=$(compose port coordinator 8765)
if [ -z "$coordinator_address" ]; then
    printf '%s\n' 'The Citrus coordinator has no published address.' >&2
    exit 1
fi

dashboard_url="http://$coordinator_address/ui/"
printf 'Citrus dashboard: %s\n' "$dashboard_url"

case "$(uname -s)" in
    Darwin) launcher=open ;;
    Linux)
        [ -n "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ] || exit 0
        launcher=xdg-open
        ;;
    *) exit 0 ;;
esac

if command -v "$launcher" >/dev/null 2>&1; then
    "$launcher" "$dashboard_url" >/dev/null 2>&1 &
fi
