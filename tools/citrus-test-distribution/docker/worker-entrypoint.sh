#!/bin/bash
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

if [ "${CITRUS_DIST_COMPOSE_WORKER:-}" = true ]; then
    {
        read -r CITRUS_DIST_WORKER_POOL
        read -r key_id
        read -r CITRUS_DIST_WORKER_REGISTRATION_KEY
    } < /registration/credentials
    export CITRUS_DIST_WORKER_POOL CITRUS_DIST_WORKER_REGISTRATION_KEY
    # Docker DNS registers the Compose replica name even when the container is recreated.
    ip=$(hostname -i | awk '{print $1}')
    replica=$(getent -s dns hosts "$ip" | sed -n 's/^[^ ]*[[:space:]]*[^ ]*-worker-\([0-9][0-9]*\)\..*/\1/p' | head -1)
    if [ -z "$replica" ]; then
        echo "Could not resolve the Compose worker replica name for $ip" >&2
        exit 1
    fi
    export CITRUS_DIST_WORKER_ID="worker-$replica"
fi

worker_command=("-Dcitrus.test.distribution.worker.handshakeTimeoutMs=${CITRUS_DIST_WORKER_HANDSHAKE_TIMEOUT_MS:-90000}" -cp '/opt/citrus/citrus-jbang.jar:/opt/citrus/lib/*' org.citrusframework.jbang.cli.CitrusJBangMain dist worker start)
case "${CITRUS_DIST_WORKER_MAX_SLOTS:-1}" in
    1) exec java "${worker_command[@]}" "$@" ;;
    2) ;;
    *) echo 'CITRUS_DIST_WORKER_MAX_SLOTS must be 1 or 2' >&2; exit 2 ;;
esac

# Each slot has its own JVM, working directory, cache lock and durable result journal.
# Both register under the same Compose worker ID and drain on container shutdown.
: "${CITRUS_DIST_WORKER_ID:?Two slots require a stable worker ID}"
overflow_dir="$PWD/overflow-slot"
mkdir -p "$overflow_dir"
children=()
stop_slots() {
    trap - TERM INT
    kill -TERM "${children[@]}" 2>/dev/null || true
    wait || true
}
trap 'stop_slots; exit 0' TERM INT
java "${worker_command[@]}" "$@" &
children+=("$!")
(
    cd "$overflow_dir"
    exec java -Dcitrus.test.distribution.worker.overflowSlot=true "${worker_command[@]}" "$@" --cache-dir="$overflow_dir/cache"
) &
children+=("$!")
status=0
wait -n || status=$?
stop_slots
exit "$status"
