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

usage() {
    printf 'Usage: %s -all | -module <artifactId-or-module-path>\n' "${0##*/}"
}

usage_error() {
    usage >&2
    exit 2
}

selector=
case "${1:-}" in
    -h|--help)
        usage
        exit 0
        ;;
    -all)
        [ "$#" -eq 1 ] || usage_error
        ;;
    -module)
        [ "$#" -eq 2 ] || usage_error
        case "$2" in
            ''|-*|!*|*,*) usage_error ;;
            *[/:]*) selector=$2 ;;
            *) selector=:$2 ;;
        esac
        ;;
    *)
        usage_error
        ;;
esac

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd -P)
cd "$script_dir"
project_name=${COMPOSE_PROJECT_NAME:-citrus-test-distribution}
coordinator_url=${CITRUS_DIST_COORDINATOR_URL:-}
if [ -z "$coordinator_url" ]; then
    if ! address=$(docker compose --project-name "$project_name" --file "$script_dir/compose.yaml" \
        port coordinator 8765) || [ -z "$address" ]; then
        printf 'The Compose coordinator has no published address. Run %s/start.sh first.\n' "$script_dir" >&2
        exit 1
    fi
    coordinator_url="http://$address"
fi

repo_dir=$(CDPATH= cd "$script_dir/../../.." && pwd -P)
cd "$repo_dir"
local_repo="$repo_dir/target/citrus-distribution/maven-repository"

maven() {
    maven_base=$1
    shift
    MAVEN_BASEDIR="$maven_base" ./mvnw -B -ntp \
        "-Dmaven.repo.local=$local_repo" "-Dmaven.repo.local.tail=$HOME/.m2/repository" "$@"
}

version=$(maven "$repo_dir" -q -N help:evaluate -Dexpression=project.version -DforceStdout -Dstyle.color=never)
case "$version" in
    ''|*[!a-zA-Z0-9._+-]*)
        printf '%s\n' 'Could not determine the Citrus Maven project version.' >&2
        exit 1
        ;;
esac

extension_id=citrus-test-distribution-maven-extension
bootstrap_projects=":$extension_id"
if [ -n "$selector" ]; then
    bootstrap_projects="$bootstrap_projects,$selector"
fi

# Maven loads core extensions before it builds the reactor.
maven "$repo_dir" -pl "$bootstrap_projects" -am install -DskipTests \
    -Dmaven.source.skip=true -Dmaven.javadoc.skip=true -Dcitrus.test.distribution.enabled=false

# Use an invocation-specific descriptor so the checkout's Maven config stays untouched.
settings_dir=$(mktemp -d "${TMPDIR:-/tmp}/citrus-dist-maven.XXXXXX")
trap 'rm -r "$settings_dir"' 0
trap 'exit 130' INT
trap 'exit 143' TERM
cp -R .mvn "$settings_dir/.mvn"
awk -v version="$version" -v artifact="$extension_id" '
    index($0, "<artifactId>" artifact "</artifactId>") { found = 1 }
    /<\/extensions>[[:space:]]*$/ && !found {
        print "  <extension>"
        print "    <groupId>org.citrusframework.mvn</groupId>"
        print "    <artifactId>" artifact "</artifactId>"
        print "    <version>" version "</version>"
        print "  </extension>"
    }
    { print }
' .mvn/extensions.xml > "$settings_dir/.mvn/extensions.xml"

if [ -n "$selector" ]; then
    set -- -pl "$selector"
else
    set --
fi

maven "$settings_dir" "$@" --fail-at-end verify \
    -Dcitrus.test.distribution.enabled=true \
    "-Dcitrus.test.distribution.coordinatorUrl=$coordinator_url" \
    -Dcitrus.test.distribution.fallbackLocal=false \
    -Dcitrus.test.distribution.minRemoteDurationMs=0 \
    -Dcitrus.test.distribution.minRemoteColdUnits=0
