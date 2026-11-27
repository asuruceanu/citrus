# syntax=docker/dockerfile:1
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

FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    ./mvnw -B -ntp -pl tools/jbang -am install -DskipTests -Dmaven.test.skip=true \
      -Dmaven.source.skip=true -Dmaven.javadoc.skip=true \
    && ./mvnw -B -ntp -pl tools/jbang dependency:copy-dependencies \
      -DincludeScope=runtime -DoutputDirectory=/opt/citrus/lib \
    && cp tools/jbang/target/citrus-jbang-*.jar /opt/citrus/citrus-jbang.jar

FROM build AS playwright-cli
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    ./mvnw -B -ntp -pl connectors/citrus-playwright dependency:copy-dependencies \
      -DincludeGroupIds=com.microsoft.playwright -DincludeScope=runtime -DoutputDirectory=/opt/playwright-cli

FROM eclipse-temurin:21-jdk-jammy AS runtime
RUN useradd --uid 10001 --user-group --create-home citrus \
    && install -d -o citrus -g citrus /var/lib/citrus
COPY --from=build /opt/citrus/ /opt/citrus/
USER citrus
WORKDIR /var/lib/citrus

FROM runtime AS coordinator
EXPOSE 8765
ENTRYPOINT ["java", "-cp", "/opt/citrus/citrus-jbang.jar:/opt/citrus/lib/*", "org.citrusframework.jbang.cli.CitrusJBangMain", "dist", "coordinator", "start"]

FROM docker:29.9.2-cli AS docker-cli

FROM runtime AS worker
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=docker-cli /usr/local/libexec/docker/cli-plugins/ /usr/local/libexec/docker/cli-plugins/
USER root
ADD --checksum=sha256:1388fcc2258e2bdcd277c68a5aad5a9d7c1dc8b57ef06165a45da02c2ed9775b https://github.com/jbangdev/jbang/releases/download/v0.143.0/jbang-0.143.0.tar /tmp/jbang.tar
RUN apt-get update && apt-get install -y --no-install-recommends git libatomic1 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/jbang && tar -xf /tmp/jbang.tar --strip-components=1 -C /opt/jbang \
    && rm /tmp/jbang.tar
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright-browsers
RUN --mount=type=bind,from=playwright-cli,source=/opt/playwright-cli,target=/tmp/playwright-cli \
    java -cp '/tmp/playwright-cli/*:/opt/citrus/lib/*' com.microsoft.playwright.CLI install --with-deps chromium \
    && rm -rf /var/lib/apt/lists/*
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1
ENV PATH="/opt/jbang/bin:${PATH}"
USER citrus
ENV CITRUS_DIST_WORKER_CACHE_DIR=/var/lib/citrus/cache
COPY --chmod=755 tools/citrus-test-distribution/docker/worker-entrypoint.sh /usr/local/bin/citrus-worker
ENTRYPOINT ["/usr/local/bin/citrus-worker"]
