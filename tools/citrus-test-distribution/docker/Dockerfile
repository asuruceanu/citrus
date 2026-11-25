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

FROM eclipse-temurin:21-jdk-jammy AS runtime
RUN useradd --uid 10001 --user-group --create-home citrus \
    && install -d -o citrus -g citrus /var/lib/citrus
COPY --from=build /opt/citrus/ /opt/citrus/
USER citrus
WORKDIR /var/lib/citrus

FROM runtime AS coordinator
EXPOSE 8765
ENTRYPOINT ["java", "-cp", "/opt/citrus/citrus-jbang.jar:/opt/citrus/lib/*", "org.citrusframework.jbang.cli.CitrusJBangMain", "dist", "coordinator", "start"]

FROM runtime AS worker
ENV CITRUS_DIST_WORKER_CACHE_DIR=/var/lib/citrus/cache
ENTRYPOINT ["java", "-cp", "/opt/citrus/citrus-jbang.jar:/opt/citrus/lib/*", "org.citrusframework.jbang.cli.CitrusJBangMain", "dist", "worker", "start"]
