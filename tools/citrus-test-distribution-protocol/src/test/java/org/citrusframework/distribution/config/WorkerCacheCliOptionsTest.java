/*
 * Copyright the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.citrusframework.distribution.config;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.citrusframework.distribution.config.WorkerCacheCliOptions.Result;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerCacheCliOptionsTest {

    private static final Set<String> HOST_VALUE_OPTIONS = Set.of("--coordinator-url", "-u", "--auth-token");

    private static Result parse(String... args) {
        return WorkerCacheCliOptions.parse(List.of(args), HOST_VALUE_OPTIONS);
    }

    @Test
    void shouldKeepShortCoordinatorUrlAfterBareCacheCleanup() {
        Result result = WorkerCacheCliOptions.parse(
                List.of("--no-cache-cleanup", "-u", "http://coordinator:8765"), Set.of("--coordinator-url", "-u"));
        assertThat(result.remainingArgs()).containsExactly("-u", "http://coordinator:8765");
        assertThat(result.cacheCleanup()).isFalse();
    }

    @Test
    void shouldReturnUnsetSettingsAndAllArgsWhenNoCacheOptionIsGiven() {
        Result result = parse("--coordinator-url", "http://coordinator:9000", "--verbose");
        assertThat(result.cacheDir()).isNull();
        assertThat(result.cacheCleanup()).isNull();
        assertThat(result.cacheFreeSpaceBufferSize()).isNull();
        assertThat(result.cacheFreeSpaceBufferBytes()).isNull();
        assertThat(result.remainingArgs()).containsExactly("--coordinator-url", "http://coordinator:9000", "--verbose");
        assertThat(result.canonicalArgs()).isEmpty();
    }

    @TestFactory
    Stream<DynamicTest> shouldParseCacheDir() {
        return Stream.of(
                        List.of("--cache-dir", "/tmp/c1"),
                        List.of("--cache-dir=/tmp/c1"),
                        List.of("--cache-dir=/tmp/other", "--cache-dir", "/tmp/c1"))
                .map(args -> DynamicTest.dynamicTest(String.join(" ", args), () -> {
                    Result result = parse(args.toArray(String[]::new));
                    assertThat(result.cacheDir()).isEqualTo("/tmp/c1");
                    assertThat(result.remainingArgs()).isEmpty();
                    assertThat(result.canonicalArgs()).containsExactly("--cache-dir=/tmp/c1");
                }));
    }

    @Test
    void shouldPreserveWhitespaceInCacheDir() {
        assertThat(parse("--cache-dir", " /tmp/c1 ").cacheDir()).isEqualTo(" /tmp/c1 ");
        assertThat(parse("--cache-dir= /tmp/c1 ").canonicalArgs()).containsExactly("--cache-dir= /tmp/c1 ");
    }

    @TestFactory
    Stream<DynamicTest> shouldParseCacheCleanup() {
        return Stream.of(
                cleanupCase(true, "--cache-cleanup"),
                cleanupCase(true, "--cache-cleanup=true"),
                cleanupCase(true, "--cache-cleanup", "true"),
                cleanupCase(true, "--cache-cleanup", "TRUE"),
                cleanupCase(false, "--cache-cleanup=false"),
                cleanupCase(false, "--cache-cleanup", "false"),
                cleanupCase(false, "--no-cache-cleanup"),
                cleanupCase(false, "--no-cache-cleanup=true"),
                cleanupCase(false, "--no-cache-cleanup", "true"),
                cleanupCase(true, "--no-cache-cleanup=false"),
                cleanupCase(true, "--no-cache-cleanup", "false"),
                cleanupCase(true, "--no-cache-cleanup", "--cache-cleanup"),
                cleanupCase(true, "--no-cache-cleanup", "--cache-cleanup=true"),
                cleanupCase(false, "--cache-cleanup", "--no-cache-cleanup"),
                cleanupCase(false, "--cache-cleanup=true", "--cache-cleanup", "false"));
    }

    private static DynamicTest cleanupCase(boolean expected, String... args) {
        return DynamicTest.dynamicTest(String.join(" ", args) + " -> " + expected, () -> {
            Result result = parse(args);
            assertThat(result.cacheCleanup()).isEqualTo(expected);
            assertThat(result.remainingArgs()).isEmpty();
            assertThat(result.canonicalArgs()).containsExactly("--cache-cleanup=" + expected);
        });
    }

    @TestFactory
    Stream<DynamicTest> shouldParseCacheFreeSpaceBufferSize() {
        return Stream.of(
                        List.of("--cache-free-space-buffer-size", "512MiB"),
                        List.of("--cache-free-space-buffer-size=512MiB"),
                        List.of("--cache-free-space-buffer-size=1GiB", "--cache-free-space-buffer-size", "512MiB"))
                .map(args -> DynamicTest.dynamicTest(String.join(" ", args), () -> {
                    Result result = parse(args.toArray(String[]::new));
                    assertThat(result.cacheFreeSpaceBufferSize()).isEqualTo("512MiB");
                    assertThat(result.cacheFreeSpaceBufferBytes()).isEqualTo(536_870_912L);
                    assertThat(result.canonicalArgs()).containsExactly("--cache-free-space-buffer-size=512MiB");
                }));
    }

    @Test
    void shouldNotConsumeFollowingFlagAfterBareCacheCleanup() {
        Result result = parse("--no-cache-cleanup", "--coordinator-url", "http://coordinator:9000");
        assertThat(result.cacheCleanup()).isFalse();
        assertThat(result.remainingArgs()).containsExactly("--coordinator-url", "http://coordinator:9000");
    }

    @Test
    void shouldKeepRemainingArgsInOriginalOrder() {
        Result result = parse("-v", "--cache-dir", "/tmp/c1", "--coordinator-url", "http://b", "--no-cache-cleanup",
                "--help", "--cache-free-space-buffer-size=1MiB", "-x");
        assertThat(result.remainingArgs()).containsExactly("-v", "--coordinator-url", "http://b", "--help", "-x");
        assertThat(result.canonicalArgs()).containsExactly("--cache-dir=/tmp/c1", "--cache-cleanup=false",
                "--cache-free-space-buffer-size=1MiB");
    }

    @TestFactory
    Stream<DynamicTest> shouldSkipValuesOfHostValueOptions() {
        return Stream.of(
                        List.of("--auth-token", "--cache-x"),
                        List.of("--coordinator-url", "--cache-dir"),
                        List.of("-u", "--no-cache-cleanup"))
                .map(args -> DynamicTest.dynamicTest(String.join(" ", args), () -> {
                    Result result = parse(args.toArray(String[]::new));
                    assertThat(result.remainingArgs()).containsExactlyElementsOf(args);
                    assertThat(result.cacheDir()).isNull();
                    assertThat(result.cacheCleanup()).isNull();
                }));
    }

    @Test
    void shouldValidateCacheOptionsEvenWhenHelpIsRequested() {
        assertThatThrownBy(() -> parse("--help", "--cache"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unknown cache option: --cache");
        assertThatThrownBy(() -> parse("-h", "--cache-dir"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Missing parameter value for --cache-dir option");
    }

    @TestFactory
    Stream<DynamicTest> shouldRejectInvalidCacheOptions() {
        return Stream.of(
                invalidCase("Unknown cache option: --cache", "--cache"),
                invalidCase("Unknown cache option: --cache-foo", "--cache-foo"),
                invalidCase("Unknown cache option: --cache-dirx", "--cache-dirx", "/tmp"),
                invalidCase("Unknown cache option: --no-cache", "--no-cache"),
                invalidCase("Unknown cache option: --no-cache-dir", "--no-cache-dir"),
                invalidCase("Unknown cache option: --cache-cleanupx", "--cache-cleanupx"),
                invalidCase("--[no-]cache-cleanup", "--cache-cleanup", "flase"),
                invalidCase("--[no-]cache-cleanup", "--cache-cleanup=flase"),
                invalidCase("--[no-]cache-cleanup", "--cache-cleanup="),
                invalidCase("--[no-]cache-cleanup", "--no-cache-cleanup=maybe"),
                invalidCase("--[no-]cache-cleanup", "--no-cache-cleanup="),
                invalidCase("--[no-]cache-cleanup", "--no-cache-cleanup", "yes"),
                invalidCase("Missing parameter value for --cache-dir option", "--cache-dir"),
                invalidCase("Missing parameter value for --cache-dir option", "--cache-dir="),
                invalidCase("Missing parameter value for --cache-dir option", "--cache-dir=  "),
                invalidCase("Missing parameter value for --cache-dir option", "--cache-dir", " "),
                invalidCase("Missing parameter value for --cache-dir option", "--cache-dir", "--coordinator-url", "http://x"),
                invalidCase("Missing parameter value for --cache-dir option", "--cache-dir", "-u", "http://x"),
                invalidCase("Missing parameter value for --cache-free-space-buffer-size option",
                        "--cache-free-space-buffer-size"),
                invalidCase("Missing parameter value for --cache-free-space-buffer-size option",
                        "--cache-free-space-buffer-size="),
                invalidCase("Missing parameter value for --cache-free-space-buffer-size option",
                        "--cache-free-space-buffer-size", " "),
                invalidCase("Missing parameter value for --cache-free-space-buffer-size option",
                        "--cache-free-space-buffer-size", "--coordinator-url", "http://x"),
                invalidCase("--cache-free-space-buffer-size", "--cache-free-space-buffer-size=2GB"),
                invalidCase("--cache-free-space-buffer-size", "--cache-free-space-buffer-size", "1.5GiB"));
    }

    private static DynamicTest invalidCase(String expectedMessagePart, String... args) {
        return DynamicTest.dynamicTest(String.join(" ", args) + " -> " + expectedMessagePart, () ->
                assertThatThrownBy(() -> parse(args))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining(expectedMessagePart));
    }

    @Test
    void shouldRejectNullArguments() {
        assertThatThrownBy(() -> WorkerCacheCliOptions.parse(null, Set.of()))
                .isInstanceOf(NullPointerException.class);
        assertThat(WorkerCacheCliOptions.parse(List.of("--cache-cleanup"), null).cacheCleanup()).isTrue();
    }
}
