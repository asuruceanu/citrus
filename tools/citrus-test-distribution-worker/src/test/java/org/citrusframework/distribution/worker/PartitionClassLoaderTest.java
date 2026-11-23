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

package org.citrusframework.distribution.worker;

import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.citrusframework.distribution.model.TestOutcomeStatus;
import org.citrusframework.distribution.model.WorkerTestEventPayload;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.platform.engine.TestEngine;
import org.w3c.dom.Document;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionClassLoaderTest {

    private static final ClassLoader WORKER = PartitionClassLoaderTest.class.getClassLoader();

    @TempDir
    Path tempDir;

    @Test
    void shouldUseJdkCopyOfJdkModulePackagesEvenWhenThePartitionShipsThem() throws Exception {
        // Like xml-apis on a test classpath: a partition jar carrying org.w3c.dom next to Xerces.
        Path classes = Files.createDirectories(tempDir.resolve("classes/org/w3c/dom")).getParent().getParent().getParent();
        try (InputStream jdkDocument = ClassLoader.getSystemResourceAsStream("org/w3c/dom/Document.class")) {
            Files.write(classes.resolve("org/w3c/dom/Document.class"), jdkDocument.readAllBytes());
        }

        try (IsolatedPartitionRunner.PartitionClassLoader loader = loaderOver(classes)) {
            assertThat(loader.loadClass("org.w3c.dom.Document")).isSameAs(Document.class);
        }
    }

    @Test
    void shouldNotFallBackToTheWorkersOwnClassesOrResources() throws Exception {
        String workerOnlyClass = WorkerTestEventPayload.class.getName();
        String workerOnlyResource = workerOnlyClass.replace('.', '/') + ".class";

        try (IsolatedPartitionRunner.PartitionClassLoader loader = loaderOver(Files.createDirectories(tempDir.resolve("empty")))) {
            assertThatThrownBy(() -> loader.loadClass(workerOnlyClass)).isInstanceOf(ClassNotFoundException.class);
            assertThat(loader.getResource(workerOnlyResource)).isNull();
            assertThat(Collections.list(loader.getResources(workerOnlyResource))).isEmpty();
        }
    }

    @Test
    void shouldResolveClassesInjectedIntoTheJvmLoadersWhenThePartitionLacksThem() throws Exception {
        // Mockito's inline mock maker injects MockMethodDispatcher into the bootstrap loader at runtime.
        String injected = WorkerTestEventPayload.class.getName();
        Path bootDir = tempDir.resolve("boot");
        Path classFile = bootDir.resolve(injected.replace('.', '/') + ".class");
        Files.createDirectories(classFile.getParent());
        try (InputStream bytes = WORKER.getResourceAsStream(injected.replace('.', '/') + ".class")) {
            Files.write(classFile, bytes.readAllBytes());
        }

        try (java.net.URLClassLoader jvmLoaders = new java.net.URLClassLoader(new URL[] {bootDir.toUri().toURL()}, null);
             IsolatedPartitionRunner.PartitionClassLoader loader = new IsolatedPartitionRunner.PartitionClassLoader(
                     new URL[] {Files.createDirectories(tempDir.resolve("partition")).toUri().toURL()}, WORKER, jvmLoaders)) {
            assertThat(loader.loadClass(injected).getClassLoader()).isSameAs(jvmLoaders);
        }
    }

    @Test
    void shouldResolveClassesFromTheWorkersJavaAgentsButNothingElseOfItsClasspath() throws Exception {
        // A -javaagent (JaCoCo, an APM agent) is appended to the worker's application loader; classes it instruments
        // the tests with must resolve to the agent's own copies.
        String agentClass = WorkerTestEventPayload.class.getName();
        Path agentDir = tempDir.resolve("agent");
        Path classFile = agentDir.resolve(agentClass.replace('.', '/') + ".class");
        Files.createDirectories(classFile.getParent());
        try (InputStream bytes = WORKER.getResourceAsStream(agentClass.replace('.', '/') + ".class")) {
            Files.write(classFile, bytes.readAllBytes());
        }
        String workerOnlyClass = TestOutcomeStatus.class.getName();
        Path workerDir = tempDir.resolve("worker-classes");
        Path workerClassFile = workerDir.resolve(workerOnlyClass.replace('.', '/') + ".class");
        Files.createDirectories(workerClassFile.getParent());
        try (InputStream bytes = WORKER.getResourceAsStream(workerOnlyClass.replace('.', '/') + ".class")) {
            Files.write(workerClassFile, bytes.readAllBytes());
        }

        try (java.net.URLClassLoader workerApp = new java.net.URLClassLoader(
                new URL[] {agentDir.toUri().toURL(), workerDir.toUri().toURL()}, ClassLoader.getPlatformClassLoader());
             IsolatedPartitionRunner.PartitionClassLoader loader = new IsolatedPartitionRunner.PartitionClassLoader(
                     new URL[] {Files.createDirectories(tempDir.resolve("agent-partition")).toUri().toURL()}, WORKER,
                     ClassLoader.getPlatformClassLoader(), workerApp, List.of(agentDir))) {
            assertThat(loader.loadClass(agentClass).getClassLoader()).isSameAs(workerApp);
            assertThatThrownBy(() -> loader.loadClass(workerOnlyClass)).isInstanceOf(ClassNotFoundException.class);
        }
    }

    @Test
    void shouldReadTheJavaAgentJarsFromTheJvmArguments() {
        assertThat(IsolatedPartitionRunner.PartitionClassLoader.javaAgentPaths(List.of(
                "-Xmx1g", "-javaagent:/opt/jacoco/agent.jar=destfile=target/jacoco.exec", "-javaagent:apm.jar")))
                .containsExactly(Path.of("/opt/jacoco/agent.jar"), Path.of("apm.jar").toAbsolutePath());
    }

    @Test
    void shouldStillShareJUnitPlatformAndJdkTypesWithTheWorker() throws Exception {
        try (IsolatedPartitionRunner.PartitionClassLoader loader = loaderOver(Files.createDirectories(tempDir.resolve("shared")))) {
            assertThat(loader.loadClass(TestEngine.class.getName())).isSameAs(TestEngine.class);
            assertThat(loader.loadClass(String.class.getName())).isSameAs(String.class);
            assertThat(loader.getResource("java/lang/String.class")).isNotNull();
        }
    }

    @Test
    void shouldLetWorkerSharedLibrariesSeeTheirOwnResources() throws Exception {
        String junitResource = TestEngine.class.getName().replace('.', '/') + ".class";
        try (IsolatedPartitionRunner.PartitionClassLoader loader = loaderOver(Files.createDirectories(tempDir.resolve("res")))) {
            assertThat(loader.getResource(junitResource)).isEqualTo(WORKER.getResource(junitResource));
            assertThat(Collections.list(loader.getResources(junitResource))).isNotEmpty();
        }
    }

    @Test
    void shouldLoadJUnitServiceProvidersTheWorkerRegistersOutsideOrgJunit() throws Exception {
        // A worker jar registering a JUnit SPI implementation that lives outside org.junit.
        String provider = WorkerTestEventPayload.class.getName();
        Path workerJar = tempDir.resolve("worker-jar");
        Path services = workerJar.resolve("META-INF/services/org.junit.platform.launcher.TestExecutionListener");
        Files.createDirectories(services.getParent());
        Files.writeString(services, "# comment\n" + provider + "\n");
        Path classFile = workerJar.resolve(provider.replace('.', '/') + ".class");
        Files.createDirectories(classFile.getParent());
        try (InputStream bytes = WORKER.getResourceAsStream(provider.replace('.', '/') + ".class")) {
            Files.write(classFile, bytes.readAllBytes());
        }

        try (java.net.URLClassLoader worker = new java.net.URLClassLoader(new URL[] {workerJar.toUri().toURL()},
                ClassLoader.getPlatformClassLoader());
             IsolatedPartitionRunner.PartitionClassLoader loader = new IsolatedPartitionRunner.PartitionClassLoader(
                     new URL[] {Files.createDirectories(tempDir.resolve("spi-partition")).toUri().toURL()}, worker)) {
            assertThat(loader.loadClass(provider).getClassLoader()).isSameAs(worker);
        }
    }

    @Test
    void shouldLoadTheCitrusSourceFilterFromThePartitionLikeTheRestOfItsPackage() throws Exception {
        // CitrusTestFactorySupport (partition) calls a package-private method of CitrusDynamicSourceFilter: both must
        // come from the same loader, or the JVM sees two runtime packages and throws IllegalAccessError.
        String filter = "org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter";
        Path partition = Files.createDirectories(tempDir.resolve("jupiter-partition"));
        Path classFile = partition.resolve(filter.replace('.', '/') + ".class");
        Files.createDirectories(classFile.getParent());
        try (InputStream bytes = WORKER.getResourceAsStream(filter.replace('.', '/') + ".class")) {
            assertThat(bytes).as("citrus-junit-jupiter on the worker test classpath").isNotNull();
            Files.write(classFile, bytes.readAllBytes());
        }

        try (IsolatedPartitionRunner.PartitionClassLoader loader = loaderOver(partition)) {
            assertThat(loader.loadClass(filter).getClassLoader()).isSameAs(loader);
        }
    }

    @Test
    void shouldHaveANameLikeTheClassLoadersFrameworksExpect() throws Exception {
        // Quarkus' test extension calls getName() on the test class's loader; Surefire's is the JDK "app" loader.
        try (IsolatedPartitionRunner.PartitionClassLoader loader = loaderOver(Files.createDirectories(tempDir.resolve("named")))) {
            assertThat(loader.getName()).isNotBlank();
        }
    }

    @Test
    void shouldLoadThirdPartyJavaxLibrariesFromThePartition() {
        // wsdl4j's javax.wsdl API must come from the same loader as its com.ibm.wsdl implementation in the partition.
        assertThat(IsolatedPartitionRunner.PartitionClassLoader.shouldDelegateToParent("javax.wsdl.factory.WSDLFactory"))
                .isFalse();
        assertThat(IsolatedPartitionRunner.PartitionClassLoader.shouldDelegateToParent("javax.xml.parsers.DocumentBuilder"))
                .as("JDK javax packages stay shared").isTrue();
        assertThat(IsolatedPartitionRunner.PartitionClassLoader.shouldDelegateToParent("jdk.internal.misc.Unsafe")).isTrue();
    }

    private static IsolatedPartitionRunner.PartitionClassLoader loaderOver(Path dir) throws Exception {
        return new IsolatedPartitionRunner.PartitionClassLoader(new URL[] {dir.toUri().toURL()}, WORKER);
    }
}
