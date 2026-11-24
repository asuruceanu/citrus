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

package org.citrusframework.distribution.junit;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContext;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;
import org.junit.jupiter.engine.JupiterTestEngine;
import org.junit.platform.engine.discovery.DiscoverySelectors;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.TagFilter;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;

import static org.assertj.core.api.Assertions.assertThat;

class DiscoveredWorkExtractorTest {

    private static final String HIDDEN_PACKAGE = "org.citrusframework.junit.jupiter.";

    @Test
    void extractsPlainTestClassWhenCitrusJupiterIsNotOnClasspath() throws Exception {
        try (HidingClassLoader loader = new HidingClassLoader(testClasspath(), HIDDEN_PACKAGE)) {
            Class<?> probeType = Class.forName(ExtractProbe.class.getName(), true, loader);
            @SuppressWarnings("unchecked")
            Callable<String> probe = (Callable<String>) probeType.getDeclaredConstructor().newInstance();

            assertThat(probe.call()).isEqualTo(PlainFixture.class.getName());
        }
    }

    @Test
    void reportsCitrusJupiterAsUnavailableWhenHidden() throws Exception {
        try (HidingClassLoader loader = new HidingClassLoader(testClasspath(), HIDDEN_PACKAGE)) {
            Class<?> support = Class.forName(CitrusJupiterSupport.class.getName(), true, loader);
            java.lang.reflect.Method isAvailable = support.getDeclaredMethod("isAvailable");
            isAvailable.setAccessible(true);
            assertThat(isAvailable.invoke(null)).isEqualTo(false);
        }
        assertThat(CitrusJupiterSupport.isAvailable()).isTrue();
    }

    @Test
    void reportsCitrusJupiterAsUnavailableWhenOnlyPartOfItIsVisible() throws Exception {
        try (HidingClassLoader loader = new HidingClassLoader(testClasspath(),
                "org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter")) {
            Class<?> support = Class.forName(CitrusJupiterSupport.class.getName(), true, loader);
            java.lang.reflect.Method isAvailable = support.getDeclaredMethod("isAvailable");
            isAvailable.setAccessible(true);
            assertThat(isAvailable.invoke(null)).isEqualTo(false);
        }
    }

    @Test
    void splitsClassesWithExcludedTemplateMethods() {
        Launcher launcher = LauncherFactory.create();
        TestPlan filtered = launcher.discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(MixedTemplateFixture.class))
                .filters(TagFilter.excludeTags("slow"))
                .build());

        var partial = DiscoveredWorkExtractor.partiallySelectedClasses(launcher, filtered);
        var work = DiscoveredWorkExtractor.extract(filtered, 1_000, partial);

        assertThat(partial).hasSize(1);
        assertThat(work.remoteUnits()).extracting(unit -> unit.key())
                .containsExactly(MixedTemplateFixture.class.getName() + "#fast");
    }

    @ExtendWith(SingleInvocationProvider.class)
    static class MixedTemplateFixture {
        @TestTemplate
        void fast() {
        }

        @TestTemplate
        @Tag("slow")
        void slow() {
        }
    }

    static class SingleInvocationProvider implements TestTemplateInvocationContextProvider {
        @Override
        public boolean supportsTestTemplate(ExtensionContext context) {
            return true;
        }

        @Override
        public Stream<TestTemplateInvocationContext> provideTestTemplateInvocationContexts(ExtensionContext context) {
            return Stream.of(new TestTemplateInvocationContext() {});
        }
    }

    @Test
    void keepsNestedClassesWhenTheOuterClassIsSplitIntoMethodUnits() {
        TestPlan plan = LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(OuterWithPinnedMethod.class))
                .build());

        DiscoveredWorkExtractor.ExtractedWork work = DiscoveredWorkExtractor.extract(plan, 1000L);

        assertThat(work.localUnits()).extracting(unit -> unit.key())
                .containsExactly(OuterWithPinnedMethod.class.getName() + "#pinned");
        assertThat(work.remoteUnits()).extracting(unit -> unit.className())
                .contains(OuterWithPinnedMethod.class.getName(), OuterWithPinnedMethod.Inner.class.getName());
    }

    public static final class OuterWithPinnedMethod {
        @Test
        @org.citrusframework.distribution.annotations.LocalOnly
        void pinned() {
        }

        @Test
        void anywhere() {
        }

        @org.junit.jupiter.api.Nested
        class Inner {
            @Test
            void nested() {
            }
        }
    }

    @Test
    void distributesOnlyTheNestedClassSurefireSelectedNotItsWholeOuterClass() {
        // Surefire's include patterns can select a single nested class (e.g. *Tests); the plan then holds the outer
        // class only as that nested class's parent, and a unit for the outer class would run every other nested class.
        TestPlan plan = LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectNestedClass(List.of(OuterWithSeveralNested.class),
                        OuterWithSeveralNested.SelectedTests.class))
                .build());

        DiscoveredWorkExtractor.ExtractedWork work = DiscoveredWorkExtractor.extract(plan, 1000L);

        assertThat(work.remoteUnits()).extracting(unit -> unit.className())
                .containsExactly(OuterWithSeveralNested.SelectedTests.class.getName());
    }

    @Test
    void stillCoversAnOuterClassWithItsOwnTestsAndAllItsNestedClasses() {
        TestPlan plan = LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(OuterWithSeveralNested.class))
                .build());

        DiscoveredWorkExtractor.ExtractedWork work = DiscoveredWorkExtractor.extract(plan, 1000L);

        String outer = OuterWithSeveralNested.class.getName();
        assertThat(work.remoteUnits()).extracting(unit -> unit.key()).containsExactlyInAnyOrder(
                outer + "#outer",
                outer + "/" + OuterWithSeveralNested.SelectedTests.class.getName(),
                outer + "/" + OuterWithSeveralNested.OtherTests.class.getName());
    }

    @Test
    void keepsTheEnclosingClassesLocalOnlyPinOnItsNestedClasses() {
        TestPlan plan = LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(PinnedOuter.class))
                .build());

        DiscoveredWorkExtractor.ExtractedWork work = DiscoveredWorkExtractor.extract(plan, 1000L);

        assertThat(work.remoteUnits()).isEmpty();
        assertThat(work.localUnits()).extracting(unit -> unit.className())
                .contains(PinnedOuter.Inner.class.getName());
    }

    @Test
    void distributesInheritedNestedClassesThroughEachSubclass() {
        TestPlan plan = LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(FirstSub.class), DiscoverySelectors.selectClass(SecondSub.class))
                .build());

        DiscoveredWorkExtractor.ExtractedWork work = DiscoveredWorkExtractor.extract(plan, 1000L);

        assertThat(work.remoteUnits()).filteredOn(unit -> unit.className().equals(AbstractBase.Inner.class.getName()))
                .extracting(unit -> unit.enclosingClassNames())
                .containsExactlyInAnyOrder(List.of(FirstSub.class.getName()), List.of(SecondSub.class.getName()));
        assertThat(work.remoteUnits()).extracting(unit -> unit.key()).doesNotHaveDuplicates();
    }

    @Test
    void distributesOnlyTheTestsTheHostSelectedWhenFiltersPruneAClass() {
        // Surefire turns groups/excludedGroups and -Dtest=Class#method into discovery filters on the host plan.
        TestPlan plan = LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(TaggedFixture.class))
                .filters(org.junit.platform.launcher.TagFilter.excludeTags("slow"))
                .build());

        DiscoveredWorkExtractor.ExtractedWork work = DiscoveredWorkExtractor.extract(plan, 1000L,
                DiscoveredWorkExtractor.partiallySelectedClasses(LauncherFactory.create(), plan));

        assertThat(work.remoteUnits()).extracting(unit -> unit.key())
                .containsExactly(TaggedFixture.class.getName() + "#fast");
    }

    @Test
    void keepsWholeClassUnitsWhenTheHostSelectedEveryTest() {
        TestPlan plan = LauncherFactory.create().discover(LauncherDiscoveryRequestBuilder.request()
                .selectors(DiscoverySelectors.selectClass(TaggedFixture.class))
                .build());

        DiscoveredWorkExtractor.ExtractedWork work = DiscoveredWorkExtractor.extract(plan, 1000L,
                DiscoveredWorkExtractor.partiallySelectedClasses(LauncherFactory.create(), plan));

        assertThat(work.remoteUnits()).extracting(unit -> unit.key()).containsExactly(TaggedFixture.class.getName());
    }

    public static final class TaggedFixture {
        @Test
        void fast() {
        }

        @Test
        @org.junit.jupiter.api.Tag("slow")
        void slow() {
        }
    }

    @org.citrusframework.distribution.annotations.LocalOnly
    public static final class PinnedOuter {
        @Test
        void outer() {
        }

        @org.junit.jupiter.api.Nested
        class Inner {
            @Test
            void inner() {
            }
        }
    }

    public abstract static class AbstractBase {
        @org.junit.jupiter.api.Nested
        class Inner {
            @Test
            void inherited() {
            }
        }
    }

    public static final class FirstSub extends AbstractBase {
        @Test
        void first() {
        }
    }

    public static final class SecondSub extends AbstractBase {
        @Test
        void second() {
        }
    }

    public static final class OuterWithSeveralNested {
        @Test
        void outer() {
        }

        @org.junit.jupiter.api.Nested
        class SelectedTests {
            @Test
            void selected() {
            }
        }

        @org.junit.jupiter.api.Nested
        class OtherTests {
            @Test
            void other() {
            }
        }
    }

    /** Runs inside {@link HidingClassLoader}: discovers {@link PlainFixture} and extracts its work units. */
    public static final class ExtractProbe implements Callable<String> {
        @Override
        public String call() {
            Launcher launcher = LauncherFactory.create(LauncherConfig.builder()
                    .enableTestEngineAutoRegistration(false)
                    .enableLauncherSessionListenerAutoRegistration(false)
                    .enableLauncherDiscoveryListenerAutoRegistration(false)
                    .enablePostDiscoveryFilterAutoRegistration(false)
                    .enableTestExecutionListenerAutoRegistration(false)
                    .addTestEngines(new JupiterTestEngine())
                    .build());
            TestPlan plan = launcher.discover(LauncherDiscoveryRequestBuilder.request()
                    .selectors(DiscoverySelectors.selectClass(PlainFixture.class))
                    .build());
            return DiscoveredWorkExtractor.extract(plan, 1000L).remoteUnits().stream()
                    .map(unit -> unit.key())
                    .collect(Collectors.joining(","));
        }
    }

    public static final class PlainFixture {
        @Test
        void plain() {
        }
    }

    private static URL[] testClasspath() throws MalformedURLException {
        List<URL> urls = new ArrayList<>();
        for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
            if (!entry.isBlank()) {
                urls.add(Path.of(entry).toUri().toURL());
            }
        }
        return urls.toArray(URL[]::new);
    }

    /** Child-first loader over the test classpath that pretends {@code hiddenPrefix} classes do not exist. */
    private static final class HidingClassLoader extends URLClassLoader {
        private final String hiddenPrefix;

        HidingClassLoader(URL[] urls, String hiddenPrefix) {
            super(urls, ClassLoader.getPlatformClassLoader());
            this.hiddenPrefix = hiddenPrefix;
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith(hiddenPrefix)) {
                throw new ClassNotFoundException(name);
            }
            return super.loadClass(name, resolve);
        }
    }
}
