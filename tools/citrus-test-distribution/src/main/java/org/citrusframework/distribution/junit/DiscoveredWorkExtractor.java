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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.citrusframework.distribution.model.WorkUnitSpec;
import org.citrusframework.distribution.scheduler.CapabilityMatcher;
import org.junit.platform.engine.TestSource;
import org.junit.platform.engine.support.descriptor.ClassSource;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

/**
 * Extracts distributable remote {@link WorkUnitSpec}s and pinned {@code @LocalOnly} units from a JUnit {@link TestPlan},
 * including probing {@code @CitrusTestFactory} methods to split declarative XML/YAML/Groovy sources individually.
 */
public final class DiscoveredWorkExtractor {

    private DiscoveredWorkExtractor() {
    }

    public record ExtractedWork(
            List<WorkUnitSpec> localUnits,
            List<WorkUnitSpec> remoteUnits,
            boolean hasRemoteOnlyUnits) {
    }

    public static ExtractedWork extract(TestPlan testPlan, long defaultDurationMs) {
        return extract(testPlan, defaultDurationMs, Set.of());
    }

    /**
     * @param partiallySelectedClasses unique ids of class containers whose tests the host's discovery filters pruned
     *                                 (see {@link #partiallySelectedClasses}); they are split into method units, since a
     *                                 class unit would run the tests the host excluded
     */
    public static ExtractedWork extract(TestPlan testPlan, long defaultDurationMs, Set<String> partiallySelectedClasses) {
        PARTIAL.set(partiallySelectedClasses != null ? partiallySelectedClasses : Set.of());
        try {
            return extractWork(testPlan, defaultDurationMs);
        } finally {
            PARTIAL.remove();
        }
    }

    private static final ThreadLocal<Set<String>> PARTIAL = ThreadLocal.withInitial(Set::of);

    /**
     * Class containers of {@code testPlan} that hold fewer tests than the same classes discovered without filters:
     * Surefire's {@code -Dtest=Class#method}, {@code groups} and {@code excludedGroups} prune the host plan with
     * discovery filters that a worker would not apply to a whole-class selection. Returns no classes when the
     * unfiltered discovery fails, which keeps whole-class units.
     */
    public static Set<String> partiallySelectedClasses(org.junit.platform.launcher.Launcher launcher, TestPlan testPlan) {
        List<org.junit.platform.engine.DiscoverySelector> selectors = new ArrayList<>();
        Map<String, Integer> planned = new LinkedHashMap<>();
        for (TestIdentifier root : testPlan.getRoots()) {
            for (TestIdentifier top : testPlan.getChildren(root)) {
                if (top.getSource().orElse(null) instanceof ClassSource) {
                    selectors.add(org.junit.platform.engine.discovery.DiscoverySelectors.selectUniqueId(top.getUniqueId()));
                }
            }
            countTests(testPlan, root, planned);
        }
        if (selectors.isEmpty()) {
            return Set.of();
        }
        try {
            TestPlan full = launcher.discover(org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder.request()
                    .selectors(selectors)
                    .build());
            Map<String, Integer> available = new LinkedHashMap<>();
            for (TestIdentifier root : full.getRoots()) {
                countTests(full, root, available);
            }
            Set<String> partial = new java.util.HashSet<>();
            planned.forEach((uid, count) -> {
                Integer all = available.get(uid);
                if (all != null && count < all) {
                    partial.add(uid);
                }
            });
            return partial;
        } catch (RuntimeException e) {
            return Set.of();
        }
    }

    /** Number of tests below every class container under {@code node}, keyed by the container's unique id. */
    private static int countTests(TestPlan plan, TestIdentifier node, Map<String, Integer> counts) {
        int tests = node.isTest() ? 1 : 0;
        for (TestIdentifier child : plan.getChildren(node)) {
            tests += countTests(plan, child, counts);
        }
        if (node.isContainer() && node.getSource().orElse(null) instanceof ClassSource) {
            counts.put(node.getUniqueId(), tests);
        }
        return tests;
    }

    private static ExtractedWork extractWork(TestPlan testPlan, long defaultDurationMs) {
        Map<String, WorkUnitSpec> localByKey = new LinkedHashMap<>();
        Map<String, WorkUnitSpec> remoteByKey = new LinkedHashMap<>();
        boolean[] hasRemoteOnly = {false};

        for (TestIdentifier root : testPlan.getRoots()) {
            for (TestIdentifier topChild : testPlan.getChildren(root)) {
                collectClassOrMethodNode(testPlan, topChild, defaultDurationMs, localByKey, remoteByKey, hasRemoteOnly);
            }
        }
        return new ExtractedWork(
                List.copyOf(localByKey.values()),
                List.copyOf(remoteByKey.values()),
                hasRemoteOnly[0]);
    }

    private static void collectClassOrMethodNode(TestPlan plan,
                                                 TestIdentifier node,
                                                 long defaultDurationMs,
                                                 Map<String, WorkUnitSpec> localByKey,
                                                 Map<String, WorkUnitSpec> remoteByKey,
                                                 boolean[] hasRemoteOnly) {
        Optional<TestSource> srcOpt = node.getSource();
        if (srcOpt.isPresent() && srcOpt.get() instanceof ClassSource classSource) {
            Class<?> testClass = classSource.getJavaClass();
            inspectTestClass(plan, node, testClass, defaultDurationMs, localByKey, remoteByKey, hasRemoteOnly);
            return;
        }
        for (TestIdentifier child : plan.getChildren(node)) {
            collectClassOrMethodNode(plan, child, defaultDurationMs, localByKey, remoteByKey, hasRemoteOnly);
        }
    }

    private static void inspectTestClass(TestPlan plan,
                                         TestIdentifier classNode,
                                         Class<?> testClass,
                                         long defaultDurationMs,
                                         Map<String, WorkUnitSpec> localByKey,
                                         Map<String, WorkUnitSpec> remoteByKey,
                                         boolean[] hasRemoteOnly) {
        Set<TestIdentifier> children = plan.getChildren(classNode);
        // Classes enclosing this one in the plan (outermost first): for an inherited @Nested class this is the
        // subclass, which the worker must select it through. Their class-level pins and capabilities apply here too.
        List<Class<?>> enclosing = enclosingClasses(plan, classNode);
        List<String> enclosingNames = enclosing.stream().map(Class::getName).toList();
        String keyPrefix = enclosingNames.isEmpty() ? "" : String.join("/", enclosingNames) + "/";
        boolean enclosingLocal = enclosing.stream().anyMatch(c -> CapabilityMatcher.isLocalOnly(c, null));
        boolean enclosingRemote = enclosing.stream().anyMatch(c -> CapabilityMatcher.isRemoteOnly(c, null));
        Map<String, String> enclosingCaps = new LinkedHashMap<>();
        enclosing.forEach(c -> enclosingCaps.putAll(CapabilityMatcher.extractRequiredCapabilities(c, null)));
        boolean hasSpecialMethodAnnotations = false;
        // A class unit selects the whole class, nested classes included. When nested classes are in the plan, the
        // plan may hold only some of them (Surefire includes such as *Tests select single nested classes), so the
        // class is split into its own method units and one unit per nested class instead.
        boolean hasNestedClasses = children.stream().anyMatch(
                child -> child.getSource().orElse(null) instanceof ClassSource);
        List<Method> factoryMethods = new ArrayList<>();

        for (TestIdentifier child : children) {
            if (child.getSource().isPresent() && child.getSource().get() instanceof MethodSource ms) {
                Method javaMethod = resolveMethod(testClass, ms);
                if (javaMethod != null) {
                    if (CapabilityMatcher.isLocalOnly(testClass, javaMethod)
                            || CapabilityMatcher.isRemoteOnly(testClass, javaMethod)
                            || !CapabilityMatcher.extractRequiredCapabilities(null, javaMethod).isEmpty()) {
                        hasSpecialMethodAnnotations = true;
                    }
                    if (CitrusJupiterSupport.isFactoryMethod(testClass, javaMethod)) {
                        factoryMethods.add(javaMethod);
                    }
                }
            }
        }

        if (!factoryMethods.isEmpty() && children.size() == factoryMethods.size()) {
            List<WorkUnitSpec> factoryUnits = probeCitrusTestFactory(testClass, factoryMethods, defaultDurationMs);
            if (!factoryUnits.isEmpty()) {
                for (WorkUnitSpec spec : factoryUnits) {
                    boolean local = CapabilityMatcher.isLocalOnly(testClass, null);
                    if (CapabilityMatcher.isRemoteOnly(testClass, null)) {
                        hasRemoteOnly[0] = true;
                    }
                    (local ? localByKey : remoteByKey).putIfAbsent(spec.key(), spec);
                }
                return;
            }
        }

        boolean pruned = PARTIAL.get().contains(classNode.getUniqueId());
        if (!hasSpecialMethodAnnotations && !hasNestedClasses && !pruned && !children.isEmpty()) {
            boolean local = enclosingLocal || CapabilityMatcher.isLocalOnly(testClass, null);
            if (enclosingRemote || CapabilityMatcher.isRemoteOnly(testClass, null)) {
                hasRemoteOnly[0] = true;
            }
            Map<String, String> caps = new LinkedHashMap<>(enclosingCaps);
            caps.putAll(CapabilityMatcher.extractRequiredCapabilities(testClass, null));
            WorkUnitSpec unit = new WorkUnitSpec(keyPrefix + testClass.getName(), testClass.getName(), null, null,
                    caps, defaultDurationMs, enclosingNames);
            (local ? localByKey : remoteByKey).putIfAbsent(unit.key(), unit);
            return;
        }

        for (TestIdentifier child : children) {
            if (child.getSource().isPresent() && child.getSource().get() instanceof MethodSource ms) {
                Method javaMethod = resolveMethod(testClass, ms);
                boolean local = enclosingLocal || CapabilityMatcher.isLocalOnly(testClass, javaMethod);
                if (enclosingRemote || CapabilityMatcher.isRemoteOnly(testClass, javaMethod)) {
                    hasRemoteOnly[0] = true;
                }
                Map<String, String> caps = new LinkedHashMap<>(enclosingCaps);
                caps.putAll(CapabilityMatcher.extractRequiredCapabilities(testClass, javaMethod));
                String methodSelector = buildMethodSelectorName(ms);
                String key = keyPrefix + testClass.getName() + "#" + methodSelector;
                WorkUnitSpec unit = new WorkUnitSpec(
                        key, testClass.getName(), methodSelector, null, caps, defaultDurationMs, enclosingNames);
                (local ? localByKey : remoteByKey).putIfAbsent(key, unit);
            } else if (child.getSource().isPresent() && child.getSource().get() instanceof ClassSource) {
                // @Nested classes are not covered by the outer class's method units: give them their own units.
                collectClassOrMethodNode(plan, child, defaultDurationMs, localByKey, remoteByKey, hasRemoteOnly);
            }
        }
    }

    /** Classes of the plan's class containers above {@code classNode}, outermost first. */
    private static List<Class<?>> enclosingClasses(TestPlan plan, TestIdentifier classNode) {
        List<Class<?>> enclosing = new ArrayList<>();
        Optional<TestIdentifier> parent = plan.getParent(classNode);
        while (parent.isPresent()) {
            if (parent.get().getSource().orElse(null) instanceof ClassSource cs) {
                try {
                    enclosing.add(0, cs.getJavaClass());
                } catch (RuntimeException notLoadable) {
                    break;
                }
            }
            parent = plan.getParent(parent.get());
        }
        return enclosing;
    }

    private static List<WorkUnitSpec> probeCitrusTestFactory(Class<?> testClass,
                                                             List<Method> factoryMethods,
                                                             long defaultDurationMs) {
        if (Modifier.isAbstract(testClass.getModifiers())) {
            return List.of();
        }
        List<WorkUnitSpec> specs = new ArrayList<>();
        try {
            Object instance = testClass.getDeclaredConstructor().newInstance();
            for (Method method : factoryMethods) {
                if (method.getParameterCount() != 0) {
                    continue;
                }
                method.setAccessible(true);
                List<String> sources = CitrusJupiterSupport.probeSources(instance, method);
                Map<String, String> caps = CapabilityMatcher.extractRequiredCapabilities(testClass, method);
                for (String srcName : sources) {
                    String key = testClass.getName() + "#" + method.getName() + "[" + srcName + "]";
                    specs.add(new WorkUnitSpec(
                            key, testClass.getName(), method.getName(), srcName, caps, defaultDurationMs));
                }
            }
        } catch (Exception ignored) {
            return List.of();
        }
        return specs;
    }

    private static String buildMethodSelectorName(MethodSource ms) {
        String types = ms.getMethodParameterTypes();
        if (types != null && !types.isBlank()) {
            return ms.getMethodName() + "(" + types + ")";
        }
        return ms.getMethodName();
    }

    private static Method resolveMethod(Class<?> testClass, MethodSource ms) {
        try {
            return ms.getJavaMethod();
        } catch (Exception ignored) {
            for (Method m : testClass.getDeclaredMethods()) {
                if (m.getName().equals(ms.getMethodName())) {
                    return m;
                }
            }
            return null;
        }
    }
}
