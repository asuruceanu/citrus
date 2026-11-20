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
import org.citrusframework.junit.jupiter.CitrusDynamicSourceFilter;
import org.citrusframework.junit.jupiter.CitrusTestFactory;
import org.citrusframework.junit.jupiter.CitrusTestFactorySupport;
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
        boolean hasSpecialMethodAnnotations = false;
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
                    if (javaMethod.isAnnotationPresent(CitrusTestFactory.class)
                            && CitrusTestFactorySupport.class.isAssignableFrom(testClass)) {
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

        if (!hasSpecialMethodAnnotations && !children.isEmpty()) {
            boolean local = CapabilityMatcher.isLocalOnly(testClass, null);
            if (CapabilityMatcher.isRemoteOnly(testClass, null)) {
                hasRemoteOnly[0] = true;
            }
            Map<String, String> caps = CapabilityMatcher.extractRequiredCapabilities(testClass, null);
            WorkUnitSpec unit = new WorkUnitSpec(
                    testClass.getName(), testClass.getName(), null, null, caps, defaultDurationMs);
            (local ? localByKey : remoteByKey).putIfAbsent(unit.key(), unit);
            return;
        }

        for (TestIdentifier child : children) {
            if (child.getSource().isPresent() && child.getSource().get() instanceof MethodSource ms) {
                Method javaMethod = resolveMethod(testClass, ms);
                boolean local = CapabilityMatcher.isLocalOnly(testClass, javaMethod);
                if (CapabilityMatcher.isRemoteOnly(testClass, javaMethod)) {
                    hasRemoteOnly[0] = true;
                }
                Map<String, String> caps = CapabilityMatcher.extractRequiredCapabilities(testClass, javaMethod);
                String methodSelector = buildMethodSelectorName(ms);
                String key = testClass.getName() + "#" + methodSelector;
                WorkUnitSpec unit = new WorkUnitSpec(
                        key, testClass.getName(), methodSelector, null, caps, defaultDurationMs);
                (local ? localByKey : remoteByKey).putIfAbsent(key, unit);
            }
        }
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
                CitrusDynamicSourceFilter.startProbe();
                List<String> sources;
                try {
                    method.invoke(instance);
                } finally {
                    sources = CitrusDynamicSourceFilter.finishProbe();
                }
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
