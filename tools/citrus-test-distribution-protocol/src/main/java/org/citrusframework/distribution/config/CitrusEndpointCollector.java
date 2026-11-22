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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Zero-dependency reflective listener that captures Citrus endpoint names touched during each test
 * execution on a target {@link ClassLoader}. Safely degrades to a no-op when Citrus runtime classes
 * are not on the test classpath.
 */
public final class CitrusEndpointCollector implements AutoCloseable {

    private final Set<String> currentEndpoints = Collections.synchronizedSet(new LinkedHashSet<>());
    private volatile boolean closed;
    private Runnable cleanup;

    private CitrusEndpointCollector(Runnable cleanup) {
        this.cleanup = cleanup;
    }

    public static CitrusEndpointCollector install(ClassLoader classLoader) {
        if (classLoader == null) {
            return new CitrusEndpointCollector(() -> { });
        }
        try {
            Class<?> managerClass = Class.forName("org.citrusframework.CitrusInstanceManager", false, classLoader);
            Class<?> processorClass = Class.forName("org.citrusframework.CitrusInstanceProcessor", false, classLoader);
            Class<?> listenerClass = Class.forName("org.citrusframework.report.TestActionListener", false, classLoader);

            CitrusEndpointCollector holder = new CitrusEndpointCollector(null);
            Set<Object> attachedInstances = Collections.synchronizedSet(
                    Collections.newSetFromMap(new IdentityHashMap<>()));

            Object listenerProxy = Proxy.newProxyInstance(
                    classLoader,
                    new Class<?>[]{listenerClass},
                    (proxy, method, args) -> {
                        String m = method.getName();
                        if ("equals".equals(m)) {
                            return proxy == (args != null && args.length > 0 ? args[0] : null);
                        }
                        if ("hashCode".equals(m)) {
                            return System.identityHashCode(proxy);
                        }
                        if ("toString".equals(m)) {
                            return "CitrusEndpointCollectorListener";
                        }
                        if (("onTestActionStart".equals(m)
                                || "onTestActionFinish".equals(m)
                                || "onTestActionFailed".equals(m))
                                && args != null && args.length >= 2 && args[1] != null) {
                            holder.recordAction(args[1], Collections.newSetFromMap(new IdentityHashMap<>()));
                        }
                        return null;
                    });

            Object processorProxy = Proxy.newProxyInstance(
                    classLoader,
                    new Class<?>[]{processorClass},
                    (proxy, method, args) -> {
                        String m = method.getName();
                        if ("equals".equals(m)) {
                            return proxy == (args != null && args.length > 0 ? args[0] : null);
                        }
                        if ("hashCode".equals(m)) {
                            return System.identityHashCode(proxy);
                        }
                        if ("toString".equals(m)) {
                            return "CitrusEndpointCollectorProcessor";
                        }
                        if ("process".equals(m) && !holder.closed && args != null && args.length == 1 && args[0] != null) {
                            attachListener(args[0], listenerClass, listenerProxy, attachedInstances);
                        }
                        return null;
                    });

            Method addProcessor = managerClass.getMethod("addInstanceProcessor", processorClass);
            addProcessor.invoke(null, processorProxy);

            try {
                Method getExisting = managerClass.getMethod("get");
                Object existing = getExisting.invoke(null);
                if (existing instanceof Optional<?> opt && opt.isPresent()) {
                    attachListener(opt.get(), listenerClass, listenerProxy, attachedInstances);
                }
            } catch (Exception ignored) {
                // Best-effort attachment to an already-initialized Citrus instance
            }

            Runnable detach = () -> {
                try {
                    Method removeProcessor = managerClass.getMethod("removeInstanceProcessor", processorClass);
                    removeProcessor.invoke(null, processorProxy);
                } catch (Exception ignored) {
                    // Best-effort cleanup
                }
                synchronized (attachedInstances) {
                    for (Object citrus : attachedInstances) {
                        detachListener(citrus, listenerProxy);
                    }
                    attachedInstances.clear();
                }
            };
            holder.cleanup = detach;
            return holder;
        } catch (LinkageError | ReflectiveOperationException | RuntimeException ignored) {
            return new CitrusEndpointCollector(() -> { });
        }
    }

    public void startTest() {
        currentEndpoints.clear();
    }

    public List<String> drainEndpoints() {
        synchronized (currentEndpoints) {
            if (currentEndpoints.isEmpty()) {
                return List.of();
            }
            List<String> snapshot = List.copyOf(currentEndpoints);
            currentEndpoints.clear();
            return snapshot;
        }
    }

    @Override
    public void close() {
        closed = true;
        if (cleanup != null) {
            cleanup.run();
        }
        currentEndpoints.clear();
    }

    private void recordAction(Object action, Set<Object> visited) {
        if (closed || action == null || !visited.add(action)) {
            return;
        }
        boolean foundDirectEndpoint = false;
        Object endpoint = invokeZeroArg(action, "getEndpoint");
        if (endpoint != null) {
            String name = asNonBlankString(invokeZeroArg(endpoint, "getName"));
            if (name != null) {
                currentEndpoints.add(name);
                foundDirectEndpoint = true;
            }
        }
        if (!foundDirectEndpoint) {
            String uri = asNonBlankString(invokeZeroArg(action, "getEndpointUri"));
            if (uri != null) {
                currentEndpoints.add(uri);
            }
        }
        Object endpoints = invokeZeroArg(action, "getEndpoints");
        if (endpoints instanceof Collection<?> list) {
            for (Object ep : list) {
                String name = asNonBlankString(invokeZeroArg(ep, "getName"));
                if (name != null) {
                    currentEndpoints.add(name);
                }
            }
        }
        Object endpointNames = invokeZeroArg(action, "getEndpointNames");
        if (endpointNames instanceof Collection<?> names) {
            for (Object n : names) {
                String name = asNonBlankString(n);
                if (name != null) {
                    currentEndpoints.add(name);
                }
            }
        }
        Object executedChildren = invokeZeroArg(action, "getExecutedActions");
        if (executedChildren instanceof Collection<?> list) {
            for (Object child : list) {
                recordAction(child, visited);
            }
        }
        Object activeChild = invokeZeroArg(action, "getActiveAction");
        if (activeChild != null) {
            recordAction(activeChild, visited);
        }
    }

    private static void attachListener(
            Object citrus,
            Class<?> listenerClass,
            Object listenerProxy,
            Set<Object> attachedInstances) {
        if (!attachedInstances.add(citrus)) {
            return;
        }
        try {
            Method addListener = citrus.getClass().getMethod("addTestActionListener", listenerClass);
            addListener.invoke(citrus, listenerProxy);
        } catch (Exception ignored) {
            // Best-effort attachment
        }
    }

    private static void detachListener(Object citrus, Object listenerProxy) {
        try {
            Object context = invokeZeroArg(citrus, "getCitrusContext");
            if (context != null) {
                Object listeners = invokeZeroArg(context, "getTestActionListeners");
                if (listeners instanceof Collection<?> collection) {
                    collection.remove(listenerProxy);
                } else if (listeners != null) {
                    try {
                        Field backingField = listeners.getClass().getDeclaredField("testActionListeners");
                        backingField.setAccessible(true);
                        Object rawListeners = backingField.get(listeners);
                        if (rawListeners instanceof Collection<?> collection) {
                            collection.remove(listenerProxy);
                        }
                    } catch (Exception ignored) {
                        // Closed or unmodifiable listener holder; closed flag suppresses callbacks
                    }
                }
            }
        } catch (Exception ignored) {
            // Best-effort listener removal
        }
    }

    private static Object invokeZeroArg(Object target, String methodName) {
        if (target == null) {
            return null;
        }
        try {
            Method method = target.getClass().getMethod(methodName);
            return method.invoke(target);
        } catch (Exception ignored) {
            return null;
        }
    }

    private static String asNonBlankString(Object value) {
        if (value instanceof String str) {
            String trimmed = str.trim();
            return trimmed.isEmpty() ? null : trimmed;
        }
        return null;
    }
}
