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

package org.citrusframework.testng;

import org.citrusframework.TestCaseRunner;
import org.citrusframework.TestCaseRunnerFactory;
import org.citrusframework.annotations.CitrusTest;
import org.citrusframework.annotations.CitrusTestSource;
import org.citrusframework.api.common.TestLoader;
import org.citrusframework.api.common.TestSourceAware;
import org.citrusframework.base.DefaultTestCase;
import org.citrusframework.common.ResolvedTestSource;
import org.citrusframework.common.TestSourceHelper;
import org.citrusframework.common.TestSourceResolver;
import org.citrusframework.context.TestContext;
import org.citrusframework.exceptions.CitrusRuntimeException;
import org.citrusframework.util.ReflectionHelper;
import org.citrusframework.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.IHookCallBack;
import org.testng.IMethodInstance;
import org.testng.ITestContext;
import org.testng.ITestNGMethod;
import org.testng.ITestResult;
import org.testng.SkipException;
import org.testng.internal.BaseTestMethod;
import org.testng.internal.MethodInstance;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public final class TestNGHelper {

    public static final String BUILDER_ATTRIBUTE = "builder";

    private static final String TIMED_SOURCES_ATTRIBUTE = TestNGHelper.class.getName() + ".timedSources";

    /** Logger */
    private static final Logger logger = LoggerFactory.getLogger(TestNGHelper.class);

    /**
     * Prevent instantiation of utility class
     */
    private TestNGHelper() {
        // prevent instantiation
    }

    /**
     * Invokes test method.
     * @param target
     * @param testResult
     * @param method
     * @param testLoader
     * @param context
     * @param invocationCount
     */
    public static void invokeTestMethod(Object target, ITestResult testResult, Method method,
                                        TestLoader testLoader, TestContext context, int invocationCount) {

        try {
            Object[] params = TestNGParameterHelper.resolveParameter(target, testResult, method,
                context, invocationCount);
            testLoader.configureTestCase(
                t -> TestNGParameterHelper.injectTestParameters(method, t, params));
            testLoader.doWithTestCase(t -> ReflectionHelper.invokeMethod(method, target, params));
            testLoader.load();
        } catch (CitrusRuntimeException e) {
            SkipException skipException = getCauseOfType(e, SkipException.class);
            if (skipException != null)  {
                throw skipException;
            }

            throw e;
        }
    }

    /**
     * Recursively checks if the cause of the given exception matches the target exception.
     *
     * @param exception The exception to check.
     * @param targetCause The exception cause to search for.
     * @return true if the target cause is found in the exception chain, false otherwise.
     */
    public static <T extends Throwable> T getCauseOfType(Throwable exception, Class<T> targetCause) {
        if (exception == null) {
            return null;
        }

        if (targetCause.isInstance(exception)) {
            return targetCause.cast(exception);
        }

        return getCauseOfType(exception.getCause(), targetCause);
    }

    /**
     * Creates new test runner instance for this test method.
     * @param method
     * @param context
     * @return
     */
    public static TestCaseRunner createTestCaseRunner(Object target, Method method, TestContext context) {
        TestCaseRunner testCaseRunner = TestCaseRunnerFactory.createRunner(new DefaultTestCase(), context);
        testCaseRunner.testClass(target.getClass());
        testCaseRunner.name(target.getClass().getSimpleName());
        testCaseRunner.packageName(target.getClass().getPackage().getName());

        if (method.getAnnotation(CitrusTest.class) != null) {
            CitrusTest citrusTestAnnotation = method.getAnnotation(CitrusTest.class);
            if (StringUtils.hasText(citrusTestAnnotation.name())) {
                testCaseRunner.name(citrusTestAnnotation.name());
            } else {
                testCaseRunner.name(method.getDeclaringClass().getSimpleName() + "." + method.getName());
            }
        }  else {
            testCaseRunner.name(method.getDeclaringClass().getSimpleName() + "." + method.getName());
        }

        return testCaseRunner;
    }

    /**
     * Schedules each resolved source on a Citrus TestNG support class.
     */
    public static List<IMethodInstance> scheduleTestSources(List<IMethodInstance> methods, ITestContext context,
                                                             Class<?> supportClass) {
        Map<ITestNGMethod, AtomicInteger> timedSources = new IdentityHashMap<>();
        List<IMethodInstance> scheduled = new ArrayList<>(methods.size());
        for (IMethodInstance method : methods) {
            scheduled.add(method);
            if (!supportClass.isInstance(method.getInstance())) {
                continue;
            }

            var testngMethod = method.getMethod();
            Method testMethod = testngMethod.getConstructorOrMethod().getMethod();
            CitrusTestSource annotation = testMethod.getAnnotation(CitrusTestSource.class);
            int sources = annotation == null ? 0 : TestSourceResolver.resolve(annotation, testMethod).size();
            if (sources <= 1) {
                continue;
            }

            int invocations = testngMethod.getInvocationCount();
            int threadPoolSize = testngMethod.getThreadPoolSize();
            if (testngMethod.getInvocationTimeOut() > 0) {
                if (!(testngMethod instanceof BaseTestMethod timedMethod)
                        || (invocations > 1 && threadPoolSize > 1)) {
                    // Preserve native scheduling for an already-active timed pool or custom method implementation.
                    for (int source = 1; source < sources; source++) {
                        scheduled.add(new MethodInstance(testngMethod));
                    }
                    continue;
                }
                // The Citrus hook handles one source per timed callback. Preserve the effective timeout for
                // each source and let TestNG report each callback as a separate invocation.
                if (timedMethod.getTimeOut() <= 0) {
                    timedMethod.setTimeOut(timedMethod.getInvocationTimeOut());
                }
                timedMethod.setInvocationTimeOut(0);
            }
            if (testngMethod.getTimeOut() > 0 && (invocations <= 1 || threadPoolSize <= 1)) {
                timedSources.put(testngMethod, new AtomicInteger());
            }
            // TestNG ignores a configured pool for one invocation. Source expansion must keep it serial.
            if (invocations == 1 && threadPoolSize > 1) {
                testngMethod.setThreadPoolSize(1);
            }
            testngMethod.setInvocationCount(Math.multiplyExact(sources, invocations));
        }
        if (!timedSources.isEmpty()) {
            if (context.getAttribute(TIMED_SOURCES_ATTRIBUTE) instanceof TimedSources existing) {
                existing.ordinals().putAll(timedSources);
            } else {
                context.setAttribute(TIMED_SOURCES_ATTRIBUTE, new TimedSources(timedSources));
            }
        }
        return scheduled;
    }

    /**
     * Resolves the source index for one invocation. TestNG advances its native index inside and outside a timed callback.
     */
    public static int sourceIndex(ITestResult result, int invocationCount, int sourceCount) {
        ITestContext context = result.getTestContext();
        if (context != null && context.getAttribute(TIMED_SOURCES_ATTRIBUTE) instanceof TimedSources timedSources) {
            AtomicInteger ordinal = timedSources.ordinals().get(result.getMethod());
            if (ordinal != null) {
                return ordinal.getAndIncrement() % sourceCount;
            }
        }
        return invocationCount % sourceCount;
    }

    private record TimedSources(Map<ITestNGMethod, AtomicInteger> ordinals) {
    }

    /**
     * Creates test loader from @CitrusTestSource(type = TestLoader.SPRING) annotated test method and saves those to local member.
     * Test loaders get executed later when actual method is called by TestNG. This way user can annotate
     * multiple methods in one single class each executing several Citrus XML tests.
     *
     * @param method
     * @param provider
     * @return
     */
    public static List<TestLoader> createMethodTestLoaders(Method method, TestLoaderProvider provider) {
        List<TestLoader> methodTestLoaders = new ArrayList<>();

        if (method.getAnnotation(CitrusTestSource.class) != null) {
            CitrusTestSource citrusTestAnnotation = method.getAnnotation(CitrusTestSource.class);
            for (ResolvedTestSource resolvedTestSource : TestSourceResolver.resolve(citrusTestAnnotation, method)) {
                TestLoader testLoader = provider.createTestLoader(resolvedTestSource.getTestName(),
                        resolvedTestSource.getPackageName(), citrusTestAnnotation.type());

                if (resolvedTestSource.getSource() != null) {
                    if (testLoader instanceof TestSourceAware testSourceAware) {
                        testSourceAware.setSource(TestSourceHelper.create(resolvedTestSource.getSource()));
                        methodTestLoaders.add(testLoader);
                    } else {
                        logger.warn("Test loader {} is not able to handle test source {}", testLoader.getClass(), resolvedTestSource.getSource());
                    }
                } else {
                    methodTestLoaders.add(testLoader);
                }
            }
        }

        return methodTestLoaders;
    }

    @FunctionalInterface
    public interface TestLoaderProvider {
        /**
         * Creates new test loader which has TestNG test annotations set for test execution. Only
         * suitable for tests that get created at runtime through factory method. Subclasses
         * may overwrite this in order to provide custom test loader with custom test annotations set.
         * @param testName
         * @param packageName
         * @param type
         * @return
         */
        TestLoader createTestLoader(String testName, String packageName, String type);
    }

    /**
     * Class faking test execution as callback. Used in run hookable method when test case
     * was executed before and callback is needed for super class run method invocation.
     */
    public static final class FakeExecutionCallBack implements IHookCallBack {
        private Object[] parameters;

        public FakeExecutionCallBack(Object[] parameters) {
            this.parameters = Arrays.copyOf(parameters, parameters.length);
        }

        @Override
        public void runTestMethod(ITestResult testResult) {
            // do nothing as test case was already executed
        }

        @Override
        public Object[] getParameters() {
            return Arrays.copyOf(parameters, parameters.length);
        }

    }
}
