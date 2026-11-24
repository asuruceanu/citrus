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

import org.citrusframework.annotations.CitrusTestSource;
import org.citrusframework.common.TestSourceResolver;
import org.testng.IMethodInstance;
import org.testng.IMethodInterceptor;
import org.testng.ITestContext;

import java.util.List;

/**
 * Applies the configured TestNG invocation count to each resolved Citrus test source.
 *
 * @since 1.3.1
 */
public class TestNGCitrusMethodInterceptor implements IMethodInterceptor {

    @Override
    public List<IMethodInstance> intercept(List<IMethodInstance> methods, ITestContext context) {
        for (IMethodInstance method : methods) {
            if (method.getInstance() instanceof TestNGCitrusSupport) {
                var testngMethod = method.getMethod();
                var testMethod = testngMethod.getConstructorOrMethod().getMethod();
                CitrusTestSource annotation = testMethod.getAnnotation(CitrusTestSource.class);
                if (annotation != null) {
                    int sources = TestSourceResolver.resolve(annotation, testMethod).size();
                    if (sources > 1) {
                        // TestNG repeats each source according to the configured invocation count.
                        testngMethod.setInvocationCount(Math.multiplyExact(sources, testngMethod.getInvocationCount()));
                    }
                }
            }
        }
        return methods;
    }
}
