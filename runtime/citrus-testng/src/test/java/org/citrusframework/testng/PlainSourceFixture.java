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

import java.util.ArrayList;
import java.util.List;

import org.citrusframework.CitrusContext;
import org.citrusframework.annotations.CitrusTestSource;
import org.citrusframework.api.common.TestLoader;
import org.testng.annotations.Test;

public class PlainSourceFixture extends TestNGCitrusSupport {
    private static final String FIRST = "classpath:org/citrusframework/testng/main/echo.citrus.it.yaml";
    private static final String SECOND = "classpath:org/citrusframework/testng/main/second-echo.citrus.it.yaml";
    static final List<String> SOURCES = new ArrayList<>();

    @Override
    protected void before(CitrusContext context) {
        TestNGCitrusMethodInterceptorTest.recordSources(context, SOURCES);
    }

    @Test
    @CitrusTestSource(type = TestLoader.YAML, sources = {FIRST, SECOND})
    public void runSources() {
    }

    @Test(invocationCount = 2)
    @CitrusTestSource(type = TestLoader.YAML, sources = {FIRST, SECOND})
    public void repeatSources() {
    }
}
