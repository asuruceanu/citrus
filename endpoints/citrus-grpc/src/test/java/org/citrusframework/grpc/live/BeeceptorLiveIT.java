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
package org.citrusframework.grpc.live;

import org.citrusframework.annotations.CitrusTest;
import org.citrusframework.dsl.TestActionSupport;
import org.citrusframework.grpc.client.GrpcClient;
import org.citrusframework.grpc.endpoint.GrpcEndpoints;
import org.citrusframework.spi.BindToRegistry;
import org.citrusframework.testng.spring.TestNGCitrusSpringSupport;
import org.testng.annotations.Test;

import static org.citrusframework.json.dsl.JsonPathSupport.jsonPath;

/**
 * Live test against the public Beeceptor gRPC mock service
 * {@code gr07b55f8cdfc9.free.beeceptor.com:443} (service {@code grpc2grpc.PetStoreService}).
 * <p>
 * It proves that the Citrus gRPC client works against a real TLS endpoint on the public internet,
 * using descriptors from a classpath {@code .proto} file and the JVM's default trust store - no
 * generated stubs in the test project.
 * <p>
 * Run it with:
 *
 * <pre>
 * ./mvnw verify -Dit.test=BeeceptorLiveIT -Dfailsafe.failIfNoSpecifiedTests=false \
 *     -Dskip.unit.tests=true -pl endpoints/citrus-grpc -am
 * </pre>
 */
@Test
public class BeeceptorLiveIT extends TestNGCitrusSpringSupport implements TestActionSupport {

    private static final String TARGET = "gr07b55f8cdfc9.free.beeceptor.com:443";
    private static final String DESCRIPTORS = "classpath:org/citrusframework/grpc/live/petstore.proto";

    @BindToRegistry
    private final GrpcClient petStore = GrpcEndpoints.grpc().client()
            .target(TARGET)
            .descriptors(DESCRIPTORS)
            .tls(true)
            .timeout(10000L)
            .build();

    @CitrusTest
    public void shouldCallPetById() {
        when(grpc().client(petStore).send()
                .call("grpc2grpc.PetStoreService/PetById")
                .message()
                .body("{ \"id\": 123 }"));

        then(grpc().client(petStore).receive()
                .response()
                .message()
                .validate(jsonPath().expression("$.pet.id", "@isNumber()@"))
                .validate(jsonPath().expression("$.pet.name", "@notEmpty()@")));
    }

    @CitrusTest
    public void shouldCallUserByName() {
        when(grpc().client(petStore).send()
                .call("grpc2grpc.PetStoreService/UserByName")
                .message()
                .body("{ \"username\": \"ada\" }"));

        then(grpc().client(petStore).receive()
                .response()
                .message()
                .validate(jsonPath().expression("$.user.username", "@notEmpty()@"))
                .validate(jsonPath().expression("$.user.email", "@contains('@')@")));
    }

    @CitrusTest
    public void shouldCallClientStreamingMethod() {
        when(grpc().client(petStore).send()
                .call("grpc2grpc.PetStoreService/StoreUsers")
                .message()
                .body("{ \"id\": 1, \"username\": \"ada\", \"email\": \"ada@example.com\", \"phone\": \"555-1\" }"));

        when(grpc().client(petStore).send()
                .streamMessage()
                .message()
                .body("{ \"id\": 2, \"username\": \"bob\", \"email\": \"bob@example.com\", \"phone\": \"555-2\" }"));

        when(grpc().client(petStore).send()
                .streamMessage()
                .message()
                .body("{ \"id\": 3, \"username\": \"cid\", \"email\": \"cid@example.com\", \"phone\": \"555-3\" }"));

        when(grpc().client(petStore).send().streamCompleted());

        then(grpc().client(petStore).receive()
                .response()
                .message()
                .validate(jsonPath().expression("$.msg", "@notEmpty()@")));
    }

    @CitrusTest
    public void shouldCallBidirectionalMethod() {
        when(grpc().client(petStore).send()
                .call("grpc2grpc.PetStoreService/BulkUsers")
                .message()
                .body("{ \"id\": 1, \"username\": \"ada\", \"email\": \"ada@example.com\", \"phone\": \"555-1\" }"));

        then(grpc().client(petStore).receive()
                .streamMessage()
                .message()
                .validate(jsonPath().expression("$.username", "@notEmpty()@")));

        // keep sending while the response side is still open
        when(grpc().client(petStore).send()
                .streamMessage()
                .message()
                .body("{ \"id\": 2, \"username\": \"bob\", \"email\": \"bob@example.com\", \"phone\": \"555-2\" }"));

        then(grpc().client(petStore).receive()
                .streamMessage()
                .message()
                .validate(jsonPath().expression("$.username", "@notEmpty()@")));

        when(grpc().client(petStore).send().streamCompleted());

        then(grpc().client(petStore).receive().streamCompleted());
    }

    @CitrusTest
    public void shouldReceiveStreamMessageAndCancel() {
        when(grpc().client(petStore).send()
                .call("grpc2grpc.PetStoreService/ListUsers")
                .message()
                .body("{ \"msg\": \"hello\" }"));

        // the mock streams a random number of users - take one and cancel the subscription
        then(grpc().client(petStore).receive()
                .streamMessage()
                .message()
                .validate(jsonPath().expression("$.username", "@notEmpty()@")));

        then(grpc().client(petStore).cancel());
    }
}
