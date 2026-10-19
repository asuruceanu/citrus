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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Grpc;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.protobuf.ProtoUtils;
import org.citrusframework.grpc.descriptor.GrpcDescriptorRegistry;
import org.citrusframework.grpc.message.GrpcMessageCodec;
import org.citrusframework.grpc.tls.GrpcTlsSupport;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * PoC spike for the two RPC kinds the Citrus gRPC module does not support yet: client streaming
 * ({@code StoreUsers}) and bidirectional streaming ({@code BulkUsers}).
 * <p>
 * The spike uses exactly the parts a future Citrus feature would build on - the descriptor registry
 * from a classpath {@code .proto} file, dynamic Protobuf messages and the module's JSON codec - but
 * drives the raw grpc-java {@link ClientCall} directly, because the Citrus DSL deliberately stops at
 * unary and server-streaming calls (spec D3). It answers the PoC question "could these be tested?"
 * with the wire-level evidence; see the notes at the bottom of the report for what Citrus would have
 * to add.
 */
@Test
public class BeeceptorStreamingSpikeIT {

    private static final String TARGET = "gr07b55f8cdfc9.free.beeceptor.com:443";
    private static final String DESCRIPTORS = "classpath:org/citrusframework/grpc/live/petstore.proto";

    private ManagedChannel channel;
    private GrpcDescriptorRegistry registry;
    private GrpcMessageCodec codec;

    @BeforeClass(alwaysRun = true)
    public void connect() {
        channel = Grpc.newChannelBuilder(TARGET, GrpcTlsSupport.clientCredentials()).build();
        registry = new GrpcDescriptorRegistry(List.of(DESCRIPTORS));
        codec = new GrpcMessageCodec(registry);
    }

    @AfterClass(alwaysRun = true)
    public void disconnect() {
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    @Test
    public void shouldCallClientStreamingStoreUsers() throws Exception {
        Descriptors.MethodDescriptor method = registry.findMethod("grpc2grpc.PetStoreService/StoreUsers");
        Descriptors.Descriptor userType = registry.findMessage("grpc2grpc.User");

        List<DynamicMessage> responses = new ArrayList<>();
        AtomicReference<Status> status = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        ClientCall<DynamicMessage, DynamicMessage> call =
                channel.newCall(method(method), CallOptions.DEFAULT);
        call.start(new ClientCall.Listener<>() {
            @Override
            public void onMessage(DynamicMessage message) {
                responses.add(message);
            }

            @Override
            public void onClose(Status closeStatus, Metadata trailers) {
                status.set(closeStatus);
                done.countDown();
            }
        }, new Metadata());

        call.request(1);
        call.sendMessage(user(userType, 1, "ada"));
        call.sendMessage(user(userType, 2, "bob"));
        call.sendMessage(user(userType, 3, "cid"));
        call.halfClose();

        assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        assertThat(status.get().getCode()).isEqualTo(Status.Code.OK);
        assertThat(responses).hasSize(1);
        // the mock randomizes the message text - just require a non-empty msg field
        assertThat(codec.decode(responses.get(0))).containsPattern("\"msg\"\\s*:\\s*\"[^\"]+\"");
    }

    @Test
    public void shouldCallBidirectionalBulkUsers() throws Exception {
        Descriptors.MethodDescriptor method = registry.findMethod("grpc2grpc.PetStoreService/BulkUsers");
        Descriptors.Descriptor userType = registry.findMessage("grpc2grpc.User");

        List<DynamicMessage> responses = new ArrayList<>();
        AtomicReference<Status> status = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        ClientCall<DynamicMessage, DynamicMessage> call =
                channel.newCall(method(method), CallOptions.DEFAULT);
        call.start(new ClientCall.Listener<>() {
            @Override
            public void onMessage(DynamicMessage message) {
                responses.add(message);
            }

            @Override
            public void onClose(Status closeStatus, Metadata trailers) {
                status.set(closeStatus);
                done.countDown();
            }
        }, new Metadata());

        call.request(10);
        call.sendMessage(user(userType, 1, "ada"));
        call.sendMessage(user(userType, 2, "bob"));
        call.halfClose();

        assertThat(done.await(15, TimeUnit.SECONDS)).isTrue();
        assertThat(status.get().getCode()).isEqualTo(Status.Code.OK);
        assertThat(responses).isNotEmpty();
        for (DynamicMessage response : responses) {
            assertThat(codec.decode(response)).contains("username");
        }
    }

    private static DynamicMessage user(Descriptors.Descriptor userType, int id, String username) {
        return DynamicMessage.newBuilder(userType)
                .setField(userType.findFieldByName("id"), id)
                .setField(userType.findFieldByName("username"), username)
                .setField(userType.findFieldByName("email"), username + "@example.com")
                .setField(userType.findFieldByName("phone"), "555-" + id)
                .build();
    }

    private static MethodDescriptor<DynamicMessage, DynamicMessage> method(Descriptors.MethodDescriptor method) {
        return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
                .setType(methodType(method))
                .setFullMethodName(method.getService().getFullName() + "/" + method.getName())
                .setRequestMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(method.getInputType())))
                .setResponseMarshaller(ProtoUtils.marshaller(DynamicMessage.getDefaultInstance(method.getOutputType())))
                .build();
    }

    private static MethodDescriptor.MethodType methodType(Descriptors.MethodDescriptor method) {
        if (method.isClientStreaming() && method.isServerStreaming()) {
            return MethodDescriptor.MethodType.BIDI_STREAMING;
        }
        if (method.isClientStreaming()) {
            return MethodDescriptor.MethodType.CLIENT_STREAMING;
        }
        if (method.isServerStreaming()) {
            return MethodDescriptor.MethodType.SERVER_STREAMING;
        }
        return MethodDescriptor.MethodType.UNARY;
    }
}
