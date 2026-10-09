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
package org.citrusframework.graphql.zero;

import org.citrusframework.annotations.CitrusTest;
import org.citrusframework.dsl.TestActionSupport;
import org.citrusframework.exceptions.ValidationException;
import org.citrusframework.graphql.client.GraphQlClient;
import org.citrusframework.graphql.endpoint.builder.GraphQlEndpoints;
import org.citrusframework.spi.BindToRegistry;
import org.citrusframework.testng.spring.TestNGCitrusSpringSupport;
import org.testng.annotations.Test;

import static org.citrusframework.api.graphql.GraphQlError.error;
import static org.citrusframework.dsl.MessageSupport.MessageBodySupport.fromBody;

/**
 * Edge cases against the real GraphQLZero (Apollo Server) API: GET with Apollo's CSRF preflight
 * header, schema validation before sending, server-side validation errors, unexpected errors,
 * null data, multi-operation documents, aliases and fragments, resource documents and chaining
 * values between requests.
 */
@Test
public class GraphQlZeroEdgeCasesIT extends TestNGCitrusSpringSupport implements TestActionSupport {

    @BindToRegistry
    private final GraphQlClient graphqlZero = GraphQlEndpoints.graphql().client()
            .requestUrl(GraphQlZeroJavaIT.API)
            .schema("classpath:graphql/graphqlzero.graphqls")
            .timeout(20000L)
            .build();

    /** Apollo only accepts GET with a preflight header (CSRF prevention). */
    @CitrusTest
    public void queryOverGetWithPreflightHeader() {
        var request = graphql().client(graphqlZero)
                .send()
                .get()
                .query("query GetPost($id: ID!) { post(id: $id) { id title } }")
                .variable("id", "1");
        request.message().header("apollo-require-preflight", "true");
        $(request);

        $(graphql().client(graphqlZero)
                .receive()
                .data("$.post.title", "sunt aut facere repellat provident occaecati excepturi optio reprehenderit")
                .message()
                .status(200));
    }

    @CitrusTest
    public void queryOverGetWithoutPreflightIsBlocked() {
        $(graphql().client(graphqlZero)
                .send()
                .get()
                .query("{ post(id: 1) { id } }"));

        $(graphql().client(graphqlZero)
                .receive()
                .expectError(error().code("BAD_REQUEST").message("@contains('Cross-Site Request Forgery')@"))
                .message()
                .status(400));
    }

    /** The downloaded schema rejects the document before anything is sent. */
    @CitrusTest
    public void schemaRejectsUnknownFieldBeforeSending() {
        $(assertException()
                .exception(ValidationException.class)
                .message("@contains('isbn')@")
                .when(graphql().client(graphqlZero)
                        .send()
                        .query("{ post(id: 1) { isbn } }")));
    }

    /** With strict checks off the server validates: Apollo answers 400 with graphql-response+json. */
    @CitrusTest
    public void serverRejectsUnknownField() {
        $(graphql().client(graphqlZero)
                .send()
                .strict(false)
                .query("{ post(id: 1) { isbn } }"));

        $(graphql().client(graphqlZero)
                .receive()
                .expectError(error().code("GRAPHQL_VALIDATION_FAILED").message("Cannot query field \"isbn\" on type \"Post\"."))
                .message()
                .status(400)
                .header("Content-Type", "@startsWith('application/graphql-response+json')@"));
    }

    /** Apollo also answers 400 for legacy application/json (the spec would allow 200 there). */
    @CitrusTest
    public void serverRejectsUnknownFieldWithLegacyJson() {
        var request = graphql().client(graphqlZero)
                .send()
                .strict(false)
                .query("{ post(id: 1) { isbn } }");
        request.message().accept("application/json");
        $(request);

        $(graphql().client(graphqlZero)
                .receive()
                .expectErrors()
                .message()
                .status(400)
                .header("Content-Type", "@startsWith('application/json')@"));
    }

    @CitrusTest
    public void unexpectedErrorsFailTheTest() {
        $(graphql().client(graphqlZero)
                .send()
                .strict(false)
                .query("{ post(id: 1) { isbn } }"));

        $(assertException()
                .exception(ValidationException.class)
                .message("@startsWith('GraphQL response contains 1 unexpected error: [GRAPHQL_VALIDATION_FAILED]')@")
                .when(graphql().client(graphqlZero).receive()));
    }

    /** An unknown id is not an error in GraphQLZero: the fields come back null. */
    @CitrusTest
    public void unknownIdReturnsNullFields() {
        $(graphql().client(graphqlZero)
                .send()
                .query("query GetPost($id: ID!) { post(id: $id) { id title } }")
                .variable("id", "9999"));

        $(graphql().client(graphqlZero)
                .receive()
                .data("{\"post\": {\"id\": null, \"title\": null}}"));
    }

    @CitrusTest
    public void multiOperationDocumentWithOperationName() {
        $(graphql().client(graphqlZero)
                .send()
                .query("""
                        query GetUser { user(id: 2) { name } }
                        query GetTodo { todo(id: 2) { title completed } }""")
                .operationName("GetTodo"));

        $(graphql().client(graphqlZero)
                .receive()
                .data("{\"todo\": {\"title\": \"@ignore@\", \"completed\": \"@ignore@\"}}"));
    }

    @CitrusTest
    public void aliasesAndFragments() {
        $(graphql().client(graphqlZero)
                .send()
                .query("""
                        query Users {
                          first: user(id: 1) { ...UserFields }
                          second: user(id: 2) { ...UserFields }
                        }
                        fragment UserFields on User { name username }"""));

        $(graphql().client(graphqlZero)
                .receive()
                .data("$.first.username", "Bret")
                .data("$.second.name", "Ervin Howell"));
    }

    @CitrusTest
    public void nestedQueryFromResourceWithTypedOptions() {
        variable("limit", "3");

        $(graphql().client(graphqlZero)
                .send()
                .queryResource("classpath:graphql/post-with-comments.graphql")
                .variables("{\"id\": \"1\", \"options\": {\"paginate\": {\"page\": 1, \"limit\": ${limit}}}}"));

        $(graphql().client(graphqlZero)
                .receive()
                .data("$.post.comments.data.length()", 3)
                .data("$.post.comments.meta.totalCount", 5)
                .data("$.post.comments.data[0].email", "Eliseo@gardner.biz"));
    }

    /** A value extracted from one response drives the next request. */
    @CitrusTest
    public void chainValuesBetweenRequests() {
        $(graphql().client(graphqlZero)
                .send()
                .query("query GetPost($id: ID!) { post(id: $id) { user { id } } }")
                .variable("id", "11"));

        $(graphql().client(graphqlZero)
                .receive()
                .message()
                .extract(fromBody().expression("$.data.post.user.id", "authorId")));

        $(graphql().client(graphqlZero)
                .send()
                .query("query GetUser($id: ID!) { user(id: $id) { id username } }")
                .variable("id", "${authorId}"));

        $(graphql().client(graphqlZero)
                .receive()
                .data("$.user.id", "${authorId}")
                .data("$.user.username", "Antonette"));
    }
}
