# citrus-graphql — GraphQLZero proof of concept

Integration tests of the `citrus-graphql` module against the public
[GraphQLZero](https://graphqlzero.almansi.me) API (`https://graphqlzero.almansi.me/api`, an Apollo
Server serving the JSONPlaceholder data set). **Needs internet access.**

| Test | DSL | Covers |
|---|---|---|
| `GraphQlZeroJavaIT` | Java | all 30 operations (6 resources × get, list, create, update, delete), inline documents |
| `GraphQlZeroResourceIT` | Java | the same 30 operations, documents loaded from `graphql/zero/*.graphql` |
| `GraphQlZeroSendReceiveIT` | Java | the same 30 operations with raw send/receive action builders, records and maps as variables, responses mapped to records |
| `GraphQlZeroXmlIT` | XML | all 30 operations, one test per resource (`*.citrus.it.xml`) |
| `GraphQlZeroYamlIT` | YAML | all 30 operations, one test per resource (`*.citrus.it.yaml`) |
| `GraphQlZeroEdgeCasesIT` | Java | GET + Apollo CSRF preflight, schema check before sending, server validation errors (400), unexpected errors, null data, multi-operation documents, aliases/fragments, resource documents, chaining |

Every client validates documents against `src/test/resources/graphql/graphqlzero.graphqls`, the
schema obtained by introspection of the live API.

```bash
./mvnw verify -pl endpoints/citrus-graphql-zero-poc -am -Dskip.unit.tests=true \
    -Dit.test='GraphQlZero*IT' -Dfailsafe.failIfNoSpecifiedTests=false
```

`-am` builds this branch's Citrus modules in the same run, so the tests never pick up other
5.1.0-SNAPSHOT builds from the local Maven repository.

The Java (inline), XML and YAML operation tests are generated from one operation table; the resource
and raw-action suites reuse the same documents and expectations, so all five runs exercise exactly
the same requests.

The full test report (approach, coverage, examples, findings) is in [REPORT.html](REPORT.html).
