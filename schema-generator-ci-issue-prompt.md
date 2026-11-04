# Prompt: investigate CitrusSchemaGeneratorTest failure in partial-reactor CI builds

Use this prompt with Claude (paste into chat with repo access to citrusframework/citrus).

---

## Task

`org.citrusframework.dsl.schema.generator.CitrusSchemaGeneratorTest.verifyAggregateTestActionsJson`
fails in GitHub Actions PR builds that run a **partial (GIB incremental) reactor**, but passes in
full `main` builds and in full local builds. Find the root cause. It is NOT caused by any PR diff
content (evidence below) — suspect the CI artifact mix (stale cached SNAPSHOT on the generator's
test classpath) or a reactor-composition effect.

## Failing assertion

```
ValidationException: An item in $['createEndpoint']['propertiesSchema']['anyOf'] is missing
```

Strict JSON comparison of generated `citrus-catalog-aggregate-test-actions.json`
(`CitrusSchemaGenerator.main()` output) against committed control files
(`tools/schema-generator/src/test/resources/control/`). The generated `createEndpoint` schema is
missing one `anyOf` entry that the control file expects (previously identified as the `http`
group item — verify which item is missing from the current surefire output, it may vary).

## Evidence (verified 2026-11-02)

- Failing runs (partial reactors that include `tools/schema-generator` because its pom depends on
  `citrus-validation-hamcrest`, `citrus-validation-assertj`, `citrus-validation-json`):
  hamcrest PR #1901 runs 41432957641, 41433291360 (initial AND a maintainer rerun — rerun still
  fails, so not a one-off flake); json PR #1903 runs 41432961213, 41433296730; xml PR #1904 runs
  41432978253, 41433309143.
- Passing runs covering the same commit range: full `main` push build 41427386768 (10:15 UTC),
  nightly/scheduled main builds, and PR reactors that do NOT execute this test
  (groovy #1902 rerun 41436027317, yaml #1898, assertj #1900, binary #1897, text #1899).
- Branch diffs are test-only files in validation modules; the generator's inputs
  (generator sources, endpoint builder classes, control files) are source-identical to `main`.
  `git diff --check` clean; per-branch diffs verified module-only after rebase onto `main`.
- Local reproduction: with a mixed `~/.m2`, the same test fails with a DIFFERENT diff
  (camel `autoSetProperties` content mismatch) — proving this failure signature arises from
  SNAPSHOT artifact version skew, not source bugs.
- CI cache config (`.github/workflows/build.yml`): `actions/cache@v5`, key
  `Linux-m2-${{ hashFiles('**/pom.xml') }}`, restore-keys `Linux-m2`. Many concurrent PR runs
  share the restore prefix.

## Key code paths to inspect

- `tools/schema-generator/src/main/java/org/citrusframework/dsl/schema/generator/CitrusSchemaGenerator.java`
  (`main`, `generateSchema`, aggregate catalog writing).
- `tools/schema-generator/src/main/java/org/citrusframework/dsl/schema/Catalog.java`
  (`getTestActionCatalog`, `getEndpointCatalog` via `EndpointBuilder.lookup()` — note the recent
  lookup deadlock fix #1885 and static-cache thread-safety fix #1878).
- `tools/schema-generator/src/main/java/org/citrusframework/dsl/schema/Endpoints.java`
  (`@SchemaType(oneOf = {...})` — 19 endpoint groups incl. `http`).
- `tools/schema-generator/src/main/java/org/citrusframework/dsl/schema/generator/CitrusModule.java`
  (`resolveTypeAttribute`: `if (anyOf.isEmpty())` guard — check when the base victools generator
  pre-populates `anyOf`, e.g. via subtype/ServiceLoader discovery, and whether that depends on
  which endpoint jars are fresh-built vs cache-resolved).
- `tools/schema-generator/pom.xml` test-classpath endpoint deps (mostly NOT in a partial reactor:
  camel, ws, websocket, graphql, grpc, mail, docker, kubernetes, ftp, vertx, sql, selenium,
  spring-integration, ...).
- `tools/schema-generator/src/test/java/.../CitrusSchemaGeneratorTest.java` and
  `src/test/resources/control/citrus-catalog-aggregate-test-actions.json`.

## Questions to answer

1. Which exact `anyOf` item is missing in the failing CI output, and which artifact on the test
   classpath provides its builder class?
2. Was that artifact fresh-built or cache-resolved in the failing partial reactors? (Compare the
   failing reactor module list, e.g. hamcrest run: 31 modules ending at schema-generator [24/31],
   with the generator's full dependency closure.)
3. Is the restored `Linux-m2*` cache guaranteed to postdate the latest `main` merge that touched
   endpoint builders or control files? Check `git log` on control files vs cache-save timestamps.
4. Can you reproduce locally by deleting `~/.m2` selectively (e.g. one endpoint SNAPSHOT) and
   running `mvnw verify -pl tools/schema-generator` without `-am`?

## Constraints

- Do NOT propose changes to validation-module PR branches (#1897–#1904); their content is proven
  unrelated.
- Prefer a CI/build-level fix (cache key scoping, `-am` for schema-generator, control-file
  regeneration check) over generator code changes, unless a genuine code bug is found.
- Report: root cause, minimal fix, and how to verify (which single CI run proves it).
