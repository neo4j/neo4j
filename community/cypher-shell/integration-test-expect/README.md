# Cypher Shell Integration Tests

This module contains integration tests that manage their own dependencies with testcontainers.

## How to run

1. `mvn integration-test --projects org.neo4j:cypher-shell-integration-test-expect -DenableCypherShellIntegrationTest`
   or directly from your IDE of choice.

## Add tests

1. Create an [Expect](https://core.tcl-lang.org/expect/index) script in `src/test/resources/expect/tests/`.
2. Create a text file with expected interaction with the same name and `.expected` appended.
3. Run any test extending `ExpectTestBase`.