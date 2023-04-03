# Cypher Shell Integration Tests

These are two types of integration tests in this module.

1. [Expect](https://en.wikipedia.org/wiki/Expect) based tests, see package `org.neo4j.shell.expect`. 
  Uses testcontainers to spin up neo4j and other required dependencies automatically.
2. Java based tests, other classes. Requires running neo4j database.

## How to run all integration tests

1. Start Neo4j server on localhost. 
   If authentication is required, it is assumed to be username `neo4j` and password `neo`.
2. `mvn integration-test --projects org.neo4j:cypher-shell-integration-test -DenableCypherShellIntegrationTest`

## Add tests

It's preferred to use the expect type of tests. Add a test by:

1. Create a `.expect` script in `src/test/resources/expect/tests/`.
2. Create a text file with expected interaction with the same name and `.expected` appended.
3. Run `org.neo4j.shell.expect.CypherShellIntegrationTest` to run your test (it automatically picks up files in that location).