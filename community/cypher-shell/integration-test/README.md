# Cypher Shell Integration Tests

## How to run

1. Start Neo4j server with bolt driver on localhost.

   If authentication is required, it is assumed to be username `neo4j`
   and password `neo`.

2. Run integration tests using maven `integration-test` goal with
   `-DenableCypherShellIntegrationTest` (test is disabled without this flag).

   For example, to run only cypher shell integration tests from project root:

   ```
   mvn integration-test --projects org.neo4j:cypher-shell-integration-test -DenableCypherShellIntegrationTest
   ```

## How to run, the fast way

This clears any previously known neo4j hosts, starts a throw-away
instance of neo4j, and runs the integration tests against it.

```sh
rm -rf ~/.neo4j/known_hosts
docker run --rm -p 7687:7687 -e NEO4J_AUTH=none neo4j:4.1
mvn integration-test --projects org.neo4j:cypher-shell-integration-test --also-make -DenableCypherShellIntegrationTest
```
