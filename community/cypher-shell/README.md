# Cypher Shell

This folder contains Cypher Shell, the command line interface to Neo4J.

## How to build

### JAR and ZIP distribution

JAR and ZIP distribution are build using maven from `cypher-shell` subproject.

### RPM and Debian packages

RPM and Debian packages are build using make from `packaging` directory, see 
[packaging readme](packaging/README.md) for details.


## How to run

This clears any previously known neo4j hosts, starts a throw-away
instance of neo4j using docker, and connects to it.

```sh
rm -rf ~/.neo4j/known_hosts
docker run --rm -p 7687:7687 -e NEO4J_AUTH=none neo4j:4.1
make run
```

## Development

### Integration tests

See [integration-test](integration-test/README.md).

### Tyre Kicking Test

A minimal test of the executable can be run with `make tyre-kicking-test`.