#!/bin/bash -e

#to search special characters, escape them.
#./trace.sh -t '\[\[cypher'

#output:
#target/docs/neo4j-cypher-docs-jar/dev/index.txt:1:[[cypher-query-lang]]
#target/docs/neo4j-cypher-docs-jar/dev/index.txt:52:[[cypher-parameters]]
#target/docs/neo4j-cypher-docs-jar/dev/index.txt:114:[[cypher-identifiers]]
#target/docs/neo4j-cypher-docs-jar/dev/ql/cookbook/index.txt:1:[[cypher-cookbook]]
#target/docs/neo4j-cypher-plugin-docs-jar/dev/plugins/cypher/index.txt:1:[[cypher-plugin]]

if [[ ! -d "target/docs" ]] ; then
    mvn generate-resources
fi

case "$1" in
  -t) grep --color=always -i -n -r "$2" target/docs/*
  ;;
  -c) grep --color=always -i -n -r "$2" target/sources/* target/test-sources/*
  ;;
  *) grep --color=always -i -n -r "$1" target/docs/* target/sources/* target/test-sources/*
esac

