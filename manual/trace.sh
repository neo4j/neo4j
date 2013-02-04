#!/bin/bash -e

#to search special characters, escape them.
#./trace.sh -t '\[\[cypher'

#output:
#target/docs/neo4j-cypher-docs-jar/dev/index.txt:1:[[cypher-query-lang]]
#target/docs/neo4j-cypher-docs-jar/dev/index.txt:52:[[cypher-parameters]]
#target/docs/neo4j-cypher-docs-jar/dev/index.txt:114:[[cypher-identifiers]]
#target/docs/neo4j-cypher-docs-jar/dev/ql/cookbook/index.txt:1:[[cypher-cookbook]]

if test -z "$1"
then
  echo
  echo 'Usage:'
  echo './trace.sh "string to search for"'
  echo './trace.sh -c "string to search for in code only"'
  echo './trace.sh -t "string to search for in text only"'
  echo "./trace.sh -t '\[\[cypher' # escape characters"
  echo "Note1: If target/docs doesn't exist, mvn generate-resources will be invoked to create it."
  echo "Note2: The search uses grep, that is, the search string is treated as a regular expression."
  echo
  exit 0;
fi

if [[ ! -d "target/docs" ]] ; then
  mvn generate-resources
fi

case "$1" in
  -t) grep --color=always -i -n -r "$2" target/docs/* target/src/*
  ;;
  -c) grep --color=always -i -n -r "$2" target/sources/* target/test-sources/*
  ;;
  *) grep --color=always -i -n -r "$1" target/docs/* target/sources/* target/test-sources/* target/src/*
esac

