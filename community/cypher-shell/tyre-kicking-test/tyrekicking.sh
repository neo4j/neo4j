#!/usr/bin/env bash
set -eu
# tyrekicking.sh:
# Minimal test to check if Cypher Shell executable works

if [ $# -ne 2 ]
then
  echo "Usage: ${0} <workspace> <zipfile>"
  exit 1
fi

workspace=${1}
zipfile=${2}

echo "Cypher Shell tyre kicking test"
echo "Zip file: ${zipfile}"

function prepare {
  mkdir -p ${workspace}
  unzip ${zipfile} -d ${workspace}
}

function prepare-bundle {
  mkdir -p ${workspace}/cypher-shell/tools
  mv ${workspace}/cypher-shell/*.jar ${workspace}/cypher-shell/tools
}

function testscript {
  # first try with encryption off (4.X series), if that fails with encryption on (3.X series)
  if ${workspace}/cypher-shell/cypher-shell -u neo4j -p neo "RETURN 1;"; then
    echo "$1 Success!"
  elif ${workspace}/cypher-shell/cypher-shell -a "bolt://localhost:7687" -u neo4j -p neo "RETURN 1;"; then
    echo "$1 Success!"
  else
    echo "$1 Failure!"
    exit 1
  fi
}

prepare
## Standalone test
testscript "Standalone"
## Fake bundling test
prepare-bundle
testscript "Bundling"
