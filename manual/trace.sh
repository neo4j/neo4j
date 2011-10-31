#!/bin/bash -e

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

