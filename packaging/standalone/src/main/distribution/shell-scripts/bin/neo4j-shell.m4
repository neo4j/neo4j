#!/usr/bin/env bash

include(src/main/distribution/shell-scripts/bin/neo4j-shared.m4)

call_main_class "org.neo4j.shell.StartClient" "$@"
