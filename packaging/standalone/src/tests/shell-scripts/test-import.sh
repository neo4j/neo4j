#!/usr/bin/env bash

test_description="Test running neo4j-import"

. ./lib/sharness.sh
fake_install

test_expect_success "should invoke import main class" "
  neo4j-home/bin/neo4j-import || true &&
  test_expect_java_arg 'org.neo4j.tooling.ImportTool'
"

test_done
