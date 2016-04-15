#!/usr/bin/env bash

test_description="Test differences running the shell"

. ./lib/sharness.sh
fake_install

test_expect_success "should invoke shell main class" "
  neo4j-home/bin/neo4j-shell || true &&
  test_expect_java_arg 'org.neo4j.shell.StartClient'
"

test_expect_success "should pass absolute paths through" "
  neo4j-home/bin/neo4j-shell --file /tmp/test-absolute-file || true &&
  test_expect_java_arg '--file /tmp/test-absolute-file'
"

test_expect_success "should rewrite local paths through" "
  neo4j-home/bin/neo4j-shell --file tmp/test-relative-file || true &&
  test_expect_java_arg '--file $(pwd)/tmp/test-relative-file'
"

test_done
