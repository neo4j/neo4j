#!/bin/sh

test_description="Test happy path operations for the daemon"

. ./lib/sharness.sh
fake_install
export HARNESS_JAVA_SLEEP=1

test_expect_success "should report that it's not running" "
  test_expect_code 3 neo4j-home/bin/neo4j status
"

test_expect_success "should start" "
  neo4j-home/bin/neo4j start
"

test_expect_success "should report that it's running" "
  neo4j-home/bin/neo4j status
"

test_expect_success "should stop" "
  neo4j-home/bin/neo4j stop
"

test_done
