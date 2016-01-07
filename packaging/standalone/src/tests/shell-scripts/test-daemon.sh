#!/usr/bin/env bash

test_description="Test happy path operations for the daemon"

. ./lib/sharness.sh
fake_install
export FAKE_JAVA_SLEEP=1

test_expect_success "should report that it's not running before started" "
  test_expect_code 3 neo4j-home/bin/neo4j status
"

test_expect_success "should start" "
  neo4j-home/bin/neo4j start >neo4j.stdout
"

test_expect_success "should output server URL" "
  grep 'http://localhost:7474' neo4j.stdout
"

test_expect_success "should report that it's running" "
  neo4j-home/bin/neo4j status
"

test_expect_success "should redirect output to console.log" "
  grep 'stdout from java' neo4j-home/data/log/console.log &&
  grep 'stderr from java' neo4j-home/data/log/console.log
"

test_expect_success "should exit 0 if already running" "
  neo4j-home/bin/neo4j start
"

test_expect_success "should stop" "
  neo4j-home/bin/neo4j stop
"

test_expect_success "should report that it's not running once stopped" "
  test_expect_code 3 neo4j-home/bin/neo4j status
"

test_done
