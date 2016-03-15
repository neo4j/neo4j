#!/usr/bin/env bash

test_description="Test neo4j-admin argument handling"

. ./lib/sharness.sh
fake_install

test_expect_success "should print usage for help command" "
  test_expect_stdout_matching 'Usage:' neo4j-home/bin/neo4j-admin help &&
  test_expect_stdout_matching 'Usage:' neo4j-home/bin/neo4j-admin --help &&
  test_expect_stdout_matching 'Usage:' neo4j-home/bin/neo4j-admin -h
"

test_expect_success "should fail if command is missing" "
  assert_failure_with_stderr 'you must provide a command' neo4j-home/bin/neo4j-admin
"

test_expect_success "should fail if command is unrecognized" "
  assert_failure_with_stderr 'unrecognised command' neo4j-home/bin/neo4j-admin rubbish
"

test_expect_success "should fail if --database is missing" "
  assert_failure_with_stderr 'you must provide the --database option with an argument' \
    neo4j-home/bin/neo4j-admin import --from=/foo --mode=database
"

test_expect_success "should fail if --from is missing" "
  assert_failure_with_stderr 'you must provide the --from option with an argument' \
    neo4j-home/bin/neo4j-admin import --database=foo --mode=database
"

test_expect_success "should fail if --mode is missing" "
  assert_failure_with_stderr 'you must provide the --mode option with an argument' \
    neo4j-home/bin/neo4j-admin import --database=foo --from=/foo
"

test_done
