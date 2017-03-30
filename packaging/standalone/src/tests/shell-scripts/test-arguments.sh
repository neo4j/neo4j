#!/usr/bin/env bash

test_description="Test handling of command line arguments"

. ./lib/sharness.sh
fake_install

test_expect_success "should print usage to stderr and exit non-zero for unknown argument" "
  test_expect_stderr_matching '^Usage: ' \
      test_expect_code 1 neo4j-home/bin/neo4j nonsense
"

test_expect_success "should print usage stdout and exit zero when help requested explicitly" "
  test_expect_stdout_matching '^Usage: ' neo4j-home/bin/neo4j help
"

test_expect_success "should include script name in usage" "
  test_expect_stdout_matching ' neo4j ' neo4j-home/bin/neo4j help
"

test_expect_success "should include --version in java args when --version given" "
  neo4j-home/bin/neo4j --version && test_expect_java_arg '--version'
"

test_expect_success "should include --version in java args when version given" "
  neo4j-home/bin/neo4j version && test_expect_java_arg '--version'
"

test_done
