#!/usr/bin/env bash

test_description="Test handling of command line arguments"

. ./lib/sharness.sh
fake_install

test_expect_success "should print usage for unknown argument" "
  test_expect_stdout_matching '^Usage: ' neo4j-home/bin/neo4j nonsense
"

test_expect_success "should include script name in usage" "
  test_expect_stdout_matching ' neo4j ' neo4j-home/bin/neo4j help
"

test_done
