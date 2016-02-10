#!/usr/bin/env bash

test_description="Test neo4j-admin upgrade"

. ./lib/sharness.sh
fake_install

test_expect_success "should fail if source is missing" "
  test_expect_stderr_matching 'you must provide the --from option with an argument' \
    test_expect_code 1 neo4j-home/bin/neo4j-admin upgrade
"

test_expect_success "should run successfully" "
  neo4j-home/bin/neo4j-admin upgrade --from=old-neo4j-home
"

test_done
