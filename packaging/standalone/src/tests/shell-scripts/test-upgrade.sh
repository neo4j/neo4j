#!/usr/bin/env bash

test_description="Test neo4j-admin upgrade"

. ./lib/sharness.sh
fake_install
fake_install ./old-neo4j-home "2.3.0"
set_config "foo" "bar" neo4j.properties ./old-neo4j-home
set_config "server-setting" "server-value" neo4j-server.properties ./old-neo4j-home

test_expect_success "should fail if source is missing" "
  test_expect_stderr_matching 'you must provide the --from option with an argument' \
    test_expect_code 1 neo4j-home/bin/neo4j-admin upgrade
"

test_expect_success "should run successfully" "
  neo4j-home/bin/neo4j-admin upgrade --from=./old-neo4j-home
"

test_expect_success "should rename neo4j.properties to neo4j.conf" "
  grep 'foo=bar' ./neo4j-home/conf/neo4j.conf
"

test_expect_success "should merge neo4j-server.properties into neo4j.conf" "
  test_expect_file_matching 'server-setting=server-value' ./neo4j-home/conf/neo4j.conf
"

test_done
