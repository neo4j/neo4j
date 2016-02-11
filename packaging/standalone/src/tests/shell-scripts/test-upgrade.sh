#!/usr/bin/env bash

test_description="Test neo4j-admin upgrade"

. ./lib/sharness.sh
fake_install

fake_install ./old-neo4j-home "2.3.0"

set_config "foo" "bar" neo4j.properties ./old-neo4j-home
set_config "server-setting" "server-value" neo4j-server.properties ./old-neo4j-home

mkdir -p ./old-neo4j-home/data/graph.db
touch ./old-neo4j-home/data/graph.db/a-store-file
mkdir -p ./old-neo4j-home/data/some-other-db
touch ./old-neo4j-home/data/some-other-db/a-store-file

mkdir -p ./old-neo4j-home/data/log
touch ./old-neo4j-home/data/log/some-log-file

mkdir -p ./old-neo4j-home/data/dbms
touch ./old-neo4j-home/data/dbms/auth

test_expect_success "should fail if source is missing" "
  test_expect_stderr_matching 'you must provide the --from option with an argument' \
    test_expect_code 1 neo4j-home/bin/neo4j-admin upgrade
"

test_expect_success "should run successfully" "
  neo4j-home/bin/neo4j-admin upgrade --from=./old-neo4j-home
"

test_expect_success "should rename neo4j.properties to neo4j.conf" "
  test_expect_file_matching 'foo=bar' ./neo4j-home/conf/neo4j.conf
"

test_expect_success "should merge neo4j-server.properties into neo4j.conf" "
  test_expect_file_matching 'server-setting=server-value' ./neo4j-home/conf/neo4j.conf
"

test_expect_success "should move database dirs into data/databases" "
  [[ -e ./neo4j-home/data/databases/graph.db/a-store-file ]] &&
  [[ -e ./neo4j-home/data/databases/some-other-db/a-store-file ]]
"

test_expect_success "should not treat the log directory as a database" "
  [[ ! -e ./neo4j-home/data/databases/log/some-log-file ]]
"

test_expect_success "should not treat the dbms directory as a database" "
  [[ ! -e ./neo4j-home/data/databases/dbms/auth ]]
"

test_done
