#!/usr/bin/env bash

test_description="Test neo4j-admin import --mode=database"

. ./lib/sharness.sh
fake_install

mkdir old-db
touch old-db/a-store-file
touch old-db/messages.log

test_expect_success "should run successfully" "
  clear_config &&
  neo4j-home/bin/neo4j-admin import --mode=database --database=foo.db --from=./old-db
"

test_expect_success "should move database dirs into data/databases" "
  [[ -e ./neo4j-home/data/databases/foo.db/a-store-file ]]
"

test_expect_success "should not import messages.log" "
  [[ ! -e ./neo4j-home/data/databases/foo.db/messages.log ]]
"

test_expect_success "should be able to configure a data directory outside neo4j-home" "
  mkdir -p some-other-data/databases
  set_config 'dbms.directories.data' '$(pwd)/some-other-data' neo4j.conf &&
  neo4j-home/bin/neo4j-admin import --mode=database --database=foo.db --from=./old-db &&
  [[ -e '$(pwd)/some-other-data/databases/foo.db/a-store-file' ]]
"

test_done
