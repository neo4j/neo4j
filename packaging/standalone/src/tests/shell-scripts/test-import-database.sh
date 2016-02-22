#!/usr/bin/env bash

test_description="Test neo4j-admin import --mode=database"

. ./lib/sharness.sh
fake_install

mkdir old-db
touch old-db/a-store-file
touch old-db/messages.log

test_expect_success "should run successfully" "
  neo4j-home/bin/neo4j-admin import --mode=database --database=foo.db --from=./old-db
"

test_expect_success "should move database dirs into data/databases" "
  [[ -e ./neo4j-home/data/databases/foo.db/a-store-file ]]
"

test_expect_success "should not import messages.log" "
  [[ ! -e ./neo4j-home/data/databases/foo.db/messages.log ]]
"

test_done
