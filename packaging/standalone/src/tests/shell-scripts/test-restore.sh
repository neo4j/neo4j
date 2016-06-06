#!/usr/bin/env bash

test_description="Test neo4j-admin restore"

. ./lib/sharness.sh
fake_install

test_expect_success "should invoke restore main class, pass in database name, config directory and from directory" "
  neo4j-home/bin/neo4j-admin restore --from=backup.db --database=foo.db  &&
  test_expect_java_arg 'org.neo4j.restore.RestoreDatabaseCli' &&
  test_expect_java_arg '--home-dir=$(neo4j_home)' &&
  test_expect_java_arg '--database=foo.db' &&
  test_expect_java_arg '--from=backup.db' &&
  test_expect_java_arg '--config=$(neo4j_home)/conf'
"

test_done
