#!/usr/bin/env bash

test_description="Test neo4j-admin core-convert"

. ./lib/sharness.sh
fake_install

test_expect_success "should invoke convert classic store main class, pass in database name and config directory" "
  neo4j-home/bin/neo4j-admin core-convert --database=foo.db  &&
  test_expect_java_arg '--home-dir=$(neo4j_home)' &&
  test_expect_java_arg 'org.neo4j.coreedge.convert.ConvertNonCoreEdgeStoreCli' &&
  test_expect_java_arg '--database=foo.db' &&
  test_expect_java_arg '--config=$(neo4j_home)/conf'
"

test_done
