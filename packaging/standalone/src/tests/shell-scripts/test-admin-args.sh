#!/usr/bin/env bash

test_description="Test neo4j-admin argument handling"

. ./lib/sharness.sh
fake_install

test_expect_success "should delegate help to the Java tool" "
  neo4j-home/bin/neo4j-admin help &&
  test_expect_java_arg 'org.neo4j.commandline.admin.AdminTool'
"

test_expect_success "should delegate missing commands to the Java tool" "
  neo4j-home/bin/neo4j-admin &&
  test_expect_java_arg 'org.neo4j.commandline.admin.AdminTool'
"

test_expect_success "should delegate unknown commands to the Java tool" "
  neo4j-home/bin/neo4j-admin unknown &&
  test_expect_java_arg 'org.neo4j.commandline.admin.AdminTool'
"

test_expect_success "should delegate error reporting to the Java tool" "
  neo4j-home/bin/neo4j-admin import --from=/foo --mode=database &&
  test_expect_java_arg 'you must provide the --database option with an argument' &&

  neo4j-home/bin/neo4j-admin import --database=foo --mode=database &&
  test_expect_java_arg 'you must provide the --from option with an argument' &&

  neo4j-home/bin/neo4j-admin import --database=foo --from=/foo &&
  test_expect_java_arg 'you must provide the --mode option with an argument'
"

test_done
