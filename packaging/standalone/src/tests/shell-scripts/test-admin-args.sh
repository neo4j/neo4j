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

test_expect_success "should specify heap size when given" "
  HEAP_SIZE=666m neo4j-home/bin/neo4j-admin backup &&
  test_expect_java_arg '-Xmx666m'
"

test_done
