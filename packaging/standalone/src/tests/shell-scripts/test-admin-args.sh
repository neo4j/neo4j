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

test_expect_success "should let higher heap size env var override memory setting when both given" "
  clear_config &&
  set_config 'dbms.memory.heap.initial_size' '111m' neo4j.conf &&
  set_config 'dbms.memory.heap.max_size' '222m' neo4j.conf &&
  HEAP_SIZE=666m neo4j-home/bin/neo4j-admin backup &&
  test_expect_java_arg '-Xmx666m'
  test_expect_java_arg '-Xms666m'
"

test_expect_success "should let lower heap size env var override memory setting when both given" "
  clear_config &&
  set_config 'dbms.memory.heap.initial_size' '666m' neo4j.conf &&
  set_config 'dbms.memory.heap.max_size' '777m' neo4j.conf &&
  HEAP_SIZE=222m neo4j-home/bin/neo4j-admin backup &&
  test_expect_java_arg '-Xmx222m'
  test_expect_java_arg '-Xms222m'
"

test_expect_success "should pass parallel collector option" "
  neo4j-home/bin/neo4j-admin &&
  test_expect_java_arg '-XX:+UseParallelGC'
"

test_done
