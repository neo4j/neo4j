#!/bin/sh

test_description="Test Java arguments"

. ./lib/sharness.sh
fake_install

test_expect_success "should specify -server" "
  neo4j-home/bin/neo4j console &&
  test_expect_java_arg '-server'
"

test_expect_success "should disable explicit GC" "
  neo4j-home/bin/neo4j console &&
  test_expect_java_arg '-XX:+DisableExplicitGC'
"

test_expect_success "should add additional options" "
  set_config 'wrapper.java.additional' '-XX:+UseG1GC', neo4j-wrapper.conf &&
  neo4j-home/bin/neo4j console &&
  test_expect_java_arg '-XX:+UseG1GC'
"

test_expect_success "should set heap size constraints" "
  set_config 'wrapper.java.initmemory' '512' neo4j-wrapper.conf &&
  set_config 'wrapper.java.maxmemory' '1024' neo4j-wrapper.conf &&
  neo4j-home/bin/neo4j console &&
  test_expect_java_arg '-Xms512m' &&
  test_expect_java_arg '-Xmx1024m'
"

test_expect_success "should set neo4j.home" "
  neo4j-home/bin/neo4j console &&
  test_expect_java_arg '-Dneo4j.home=$(neo4j_home)'
"

test_expect_success "should invoke main class" "
  set_main_class some.main.class &&
  neo4j-home/bin/neo4j console &&
  test_expect_java_arg 'some.main.class'
"

test_expect_success "should set default charset to UTF-8" "
  neo4j-home/bin/neo4j console &&
  test_expect_java_arg '-Dfile.encoding=UTF-8'
"

test_done
