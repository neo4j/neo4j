#!/usr/bin/env bash

test_description="Test Java arguments"

. ./lib/sharness.sh
fake_install

for run_command in run_console run_daemon; do
  clear_config

  test_expect_success "should specify -server" "
    ${run_command} &&
    test_expect_java_arg '-server'
  "

  test_expect_success "should add additional options" "
    set_config 'dbms.jvm.additional' '-XX:+UseG1GC', neo4j-wrapper.conf &&
    ${run_command} &&
    test_expect_java_arg '-XX:+UseG1GC'
  "

  test_expect_success "should set heap size constraints" "
    clear_config &&
    set_config 'dbms.memory.heap.initial_size' '512' neo4j-wrapper.conf &&
    set_config 'dbms.memory.heap.max_size' '1024' neo4j-wrapper.conf &&
    ${run_command} &&
    test_expect_java_arg '-Xms512m' &&
    test_expect_java_arg '-Xmx1024m'
  "

  test_expect_success "should invoke main class" "
    set_main_class some.main.class &&
    ${run_command} &&
    test_expect_java_arg 'some.main.class'
  "

  test_expect_success "should set default charset to UTF-8" "
    ${run_command} &&
    test_expect_java_arg '-Dfile.encoding=UTF-8'
  "

  test_expect_success "should add lib dirs to classpath" "
    ${run_command} &&
    test_expect_java_arg '-cp $(neo4j_home)/lib/*'
  "

  test_expect_success "should add plugins to classpath" "
    ${run_command} &&
    test_expect_java_arg ':$(neo4j_home)/plugins/*'
  "

  test_expect_success "should set gc logging options when gc log is enabled" "
    clear_config &&
    set_config 'dbms.logs.gc.enabled' 'true' neo4j.conf &&
    set_config 'dbms.logs.gc.options' '-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution' neo4j.conf &&
    ${run_command} &&
    test_expect_java_arg '-Xloggc:$(neo4j_home)/logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution'
  "

  test_expect_success "should set default gc logging options when none are provided" "
    clear_config &&
    set_config 'dbms.logs.gc.enabled' 'true' neo4j.conf &&
    ${run_command} &&
    test_expect_java_arg '-Xloggc:$(neo4j_home)/logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=5 -XX:GCLogFileSize=20m'
  "

  test_expect_success "should set gc logging rotation options" "
    clear_config &&
    set_config 'dbms.logs.gc.rotation.size' '10m' neo4j.conf &&
    set_config 'dbms.logs.gc.rotation.keep_number' '8' neo4j.conf &&
    set_config 'dbms.logs.gc.enabled' 'true' neo4j.conf &&
    set_config 'dbms.logs.gc.options' '-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution -XX:+UseGCLogFileRotation' neo4j.conf &&

    ${run_command} &&
    test_expect_java_arg '-Xloggc:$(neo4j_home)/logs/gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution'
    test_expect_java_arg '-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=8 -XX:GCLogFileSize=10m'
  "
done

test_expect_success "should set heap size constraints when checking version" "
  clear_config &&
  set_config 'dbms.memory.heap.initial_size' '512' neo4j-wrapper.conf &&
  set_config 'dbms.memory.heap.max_size' '1024' neo4j-wrapper.conf &&
  neo4j-home/bin/neo4j status || true &&
  test_expect_java_arg '-Xms512m' &&
  test_expect_java_arg '-Xmx1024m'
"

test_done
