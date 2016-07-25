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

  test_expect_success "should add additional options from wrapper conf" "
    set_config 'dbms.jvm.additional' '-XX:+UseG1GC', neo4j-wrapper.conf &&
    ${run_command} &&
    test_expect_java_arg '-XX:+UseG1GC'
  "

  test_expect_success "should add additional options" "
    set_config 'dbms.jvm.additional' '-XX:+UseG1GC', neo4j.conf &&
    ${run_command} &&
    test_expect_java_arg '-XX:+UseG1GC'
  "

  test_expect_success "should set heap size constraints from wrapper conf" "
    clear_config &&
    set_config 'dbms.memory.heap.initial_size' '512' neo4j-wrapper.conf &&
    set_config 'dbms.memory.heap.max_size' '1024' neo4j-wrapper.conf &&
    ${run_command} &&
    test_expect_java_arg '-Xms512m' &&
    test_expect_java_arg '-Xmx1024m'
  "

  test_expect_success "should set heap size constraints" "
    clear_config &&
    set_config 'dbms.memory.heap.initial_size' '512' neo4j.conf &&
    set_config 'dbms.memory.heap.max_size' '1024' neo4j.conf &&
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

  test_expect_success "should construct the classpath and include plugins and conf dirs so that plugins can load config files from the classpath and developers can override plugin classes" "
    ${run_command} &&
    test_expect_java_arg '-cp $(neo4j_home)/plugins:$(neo4j_home)/conf:$(neo4j_home)/lib/*:$(neo4j_home)/plugins/*'
  "

  test_expect_success "classpath elements should be configurable" "
    clear_config &&
    set_config 'dbms.directories.lib' 'some-other-lib' neo4j.conf &&
    set_config 'dbms.directories.plugins' 'some-other-plugins' neo4j.conf &&
    ${run_command} &&
    test_expect_java_arg '-cp $(neo4j_home)/some-other-plugins:$(neo4j_home)/conf:$(neo4j_home)/some-other-lib/*:$(neo4j_home)/some-other-plugins/*'
  "

  test_expect_success "should set gc log location when gc log is enabled" "
    clear_config &&
    set_config 'dbms.logs.gc.enabled' 'true' neo4j.conf &&
    ${run_command} &&
    test_expect_java_arg '-Xloggc:$(neo4j_home)/logs/gc.log'
  "

  test_expect_success "should put gc log into configured logs directory" "
    mkdir -p '$(neo4j_home)/some-other-logs' &&
    clear_config &&
    set_config 'dbms.logs.gc.enabled' 'true' neo4j.conf &&
    set_config 'dbms.directories.logs' 'some-other-logs' neo4j.conf &&
    ${run_command} &&
    test_expect_java_arg '-Xloggc:$(neo4j_home)/some-other-logs/gc.log'
  "

  test_expect_success "should set gc logging options when gc log is enabled" "
    clear_config &&
    set_config 'dbms.logs.gc.enabled' 'true' neo4j.conf &&
    set_config 'dbms.logs.gc.options' '-XX:+PrintSomeOtherGCOption' neo4j.conf &&
    ${run_command} &&
    test_expect_java_arg '-XX:+PrintSomeOtherGCOption'
  "

  test_expect_success "should set default gc logging options when none are provided" "
    clear_config &&
    set_config 'dbms.logs.gc.enabled' 'true' neo4j.conf &&
    ${run_command} &&
    test_expect_java_arg '-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+PrintPromotionFailure -XX:+PrintTenuringDistribution'
  "

  test_expect_success "should set gc logging rotation options" "
    clear_config &&
    set_config 'dbms.logs.gc.rotation.size' '10m' neo4j.conf &&
    set_config 'dbms.logs.gc.rotation.keep_number' '8' neo4j.conf &&
    set_config 'dbms.logs.gc.enabled' 'true' neo4j.conf

    ${run_command} &&
    test_expect_java_arg '-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=8 -XX:GCLogFileSize=10m'
  "

  test_expect_success "should pass config dir location" "
    ${run_command} &&
    test_expect_java_arg '--config-dir=$(neo4j_home)/conf'
  "

  test_expect_success "should be able to override config dir location" "
    NEO4J_CONF=/some/other/conf/dir ${run_command} &&
    test_expect_java_arg '--config-dir=/some/other/conf/dir'
  "
done

test_expect_success "should set heap size constraints when checking version from wrapper conf" "
  clear_config &&
  set_config 'dbms.memory.heap.initial_size' '512' neo4j-wrapper.conf &&
  set_config 'dbms.memory.heap.max_size' '1024' neo4j-wrapper.conf &&
  neo4j-home/bin/neo4j status || true &&
  test_expect_java_arg '-Xms512m' &&
  test_expect_java_arg '-Xmx1024m'
"

test_expect_success "should set heap size constraints when checking version" "
  clear_config &&
  set_config 'dbms.memory.heap.initial_size' '512' neo4j.conf &&
  set_config 'dbms.memory.heap.max_size' '1024' neo4j.conf &&
  neo4j-home/bin/neo4j status || true &&
  test_expect_java_arg '-Xms512m' &&
  test_expect_java_arg '-Xmx1024m'
"

test_done
