#!/usr/bin/env bash

test_description="Test config parsing"

. ./lib/sharness.sh
fake_install

test_expect_success "should default port and address if none are provided" "
  test_expect_stdout_matching 'By default, it is available at http://localhost:7474/' run_daemon
"

#test_expect_success "should read port and address from config" "
#  set_config 'dbms.connector.0.type' 'HTTP' neo4j.conf &&
#  set_config 'dbms.connector.0.address' 'neo4j.example.com' neo4j.conf &&
#  set_config 'dbms.connector.0.port' '1234' neo4j.conf &&
#  test_expect_stdout_matching 'Started at http://neo4j.example.com:1234' run_daemon
#"

test_expect_success "should write a specific message in HA mode" "
  set_config 'dbms.mode' 'HA' neo4j.conf &&
  test_expect_stdout_matching 'This HA instance will be operational once it has joined the cluster' run_daemon
"

test_expect_success "should respect log directory configuration" "
  mkdir -p '$(neo4j_home)/other-log-dir' &&
  set_config 'dbms.directories.logs' 'other-log-dir' neo4j.conf &&
  run_daemon &&
  test_expect_file_matching 'stdout from java' '$(neo4j_home)/other-log-dir/neo4j.log'
"

test_expect_success "can configure log directory outside neo4j-root" "
  clear_config &&
  mkdir -p other-log-dir &&
  set_config 'dbms.directories.logs' '$(pwd)/other-log-dir' neo4j.conf &&
  run_daemon &&
  test_expect_file_matching 'stdout from java' '$(pwd)/other-log-dir/neo4j.log'
"

test_done
