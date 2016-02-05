#!/usr/bin/env bash

test_description="Test config parsing"

. ./lib/sharness.sh
fake_install

test_expect_success "should default port and address if none are provided" "
  test_expect_stdout_matching 'Started at http://localhost:7474' run_daemon
"

test_expect_success "should read port and address from config" "
  set_config 'org.neo4j.server.webserver.port' '1234' neo4j.properties &&
  set_config 'org.neo4j.server.webserver.address' 'neo4j.example.com' neo4j.properties &&
  test_expect_stdout_matching 'Started at http://neo4j.example.com:1234' run_daemon
"

test_expect_success "should write a specific message in HA mode" "
  set_config 'org.neo4j.server.database.mode' 'HA' neo4j.properties &&
  test_expect_stdout_matching 'This HA instance will be operational once it has joined the cluster' run_daemon
"

test_done
