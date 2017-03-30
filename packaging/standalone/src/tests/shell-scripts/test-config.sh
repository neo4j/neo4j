#!/usr/bin/env bash

test_description="Test config parsing"

. ./lib/sharness.sh
fake_install

test_expect_success "should default port and address if none are provided" "
  clear_config &&
  test_expect_stdout_matching 'It is available at http://localhost:7474/' run_daemon
"

test_expect_success "http: should read port and address from config" "
  clear_config &&
  set_config 'dbms.connector.http.address' '1.2.3.4:1234' neo4j.conf &&
  set_config 'dbms.connector.http.enabled' 'true' neo4j.conf &&
  test_expect_stdout_matching 'It is available at http://1.2.3.4:1234/' run_daemon
"

test_expect_success "http: should read only port from config" "
  clear_config &&
  set_config 'dbms.connector.http.address' ':1234' neo4j.conf &&
  set_config 'dbms.connector.http.enabled' 'true' neo4j.conf &&
  test_expect_stdout_matching 'It is available at http://localhost:1234/' run_daemon
"

test_expect_success "http: should fallback to default listening address if defined" "
  clear_config &&
  set_config 'dbms.connectors.default_listen_address' '100.200.300.400' neo4j.conf &&
  set_config 'dbms.connector.http.enabled' 'true' neo4j.conf &&
  test_expect_stdout_matching 'It is available at http://100.200.300.400:7474/' run_daemon
"

test_expect_success "http: should read port and address from config and prioritize non-deprecated" "
  clear_config &&
  set_config 'dbms.connector.http.address' '1.2.3.4:1234' neo4j.conf &&
  set_config 'dbms.connector.http.listen_address' 'a.b.c.d:333' neo4j.conf &&
  set_config 'dbms.connector.http.enabled' 'true' neo4j.conf &&
  test_expect_stdout_matching 'It is available at http://a.b.c.d:333/' run_daemon
"

test_expect_success "https: should display default https if http disabled and no other options" "
  clear_config &&
  set_config 'dbms.connector.http.enabled' 'false' neo4j.conf &&
  test_expect_stdout_matching 'It is available at https://localhost:7473/' run_daemon
"

test_expect_success "https: should read port and address from config" "
  clear_config &&
  set_config 'dbms.connector.http.enabled' 'false' neo4j.conf &&
  set_config 'dbms.connector.https.address' '1.2.3.4:1234' neo4j.conf &&
  set_config 'dbms.connector.https.enabled' 'true' neo4j.conf &&
  test_expect_stdout_matching 'It is available at https://1.2.3.4:1234/' run_daemon
"

test_expect_success "https: should read only port from config" "
  clear_config &&
  set_config 'dbms.connector.http.enabled' 'false' neo4j.conf &&
  set_config 'dbms.connector.https.address' ':1234' neo4j.conf &&
  set_config 'dbms.connector.https.enabled' 'true' neo4j.conf &&
  test_expect_stdout_matching 'It is available at https://localhost:1234/' run_daemon
"

test_expect_success "https: should fallback to default listening address if defined" "
  clear_config &&
  set_config 'dbms.connector.http.enabled' 'false' neo4j.conf &&
  set_config 'dbms.connectors.default_listen_address' '100.200.300.400' neo4j.conf &&
  set_config 'dbms.connector.https.enabled' 'true' neo4j.conf &&
  test_expect_stdout_matching 'It is available at https://100.200.300.400:7473/' run_daemon
"

test_expect_success "https: should read port and address from config and prioritize non-deprecated" "
  clear_config &&
  set_config 'dbms.connector.http.enabled' 'false' neo4j.conf &&
  set_config 'dbms.connector.https.address' '1.2.3.4:1234' neo4j.conf &&
  set_config 'dbms.connector.https.listen_address' 'a.b.c.d:333' neo4j.conf &&
  set_config 'dbms.connector.https.enabled' 'true' neo4j.conf &&
  test_expect_stdout_matching 'It is available at https://a.b.c.d:333/' run_daemon
"

test_expect_success "should write a specific message in HA mode" "
  clear_config &&
  set_config 'dbms.mode' 'HA' neo4j.conf &&
  test_expect_stdout_matching 'This HA instance will be operational once it has joined the cluster' run_daemon
"

test_expect_success "should respect log directory configuration" "
  clear_config &&
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

test_expect_success "should write paths in use" "
  clear_config &&
  mkdir '$(neo4j_home)/loogs' &&
  mkdir '$(neo4j_home)/ruun' &&
  set_config 'dbms.directories.data' '/data/bob' neo4j.conf &&
  set_config 'dbms.directories.import' '/import/bob' neo4j.conf &&
  set_config 'dbms.directories.plugins' '/plugins/bob' neo4j.conf &&
  set_config 'dbms.directories.certificates' '/certs/bob' neo4j.conf &&
  set_config 'dbms.directories.run' 'ruun' neo4j.conf &&
  set_config 'dbms.directories.logs' 'loogs' neo4j.conf &&
  test_expect_stdout_matching '  home:         $(neo4j_home)' run_daemon &&
  test_expect_stdout_matching '  config:       $(neo4j_home)/conf' run_daemon &&
  test_expect_stdout_matching '  data:         /data/bob' run_daemon &&
  test_expect_stdout_matching '  import:       /import/bob' run_daemon &&
  test_expect_stdout_matching '  plugins:      /plugins/bob' run_daemon &&
  test_expect_stdout_matching '  certificates: /certs/bob' run_daemon &&
  test_expect_stdout_matching '  logs:         $(neo4j_home)/loogs' run_daemon &&
  test_expect_stdout_matching '  run:          $(neo4j_home)/ruun' run_daemon
"

test_expect_success "should write active database" "
  clear_config &&
  set_config 'dbms.active_database' 'bigdata' neo4j.conf &&
  test_expect_stdout_matching 'Active database: bigdata' run_daemon
"

test_done
