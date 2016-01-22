#!/usr/bin/env bash

test_description="Test differences running the arbiter"

. ./lib/sharness.sh
fake_install
export FAKE_JAVA_SLEEP=1

set_config 'org.neo4j.server.database.mode' 'ARBITER' neo4j-server.properties

test_expect_success "should start" "
  neo4j-home/bin/neo4j start >neo4j.stdout
"

test_expect_success "should redirect output to arbiter-console.log" "
  grep 'stdout from java' neo4j-home/data/log/arbiter-console.log &&
  grep 'stderr from java' neo4j-home/data/log/arbiter-console.log
"

test_expect_success "should invoke arbiter main class" "
  test_expect_java_arg 'org.neo4j.server.enterprise.StandaloneClusterClient'
"

test_expect_success "should print a specific startup message" "
  grep 'This instance is now joining the cluster.' neo4j.stdout
"

test_done
