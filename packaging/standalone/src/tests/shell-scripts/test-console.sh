#!/usr/bin/env bash

test_description="Test running the console"

. ./lib/sharness.sh
fake_install

test_expect_success "should run okay" "
  run_console
"

test_expect_success "should run okay with env var" "
  set_config '1a' 'foo' neo4j.conf &&
  run_console
"

test_done
