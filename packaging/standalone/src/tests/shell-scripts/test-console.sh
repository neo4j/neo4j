#!/usr/bin/env bash

test_description="Test running the console"

. ./lib/sharness.sh
fake_install

test_expect_success "should run okay" "
  run_console
"

test_done
