#!/bin/sh

test_description="Test running the console"

. ./lib/sharness.sh

. ../harness/fixture

test_expect_success "should run okay" "
  neo4j-home/bin/neo4j console
"

test_done
