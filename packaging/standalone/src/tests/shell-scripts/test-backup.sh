#!/usr/bin/env bash

test_description="Test running neo4j-backup"

. ./lib/sharness.sh
fake_install

test_expect_success "should invoke backup main class" "
  neo4j-home/bin/neo4j-backup || true &&
  test_expect_java_arg 'org.neo4j.backup.BackupTool'
"

test_expect_success "should pass -to <absolute-path> through unchanged" "
  neo4j-home/bin/neo4j-backup -to /tmp/test-absolute || true &&
  test_expect_java_arg '-to /tmp/test-absolute'
"

test_expect_success "should rewrite -to <relative-path> to be absolute" "
  neo4j-home/bin/neo4j-backup -to tmp/test-relative-file || true &&
  test_expect_java_arg '-to $(pwd)/tmp/test-relative-file'
"

test_expect_success "should pass --to <absolute-path> through unchanged" "
  neo4j-home/bin/neo4j-backup --to /tmp/test-absolute || true &&
  test_expect_java_arg '--to /tmp/test-absolute'
"

test_expect_success "should rewrite --to <relative-path> to be absolute" "
  neo4j-home/bin/neo4j-backup --to tmp/test-relative-file || true &&
  test_expect_java_arg '--to $(pwd)/tmp/test-relative-file'
"

test_expect_success "should pass -to=<absolute-path> through unchanged" "
  neo4j-home/bin/neo4j-backup -to=/tmp/test-absolute || true &&
  test_expect_java_arg '-to=/tmp/test-absolute'
"

test_expect_success "should rewrite -to=<relative-path> to be absolute" "
  neo4j-home/bin/neo4j-backup -to=tmp/test-relative-file || true &&
  test_expect_java_arg '-to=$(pwd)/tmp/test-relative-file'
"

test_expect_success "should pass --to=<absolute-path> through unchanged" "
  neo4j-home/bin/neo4j-backup --to=/tmp/test-absolute || true &&
  test_expect_java_arg '--to=/tmp/test-absolute'
"

test_expect_success "should rewrite --to=<relative-path> to be absolute" "
  neo4j-home/bin/neo4j-backup --to=tmp/test-relative-file || true &&
  test_expect_java_arg '--to=$(pwd)/tmp/test-relative-file'
"

test_done
