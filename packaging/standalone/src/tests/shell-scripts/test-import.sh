#!/usr/bin/env bash

test_description="Test running neo4j-import"

. ./lib/sharness.sh
fake_install

test_expect_success "should invoke import main class" "
  neo4j-home/bin/neo4j-import || true &&
  test_expect_java_arg 'org.neo4j.tooling.ImportTool'
"

for path_arg in into db-config; do
  test_expect_success "should pass --${path_arg} <absolute-path> through unchanged" "
    neo4j-home/bin/neo4j-import --${path_arg} /tmp/test-absolute-file || true &&
    test_expect_java_arg '--${path_arg} /tmp/test-absolute-file'
  "

  test_expect_success "should rewrite --${path_arg} <relative-path> to be absolute" "
    neo4j-home/bin/neo4j-import --${path_arg} tmp/test-relative-file || true &&
    test_expect_java_arg '--${path_arg} $(pwd)/tmp/test-relative-file'
  "

  test_expect_success "should pass -${path_arg} <absolute-path> through unchanged" "
    neo4j-home/bin/neo4j-import -${path_arg} /tmp/test-absolute-file || true &&
    test_expect_java_arg '-${path_arg} /tmp/test-absolute-file'
  "

  test_expect_success "should rewrite -${path_arg} <relative-path> to be absolute" "
    neo4j-home/bin/neo4j-import -${path_arg} tmp/test-relative-file || true &&
    test_expect_java_arg '-${path_arg} $(pwd)/tmp/test-relative-file'
  "

  test_expect_success "should pass --${path_arg}=<absolute-path> through unchanged" "
    neo4j-home/bin/neo4j-import --${path_arg}=/tmp/test-absolute-file || true &&
    test_expect_java_arg '--${path_arg}=/tmp/test-absolute-file'
  "

  test_expect_success "should rewrite --${path_arg}=<relative-path> to be absolute" "
    neo4j-home/bin/neo4j-import --${path_arg}=tmp/test-relative-file || true &&
    test_expect_java_arg '--${path_arg}=$(pwd)/tmp/test-relative-file'
  "

  test_expect_success "should pass -${path_arg}=<absolute-path> through unchanged" "
    neo4j-home/bin/neo4j-import -${path_arg}=/tmp/test-absolute-file || true &&
    test_expect_java_arg '-${path_arg}=/tmp/test-absolute-file'
  "

  test_expect_success "should rewrite -${path_arg}=<relative-path> to be absolute" "
    neo4j-home/bin/neo4j-import -${path_arg}=tmp/test-relative-file || true &&
    test_expect_java_arg '-${path_arg}=$(pwd)/tmp/test-relative-file'
  "
done

test_done
