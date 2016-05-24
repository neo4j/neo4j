#!/usr/bin/env bash

test_description="Test shell argument handling."

. ./lib/sharness.sh
fake_install

test_expect_success "should invoke shell main class" "
  neo4j-home/bin/neo4j-shell || true &&
  test_expect_java_arg 'org.neo4j.shell.StartClient'
"

for path_arg in file path conf; do
  test_expect_success "should pass --${path_arg} <absolute-path> through unchanged" "
    neo4j-home/bin/neo4j-shell --${path_arg} /tmp/test-absolute-file || true &&
    test_expect_java_arg '--${path_arg} /tmp/test-absolute-file'
  "

  test_expect_success "should rewrite --${path_arg} <relative-path> to be absolute" "
    neo4j-home/bin/neo4j-shell --${path_arg} tmp/test-relative-file || true &&
    test_expect_java_arg '--${path_arg} $(pwd)/tmp/test-relative-file'
  "

  test_expect_success "should pass -${path_arg} <absolute-path> through unchanged" "
    neo4j-home/bin/neo4j-shell -${path_arg} /tmp/test-absolute-file || true &&
    test_expect_java_arg '-${path_arg} /tmp/test-absolute-file'
  "

  test_expect_success "should rewrite -${path_arg} <relative-path> to be absolute" "
    neo4j-home/bin/neo4j-shell -${path_arg} tmp/test-relative-file || true &&
    test_expect_java_arg '-${path_arg} $(pwd)/tmp/test-relative-file'
  "

  test_expect_success "should pass --${path_arg}=<absolute-path> through unchanged" "
    neo4j-home/bin/neo4j-shell --${path_arg}=/tmp/test-absolute-file || true &&
    test_expect_java_arg '--${path_arg} /tmp/test-absolute-file'
  "

  test_expect_success "should rewrite --${path_arg}=<relative-path> to be absolute" "
    neo4j-home/bin/neo4j-shell --${path_arg}=tmp/test-relative-file || true &&
    test_expect_java_arg '--${path_arg} $(pwd)/tmp/test-relative-file'
  "

  test_expect_success "should pass -${path_arg}=<absolute-path> through unchanged" "
    neo4j-home/bin/neo4j-shell -${path_arg}=/tmp/test-absolute-file || true &&
    test_expect_java_arg '-${path_arg} /tmp/test-absolute-file'
  "

  test_expect_success "should rewrite -${path_arg}=<relative-path> to be absolute" "
    neo4j-home/bin/neo4j-shell -${path_arg}=tmp/test-relative-file || true &&
    test_expect_java_arg '-${path_arg} $(pwd)/tmp/test-relative-file'
  "
done

test_expect_success "should rewrite all relative paths appropriately" "
  neo4j-home/bin/neo4j-shell --file tmp/test-relative-file --path a/test/path || true &&
  test_expect_java_arg '--file $(pwd)/tmp/test-relative-file' &&
  test_expect_java_arg '--path $(pwd)/a/test/path'
"

test_expect_success "should rewrite relative paths and leave absolute paths unchanged" "
  neo4j-home/bin/neo4j-shell --file /tmp/absolute-file --path a/relative/path --conf /an/absolute/conf || true &&
  test_expect_java_arg '--file /tmp/absolute-file' &&
  test_expect_java_arg '--path $(pwd)/a/relative/path' &&
  test_expect_java_arg '--conf /an/absolute/conf'
"

test_expect_success "should not resolve '-' for --file -" "
  neo4j-home/bin/neo4j-shell --file - --path - --conf - || true &&
  test_expect_java_arg '--file -' &&
  test_expect_java_arg '--path $(pwd)/-' &&
  test_expect_java_arg '--conf $(pwd)/-'
"

test_expect_success "should not resolve '-' for --file=-" "
  neo4j-home/bin/neo4j-shell --file=- --path=- --conf=- || true &&
  test_expect_java_arg '--file -' &&
  test_expect_java_arg '--path $(pwd)/-' &&
  test_expect_java_arg '--conf $(pwd)/-'
"

test_expect_success "should rewrite paths as needed and leave other options unchanged" "
  neo4j-home/bin/neo4j-shell -file /tmp/absolute-file -path=a/relative/path -host foobar -port 1234 -readonly || true &&
  test_expect_java_arg '-file /tmp/absolute-file' &&
  test_expect_java_arg '-path $(pwd)/a/relative/path' &&
  test_expect_java_arg '-host foobar' &&
  test_expect_java_arg '-port 1234' &&
  test_expect_java_arg '-readonly'
"

test_done
