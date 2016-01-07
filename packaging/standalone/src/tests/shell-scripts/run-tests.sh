#!/usr/bin/env bash
set -e

for test in test-*.sh; do
  "./${test}" "$@"
done
