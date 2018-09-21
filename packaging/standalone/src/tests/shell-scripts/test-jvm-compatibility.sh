#!/usr/bin/env bash

test_description="Test JVM compatibility checking"

. ./lib/sharness.sh
fake_install

test_expect_success "should run happily with Java 11" "
  FAKE_JAVA_VERSION='11' test_expect_stdout_matching 'unsupported Java runtime' run_console
"

test_expect_success "should run happily with Java 10" "
  FAKE_JAVA_VERSION='10' test_expect_stdout_matching 'unsupported Java runtime' run_console
"

test_expect_success "should run happily with Java 9" "
  FAKE_JAVA_VERSION='9' test_expect_stdout_matching 'unsupported Java runtime' run_console
"

test_expect_success "should run happily with Java 8" "
  FAKE_JAVA_VERSION='1.8.0_51' run_console
"

test_expect_success "should refuse to run with Java 7" "
  FAKE_JAVA_VERSION='1.7.0_b76' test_expect_code 1 run_console
"

test_expect_success "should refuse to run with Java 6" "
  FAKE_JAVA_VERSION='1.6.0_b21' test_expect_code 1 run_console
"

test_expect_success "should run happily with Oracle JVM" "
  ! (FAKE_JAVA_JVM='Java HotSpot(TM)' run_console | grep 'unsupported Java runtime')
"
test_expect_success "should run happily with IBM JVM" "
  ! (FAKE_JAVA_JVM='IBM J9 VM' run_console | grep 'unsupported Java runtime')
"

test_expect_success "should run happily with OpenJDK" "
  ! (FAKE_JAVA_JVM='OpenJDK' run_console | grep 'unsupported Java runtime')
"

test_expect_success "should warn when run with other JDKs" "
  FAKE_JAVA_JVM='Some Other JDK' run_console | grep 'unsupported Java runtime'
"

test_done
