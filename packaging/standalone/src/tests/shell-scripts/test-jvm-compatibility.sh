#!/bin/sh

test_description="Test JVM compatibility checking"

. ./lib/sharness.sh
fake_install

test_expect_success "should run happily with Java 8" "
  FAKE_JAVA_VERSION='1.8.0_51' neo4j-home/bin/neo4j console
"

test_expect_success "should refuse to run with Java 7" "
  FAKE_JAVA_VERSION='1.7.0_b76' test_expect_code 1 neo4j-home/bin/neo4j console
"

test_expect_success "should refuse to run with Java 6" "
  FAKE_JAVA_VERSION='1.6.0_b21' test_expect_code 1 neo4j-home/bin/neo4j console
"

test_expect_success "should run happily with Oracle JVM" "
  ! (FAKE_JAVA_JVM='Java HotSpot(TM)' neo4j-home/bin/neo4j console | grep 'unsupported Java runtime')
"

test_expect_success "should run happily with OpenJDK" "
  ! (FAKE_JAVA_JVM='OpenJDK' neo4j-home/bin/neo4j console | grep 'unsupported Java runtime')
"

test_expect_success "should warn when run with other JDKs" "
  FAKE_JAVA_JVM='Some Other JDK' neo4j-home/bin/neo4j console | grep 'unsupported Java runtime'
"

test_done
