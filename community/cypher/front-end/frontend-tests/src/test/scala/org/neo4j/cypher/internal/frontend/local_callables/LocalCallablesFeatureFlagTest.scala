/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.local_callables

class LocalCallablesFeatureFlagTest extends LocalCallablesSemanticAnalysisTest {

  test(
    """DEFINE PROCEDURE foo.bar() { FINISH }
      |^
      |
      |CALL foo.bar()
      |FINISH
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """DEFINE FUNCTION foo.bar() = 1
      |^
      |
      |RETURN foo.bar() AS x
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """DEFINE FUNCTION hej(n) = EXISTS { MATCH (n)-->(:B) }
      |^
      |
      |MATCH (a:A) WHERE hej(a)
      |RETURN *
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """DEFINE FUNCTION hej(n :: NODE) = EXISTS { MATCH (n)-->(:B) }
      |^
      |
      |MATCH (a:A) WHERE hej(a)
      |RETURN *
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """DEFINE FUNCTION hej() =
      |^
      |  EXISTS {
      |    DEFINE FUNCTION nested() = 1
      |    MATCH (:A)-->(:B {p: nested() })
      |  }
      |
      |MATCH (a:A) WHERE hej()
      |RETURN *
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """RETURN head(COLLECT {
      |  DEFINE FUNCTION foo() = "foo"
      |  ^
      |
      |  RETURN foo() AS x
      |}) AS foo
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """DEFINE PROCEDURE unused() {
      |^
      |  DEFINE PROCEDURE foo.bar(x :: INT) { FINISH }
      |  DEFINE FUNCTION foo.one() = 1
      |
      |  CALL foo.bar(foo.one())
      |  FINISH
      |}
      |
      |FINISH
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """DEFINE PROCEDURE foo.bar(x :: INT) { FINISH }
      |^
      |
      |{
      |  DEFINE FUNCTION foo.one() = 1
      |  CALL foo.bar(foo.one())
      |  FINISH
      |}
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """{
      |  DEFINE PROCEDURE foo.bar() { FINISH }
      |  ^
      |
      |  CALL foo.bar()
      |  FINISH
      |}
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """{
      |  DEFINE FUNCTION one() = 1
      |  ^
      |
      |  RETURN one() AS x
      |}
      |UNION
      |{
      |  RETURN 2 AS x
      |}
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """{
      |  RETURN 1 AS x
      |}
      |UNION
      |{
      |  DEFINE PROCEDURE two() { RETURN 2 AS foo }
      |  ^
      |
      |  CALL two() YIELD foo AS x
      |  RETURN x
      |}
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """{
      |  DEFINE FUNCTION one() = 1
      |  ^
      |
      |  RETURN one() AS x
      |}
      |
      |NEXT
      |
      |{
      |  RETURN 2 AS x
      |}
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """{
      |  RETURN 1 AS x
      |}
      |
      |NEXT
      |
      |{
      |  DEFINE PROCEDURE two() { RETURN 2 AS foo }
      |  ^
      |
      |  CALL two() YIELD foo AS y
      |  RETURN y
      |}
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """WHEN 1 = 1 THEN {
      |  DEFINE FUNCTION one() = 1
      |  ^
      |
      |  RETURN one() AS x
      |}
      |ELSE RETURN 2 AS x
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """WHEN 1 = 1 THEN RETURN 1 AS x
      |WHEN 1 = head(COLLECT {
      |  DEFINE FUNCTION two() = 2
      |  ^
      |
      |  RETURN two() AS x
      |}) THEN RETURN 2 AS x
      |ELSE RETURN 3 AS x
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """WHEN 1 = 1 THEN RETURN 1 AS x
      |WHEN 1 = 2 THEN {
      |  DEFINE FUNCTION two() = 2
      |  ^
      |
      |  RETURN two() AS x
      |}
      |ELSE RETURN 3 AS x
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """WHEN 1 = 1 THEN RETURN 1 AS x
      |WHEN 1 = 2 THEN RETURN 2 AS x
      |ELSE {
      |  DEFINE FUNCTION thr33() = 3
      |  ^
      |
      |  RETURN thr33() AS x
      |}
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """CALL {
      |  DEFINE FUNCTION foo.bar() = 1
      |  ^
      |
      |  RETURN foo.bar() AS error
      |}
      |
      |FINISH
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }

  test(
    """CALL () {
      |  DEFINE FUNCTION foo.bar() = 1
      |  ^
      |
      |  RETURN foo.bar() AS error
      |}
      |
      |FINISH
      |""".stripMargin
  ) {
    runWithoutLC().hasErrorsWithMarkedPosition(p => errorFeatureRequired(p))
  }
}
