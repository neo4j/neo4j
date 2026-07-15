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

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.gqlstatus.GqlHelper

import scala.jdk.CollectionConverters.SeqHasAsJava

class LocalFunctionsSemanticAnalysisTest extends LocalCallablesSemanticAnalysisTest {

  test(
    """DEFINE FUNCTION foo.baz() = 1
      |
      |RETURN foo.baz()
      |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    """DEFINE FUNCTION foo.baz() {
      |  RETURN 1 LIMIT 1
      |}
      |
      |RETURN foo.baz()
      |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    """DEFINE FUNCTION foo.baz() {
      |  RETURN COUNT(1)
      |}
      |
      |RETURN foo.baz()
      |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    """DEFINE FUNCTION foo.baz() = 1
      |
      |LET x = foo.baz()
      |FINISH
      |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    """DEFINE FUNCTION foo.baz() = 1
      |
      |CREATE (:L {x: foo.baz()})
      |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    """DEFINE FUNCTION foo.baz() = 1
      |
      |MATCH (l:L) WHERE l.x = foo.baz()
      |RETURN l
      |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    """{
      |  DEFINE FUNCTION foo.baz() = 1
      |
      |  RETURN foo.baz() AS x
      |}""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a(x :: INT) {
       |  RETURN x AS z LIMIT 1
       |}
       |
       |RETURN foo.a(1)
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a(x, y) = x + y
       |
       |RETURN foo.a(1, 2)
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a(x :: INT, y :: INT) = x + y
       |
       |RETURN foo.a(1, 2)
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a(x) = 1
       |DEFINE FUNCTION foo.b(x) = 2
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.baz(x) = head(COLLECT {
       |  WHEN x = 1 THEN RETURN 1 AS y LIMIT 1
       |  WHEN x = 2 THEN {
       |    DEFINE FUNCTION foo.bar() {
       |      RETURN 1 AS z
       |
       |      NEXT
       |
       |      RETURN 2 AS z LIMIT 1
       |    }
       |
       |    RETURN 2 AS y LIMIT 1
       |  }
       |  ELSE RETURN 3 AS y LIMIT 1
       |})
       |
       |CALL foo.baz(1) YIELD y
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  /*
   * USE clause is not supported
   */
  {
    val msg = s"USE clause is not supported in local function definitions"
    val gqlError = GqlHelper.getGql42001_42NAG
    test(
      s"""DEFINE FUNCTION foo.baz() {
         |  USE graph
         |  ^
         |  RETURN 1 LIMIT 1
         |}
         |
         |LET _res = foo.baz()
         |FINISH
         |""".stripMargin
    ) {
      runWithLC().hasAtLeastErrorWithMarkedPosition(_ => gqlError, msg)
    }

    test(
      s"""DEFINE FUNCTION foo.baz() {
         |  DEFINE FUNCTION foo.bar() {
         |    USE graph
         |    ^
         |    RETURN 1 LIMIT 1
         |  }
         |
         |  RETURN foo.bar() LIMIT 1
         |}
         |
         |LET _res = foo.baz()
         |FINISH
         |""".stripMargin
    ) {
      runWithLC().hasAtLeastErrorWithMarkedPosition(_ => gqlError, msg)
    }

    /*
    // This test is also an invalid query, but it runs into a different error on embedded testing
    test(s"""DEFINE FUNCTION foo.baz() {
            |  CALL () {
            |    USE graph
            |    ^
            |    RETURN 1
            |  }
            |
            |  RETURN 1 LIMIT 1
            |}
            |
            |LET _res = foo.baz()
            |FINISH
            |""".stripMargin) {
      runWithLC().hasErrorWithMarkedPosition(_ => gqlError, msg)
    }
     */
    test(
      s"""DEFINE FUNCTION foo.baz() = head(COLLECT {
         |  RETURN 1 LIMIT 1
         |  UNION
         |  USE graph
         |  ^
         |  RETURN 1 LIMIT 1
         |})
         |
         |LET _res = foo.baz()
         |FINISH
         |""".stripMargin
    ) {
      runWithLC(SemanticFeature.UseAsSingleGraphSelector).hasErrorWithMarkedPosition(_ => gqlError, msg)
    }

    test(
      s"""DEFINE FUNCTION foo.baz() {
         |  DEFINE FUNCTION foo.bar() {
         |    RETURN 1 AS x
         |
         |    NEXT
         |
         |    USE graph
         |    ^
         |    RETURN 1 LIMIT 1
         |  }
         |
         |  RETURN 1 LIMIT 1
         |}
         |
         |LET _res = foo.baz()
         |FINISH
         |""".stripMargin
    ) {
      runWithLC().hasAtLeastErrorWithMarkedPosition(_ => gqlError, msg)
    }

    test(
      s"""DEFINE FUNCTION foo.baz(x) = head(COLLECT {
         |  WHEN x = 1 THEN RETURN 1 AS y
         |  WHEN x = 2 THEN {
         |    DEFINE FUNCTION foo.bar() {
         |      RETURN 1 AS z
         |
         |      NEXT
         |
         |      USE graph
         |      ^
         |      RETURN 2 AS z
         |    }
         |
         |    RETURN 2 AS y
         |  }
         |  ELSE RETURN 3 AS y
         |})
         |
         |LET _res = foo.baz(1)
         |FINISH
         |""".stripMargin
    ) {
      runWithLC().hasErrorWithMarkedPosition(_ => gqlError, msg)
    }
  }

  /*
   * Updates in query body
   */
  test(
    s"""DEFINE FUNCTION foo.a() {
       |                ^
       |  CREATE (:A)
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasAtLeastErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a() {
       |                ^
       |  CREATE (a:A)
       |  RETURN a LIMIT 1
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a() {
       |                ^
       |  INSERT (a:A)
       |  RETURN a LIMIT 1
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a() {
       |                ^
       |  MERGE (a:A)
       |  RETURN a LIMIT 1
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(n) {
       |                ^
       |  DETACH DELETE n
       |  RETURN null LIMIT 1
       |}
       |
       |MATCH (a:A)
       |RETURN foo.a(a)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(n) {
       |                ^
       |  REMOVE n:A
       |  RETURN labels(n) LIMIT 1
       |}
       |
       |MATCH (a:B)
       |RETURN foo.a(a)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(n) {
       |                ^
       |  SET n:A
       |  RETURN labels(n) LIMIT 1
       |}
       |
       |MATCH (a:B)
       |RETURN foo.a(a)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(ns) {
       |                ^
       |  FOREACH (n IN ns | SET n:A)
       |  RETURN 1 LIMIT 1
       |}
       |
       |MATCH (a:B)
       |WITH COLLECT(a) AS allA
       |RETURN foo.a(allA)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(n) {
       |                ^
       |  CALL (n) {
       |    SET n:A
       |    RETURN labels(n) AS ls
       |  }
       |  RETURN ls LIMIT 1
       |}
       |
       |MATCH (a:B)
       |RETURN foo.a(a)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  test(
    s"""DEFINE PROCEDURE set(o) {
       |  SET o:A
       |  RETURN o
       |}
       |DEFINE FUNCTION foo.a(n) {
       |                ^
       |  CALL set(n) YIELD o AS nUpdated
       |  RETURN labels(nUpdated) LIMIT 1
       |}
       |
       |MATCH (a:B)
       |RETURN foo.a(a)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(n) {
       |                ^
       |  DEFINE PROCEDURE set(o) {
       |    SET o:A
       |    RETURN o
       |  }
       |
       |  CALL set(n) YIELD o AS nUpdated
       |  RETURN labels(nUpdated) LIMIT 1
       |}
       |
       |MATCH (a:A)
       |RETURN foo.a(a)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(n) {
       |                ^
       |  DEFINE PROCEDURE bar(o) {
       |    DEFINE PROCEDURE set(o) {
       |      SET o:A
       |      FINISH
       |    }
       |
       |    CALL set(o)
       |    RETURN o
       |  }
       |
       |  CALL bar(n) YIELD o AS nUpdated
       |  RETURN labels(nUpdated) LIMIT 1
       |}
       |
       |MATCH (a:A)
       |RETURN foo.a(a)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N57(
          "Local function",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N57()
    )
  }

  /*
   * Valid query body returns
   */
  // one aliased return item with LIMIT 1
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  RETURN 1 AS x LIMIT 1
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  // one unaliased return item with LIMIT 1
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  RETURN 1 LIMIT 1
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  // one aliased total aggregate 1
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  RETURN COUNT(*)
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  // one aliased total aggregate 2
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  UNWIND [1, 2, 3] AS x
       |  RETURN MIN(x * -1)
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  // one unaliased total aggregate
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  UNWIND [1, 2, 3] AS x
       |  RETURN AVG(x)
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  // one unaliased total aggregate with LIMIT 1
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  UNWIND [1, 2, 3] AS x
       |  RETURN SUM(x)
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  /*
   * Invalid query body returns
   */
  // FINISH
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  FINISH
       |  ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  // RETURN * with multiple variables in scope
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  LET x = 1, y = 2
       |  RETURN *
       |  ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  // RETURN * with a single variable in scope
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  LET x = 1
       |  RETURN *
       |  ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  // RETURN * with a single variable in scope and LIMIT 1
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  LET x = 1
       |  RETURN * LIMIT 1
       |  ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  // more than one aliased return item
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  RETURN 1 AS a, 2 AS b LIMIT 1
       |  ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  // more than one unaliased return item
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  RETURN 1, 2 LIMIT 1
       |  ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  // more than one total aggregate
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  RETURN SUM(1), COUNT(*)
       |  ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  // missing LIMIT 1 (aliased)
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  RETURN 1 AS x
       |  ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  // missing LIMIT 1 (unaliased)
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  RETURN 1
       |  ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  // missing LIMIT 1 — but LIMIT 2
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  RETURN 1 LIMIT 2
       |  ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  // missing LIMIT 1 — but LIMIT 2 SKIP 1
  test(
    s"""DEFINE FUNCTION foo.a() {
       |  RETURN 1 SKIP 1 LIMIT 2
       |  ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  /* These three tests do not work with -ea due to SURF-839
  // missing aliases in top-level conditional query
  test(
    s"""DEFINE FUNCTION foo.a(x) {
       |  WHEN x = 2 THEN RETURN 2 LIMIT 1
       |                         ^
       |  ELSE RETURN "other" LIMIT 1
       |}
       |
       |RETURN foo.a(1)
       |""".stripMargin
  ) {
    runWithLC().hasAtLeastErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N21(
          "DEFINE",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N21("DEFINE")
    )
  }

  // missing aliases in top-level conditional query after NEXT
  test(
    s"""DEFINE FUNCTION foo.a(x) {
       |  UNWIND [1, 2, 3] AS y
       |  RETURN y
       |
       |  NEXT
       |
       |  WHEN x = 2 THEN RETURN 2 LIMIT 1
       |                         ^
       |  ELSE RETURN "other" LIMIT 1
       |}
       |
       |RETURN foo.a(1)
       |""".stripMargin
  ) {
    runWithLC().hasAtLeastErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N21(
          "DEFINE",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N21("DEFINE")
    )
  }

  // missing aliases in non-top-level conditional query
  test(
    s"""DEFINE FUNCTION foo.a(x) {
       |  UNWIND [1, 2, 3] AS y
       |  CALL (x, y) {
       |    WHEN x = 2 THEN RETURN 2 LIMIT 1
       |                           ^
       |    ELSE RETURN "other" LIMIT 1
       |  }
       |  RETURN y LIMIT 1
       |}
       |
       |RETURN foo.a(1)
       |""".stripMargin
  ) {
    runWithLC().hasAtLeastErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N21(
          "CALL () { RETURN ... }",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42N21("CALL () { RETURN ... }")
    )
  }
   */

  // aliased UNION
  test(
    s"""DEFINE FUNCTION foo.a(x) {
       |  RETURN 1 AS x LIMIT 1
       |  UNION
       |  ^
       |  RETURN 2 AS x LIMIT 1
       |}
       |
       |RETURN foo.a(1)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAN(
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAN("foo.a")
    )
  }

  // unaliased UNION
  test(
    s"""DEFINE FUNCTION foo.a(x) {
       |  RETURN 1 LIMIT 1
       |  UNION
       |  ^
       |  RETURN 2 LIMIT 1
       |}
       |
       |RETURN foo.a(1)
       |""".stripMargin
  ) {
    runWithLC().hasErrorsWithMarkedPositionMapped(positions => {
      val pos = positions.head
      Seq(
        SemanticError(
          GqlHelper.getGql42001_42NAN(
            "foo.a",
            pos.offset,
            pos.line,
            pos.column
          ),
          msg42NAN("foo.a"),
          pos
        ),
        SemanticError(
          GqlHelper.getGql42001_42N39(
            "UNION subqueries",
            pos.offset,
            pos.line,
            pos.column
          ),
          msg42N39(),
          pos
        )
      )
    })
  }

  /*
   * NOT NULL in the input signature
   * Forbidding due to current limitations of NULL type inference
   * Ideally these would be valid queries
   */
  // with query body
  test(
    s"""DEFINE FUNCTION foo.a(a :: INT, b :: INT NOT NULL) {
       |                                ^
       |  RETURN SUM(a + b)
       |}
       |
       |RETURN foo.a(1, 2)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAL(
          "INTEGER NOT NULL",
          "b",
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAL("INTEGER NOT NULL", "b", "foo.a")
    )
  }

  // with expression body
  test(
    s"""DEFINE FUNCTION foo.a(a :: INT, b :: INT NOT NULL) = a + b
       |                                ^
       |
       |RETURN foo.a(1, 2)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAL(
          "INTEGER NOT NULL",
          "b",
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAL("INTEGER NOT NULL", "b", "foo.a")
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(x :: STRING NOT NULL, y :: INT NOT NULL) = 1
       |                      ^                     ^
       |
       |RETURN foo.a(1, 2)
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAL(
          "STRING NOT NULL",
          "x",
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAL("STRING NOT NULL", "x", "foo.a"),
      pos =>
        GqlHelper.getGql42001_42NAL(
          "INTEGER NOT NULL",
          "y",
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAL("INTEGER NOT NULL", "y", "foo.a")
    )
  }

  // LIST<... NOT NULL>
  test(
    s"""DEFINE FUNCTION foo.a(a :: LIST<INT NOT NULL>) = 1
       |                      ^
       |
       |RETURN foo.a([1])
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAL(
          "LIST<INTEGER NOT NULL>",
          "a",
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAL("LIST<INTEGER NOT NULL>", "a", "foo.a")
    )
  }

  /*
   * NOT NULL in the return type
   * Forbidding due to current limitations of NULL type inference
   * Ideally these would be valid queries
   */
  // with query body
  test(
    s"""DEFINE FUNCTION foo.a() :: INT NOT NULL {
       |                ^
       |  RETURN 2 LIMIT 1
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAK(
          "INTEGER NOT NULL",
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAK("INTEGER NOT NULL", "foo.a")
    )
  }

  // with expression body
  test(
    s"""DEFINE FUNCTION foo.a() :: INT NOT NULL = 2
       |                ^
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAK(
          "INTEGER NOT NULL",
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAK("INTEGER NOT NULL", "foo.a")
    )
  }

  // LIST<... NOT NULL>
  test(
    s"""DEFINE FUNCTION foo.a() :: LIST<INT NOT NULL> = [1]
       |                ^
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42NAK(
          "LIST<INTEGER NOT NULL>",
          "foo.a",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42NAK("LIST<INTEGER NOT NULL>", "foo.a")
    )
  }

  /*
   * Type mismatch inside input signature
   */
  // with query body
  test(
    s"""DEFINE FUNCTION foo.a(x :: INT = true) {
       |                                 ^---
       |  RETURN 1 LIMIT 1
       |}
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("INTEGER").asJava,
          "BOOLEAN",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("Integer", "Boolean")
    )
  }

  // with expression body
  test(
    s"""DEFINE FUNCTION foo.a(x :: INT = true) = 1
       |                                 ^---
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("INTEGER").asJava,
          "BOOLEAN",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("Integer", "Boolean")
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(v :: BOOLEAN, x :: STRING = "true", y :: STRING = 123, z :: INT = 123) {
       |                                                                        ^--
       |  RETURN 1 LIMIT 1
       |}
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("STRING").asJava,
          "INTEGER",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("String", "Integer")
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(x :: STRING = 123, y :: INT = "abc") {
       |                                    ^--             ^----
       |  RETURN 1 LIMIT 1
       |}
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("STRING").asJava,
          "INTEGER",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("String", "Integer"),
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("INTEGER").asJava,
          "STRING",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("Integer", "String")
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(x :: ANY<INT|STRING> = true) {
       |                                             ^---
       |  RETURN 1 LIMIT 1
       |}
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("INTEGER", "STRING").asJava,
          "BOOLEAN",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("Integer or String", "Boolean")
    )
  }

  /*
   * Type mismatch inside body
   */
  // inside expression body between parameter and constant
  test(
    s"""DEFINE FUNCTION foo.a(x :: INT) = x || 4
       |                                  ^
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("STRING", "LIST").asJava,
          "INTEGER",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("String or List<T>", "Integer")
    )
  }

  // inside query body between parameter and constant
  test(
    s"""DEFINE FUNCTION foo.a(x :: INT) {
       |  RETURN x || 4 AS z LIMIT 1
       |         ^
       |}
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("STRING", "LIST").asJava,
          "INTEGER",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("String or List<T>", "Integer")
    )
  }

  // inside query body between parameters
  test(
    s"""DEFINE FUNCTION foo.a(x :: STRING, y :: INT) {
       |  RETURN x || y AS z LIMIT 1
       |              ^
       |}
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("STRING").asJava,
          "INTEGER",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("String", "Integer")
    )
  }

  // inside nested query body between parameter and constant
  test(
    s"""DEFINE FUNCTION foo.a(x :: INT) {
       |  DEFINE FUNCTION foo.b(x :: INT) {
       |    RETURN x || 4 AS z LIMIT 1
       |           ^
       |  }
       |
       |  RETURN foo.b(x) LIMIT 1
       |}
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("STRING", "LIST").asJava,
          "INTEGER",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("String or List<T>", "Integer")
    )
  }

  /*
   * Result that matches return type
   */
  // with query body
  test(
    s"""DEFINE FUNCTION foo.a() :: INT {
       |  RETURN 1 AS a LIMIT 1
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  // with expression body
  test(
    s"""DEFINE FUNCTION foo.a() :: INT = 1
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a() :: ANY<FLOAT|INT> = 2.0
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a() :: LIST<INT> = [x IN [2] | x * x]
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a() :: MAP = {a: 1}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a() :: LIST<LIST<INT>> = [[2], [2]]
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a() :: ANY = true
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a() :: ANY = 123
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a() = 123
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a() :: ANY<INT|STRING> = head(COLLECT {
       |  RETURN "abc" AS a
       |  UNION
       |  RETURN 123 AS a
       |})
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a() :: ANY<INT|STRING> = head(COLLECT {
       |  RETURN ["abc", 123, true][2] AS a
       |  UNION
       |  RETURN ["abc", 123, true][1] AS a
       |  UNION
       |  RETURN ["abc", 123, true][0] AS a
       |})
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  test(
    s"""DEFINE FUNCTION foo.a(x) :: ANY<INT|STRING> {
       |  WHEN x = 2 THEN RETURN 2 AS z LIMIT 1
       |  ELSE RETURN "other" AS z LIMIT 1
       |}
       |
       |RETURN foo.a(1)
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  // type checking of the callable body does not take arguments into account
  test(s"""DEFINE FUNCTION foo.a(x) :: INT = x
          |
          |RETURN foo.a("abc")
          |""".stripMargin) {
    runWithLC().hasNoErrors
  }

  /*
   * Type mismatch where result that does not match return type
   */
  // type mismatch even not called (with query body)
  test(
    s"""DEFINE FUNCTION foo.a(x :: STRING) :: STRING {
       |  RETURN 123 AS z LIMIT 1
       |                ^
       |}
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql22G03_22N27(
          "INTEGER",
          "`z`",
          List("STRING").asJava,
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22N27("String", "Integer")
    )
  }

  // type mismatch even not called (with expression body)
  test(
    s"""DEFINE FUNCTION foo.a(x :: STRING) :: STRING = 123
       |                                               ^--
       |
       |FINISH
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("STRING").asJava,
          "INTEGER",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("String", "Integer")
    )
  }

  // type mismatch 1
  test(
    s"""DEFINE FUNCTION foo.a() :: INT = "1"
       |                                 ^--
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("INTEGER").asJava,
          "STRING",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("Integer", "String")
    )
  }

  // type mismatch 2
  test(
    s"""DEFINE FUNCTION foo.a() :: INT = 1.0
       |                                 ^--
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("INTEGER").asJava,
          "FLOAT",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("Integer", "Float")
    )
  }

  // type mismatch 3
  test(
    s"""DEFINE FUNCTION foo.a(x :: STRING) :: INT = x
       |                                            ^
       |
       |RETURN foo.a("abc")
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql22G03_22N27(
          "STRING",
          "`x`",
          List("INTEGER").asJava,
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22N27("Integer", "String")
    )
  }

  // type mismatch 4
  test(
    s"""DEFINE FUNCTION foo.a() :: ANY<INT|STRING> = 1.0
       |                                             ^--
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("INTEGER", "STRING").asJava,
          "FLOAT",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("Integer or String", "Float")
    )
  }

  // type mismatch 5
  test(
    s"""DEFINE FUNCTION foo.a() :: INT {
       |  CALL () {
       |    RETURN true AS a
       |    UNION
       |    RETURN 1.0 AS a
       |  }
       |  RETURN a LIMIT 1
       |         ^
       |}
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql22G03_22N27(
          "ANY", // it would be better if that would be ANY<BOOLEAN|FLOAT>
          "`a`",
          List("INTEGER").asJava,
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22N27("Integer", "Boolean or Float")
    )
  }

  // type mismatch 6
  // This test does not fail at compile time due to limitation in the type system.
  // Ideally the COLLECT {...} would be inferred as ANY<BOOLEAN|FLOAT> and the check would fail.
  test(
    s"""DEFINE FUNCTION foo.a() :: ANY<INT|STRING> = head(COLLECT {
       |                                             ^
       |  RETURN true AS a
       |  UNION
       |  RETURN 1.0 AS a
       |})
       |
       |RETURN foo.a()
       |""".stripMargin
  ) {
    runWithLC().hasNoErrors
  }

  /*
   * Not enough argument
   */
  // with query body
  test(
    s"""DEFINE FUNCTION foo.a(a :: INT) {
       |  RETURN COUNT(*)
       |}
       |
       |RETURN foo.a()
       |       ^
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42I13(
          1,
          0,
          "foo.a",
          "foo.a(a :: INTEGER) :: ANY",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42I13_tooFew("foo.a")
    )
  }

  // with expression body
  test(
    s"""DEFINE FUNCTION foo.a(a :: INT) = 1
       |
       |RETURN foo.a()
       |       ^
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42I13(
          1,
          0,
          "foo.a",
          "foo.a(a :: INTEGER) :: ANY",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42I13_tooFew("foo.a")
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(a :: INT, b :: STRING, c, d, e) = 1
       |
       |RETURN foo.a(1, "2", 3)
       |       ^
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42I13(
          5,
          3,
          "foo.a",
          "foo.a(a :: INTEGER, b :: STRING, c :: ANY, d :: ANY, e :: ANY) :: ANY",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42I13_tooFew("foo.a")
    )
  }

  /*
   * To many arguments
   */
  // with query body
  test(
    s"""DEFINE FUNCTION foo.a(a :: INT) {
       |  RETURN COUNT(*)
       |}
       |
       |RETURN foo.a(1, 2)
       |       ^
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42I13(
          1,
          2,
          "foo.a",
          "foo.a(a :: INTEGER) :: ANY",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42I13_tooMany("foo.a")
    )
  }

  // with expression body
  test(
    s"""DEFINE FUNCTION foo.a(a :: INT) = 1
       |
       |RETURN foo.a(1, 2)
       |       ^
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42I13(
          1,
          2,
          "foo.a",
          "foo.a(a :: INTEGER) :: ANY",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42I13_tooMany("foo.a")
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(a :: INT, b :: STRING, c, d, e) = 1
       |
       |RETURN foo.a(1, "2", 3, 4, 5, 6, 7, 8)
       |       ^
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42I13(
          5,
          8,
          "foo.a",
          "foo.a(a :: INTEGER, b :: STRING, c :: ANY, d :: ANY, e :: ANY) :: ANY",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg42I13_tooMany("foo.a")
    )
  }

  /*
   * Type mismatch in argument
   */
  // with query body
  test(
    s"""DEFINE FUNCTION foo.a(a :: INT) {
       |  RETURN COUNT(*)
       |}
       |
       |RETURN foo.a(true)
       |             ^---
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql22G03_22N27(
          "BOOLEAN",
          "argument at index 0 of function foo.a()",
          List("INTEGER").asJava,
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22N27("Integer", "Boolean")
    )
  }

  // with expression body
  test(
    s"""DEFINE FUNCTION foo.a(a :: INT) = 1
       |
       |RETURN foo.a(true)
       |             ^---
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql22G03_22N27(
          "BOOLEAN",
          "argument at index 0 of function foo.a()",
          List("INTEGER").asJava,
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22N27("Integer", "Boolean")
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(a :: INT, b :: STRING) = 1
       |
       |RETURN foo.a("a", 10)
       |             ^--
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql22G03_22N27(
          "STRING",
          "argument at index 0 of function foo.a()",
          List("INTEGER").asJava,
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22N27("Integer", "String")
    )
  }

  /*
   * Type mismatch after invocation
   */
  // between result and constant (with query body)
  test(
    s"""DEFINE FUNCTION foo.a(x :: STRING) :: STRING {
       |  RETURN x || "def" AS z LIMIT 1
       |}
       |
       |LET z = foo.a("abc")
       |RETURN z / 3
       |       ^
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("FLOAT", "INTEGER", "DURATION").asJava,
          "STRING",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("Float, Integer or Duration", "String")
    )
  }

  // between result and constant (with expression body)
  test(
    s"""DEFINE FUNCTION foo.a(x :: STRING) :: STRING = x || "def"
       |
       |LET z = foo.a("abc")
       |RETURN z / 3
       |       ^
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("FLOAT", "INTEGER", "DURATION").asJava,
          "STRING",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("Float, Integer or Duration", "String")
    )
  }

  // between results
  test(
    s"""DEFINE FUNCTION foo.a() :: INT = 1
       |DEFINE FUNCTION foo.b() :: STRING {
       |  RETURN "abc" LIMIT 1
       |}
       |
       |LET a = foo.a()
       |LET b = foo.b()
       |RETURN a * b AS z
       |           ^
       |""".stripMargin
  ) {
    runWithLC().hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_22NB1(
          List("FLOAT", "INTEGER", "DURATION").asJava,
          "STRING",
          pos.offset,
          pos.line,
          pos.column
        ),
      msg22NB1("Float, Integer or Duration", "String")
    )
  }

  /*
   * body does not cause shadowing notification
   */
  test(
    s"""MATCH (n)
       |CALL (n) {
       |  DEFINE FUNCTION foo() {
       |    MATCH (n)
       |    RETURN n LIMIT 1
       |  }
       |  RETURN foo() AS m
       |}
       |RETURN n, m""".stripMargin
  ) {
    runWithLC().hasNoErrors.hasNoNotifications
  }
}
