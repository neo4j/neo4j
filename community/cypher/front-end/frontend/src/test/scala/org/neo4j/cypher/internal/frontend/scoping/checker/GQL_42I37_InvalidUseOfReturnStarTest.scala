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
package org.neo4j.cypher.internal.frontend.scoping.checker

import org.neo4j.cypher.internal.frontend.scoping.E42I37
import org.neo4j.cypher.internal.frontend.scoping.E42N62
import org.neo4j.cypher.internal.frontend.scoping.Exactly
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25
import org.neo4j.cypher.internal.frontend.scoping.checker.CompositionRestriction.NoCountOrExistsSubqueryBody

/**
 * Test for 42I37 - Invalid Use Of Return Star
 */
class GQL_42I37_InvalidUseOfReturnStarTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      s"""RETURN EXISTS {
         |  DEFINE PROCEDURE foo(x) {
         |    RETURN *
         |  }
         |  CALL foo(1)
         |  RETURN 1 AS x
         |} AS y""".stripMargin,
      ignoreBeforeCypher25(E42I37),
      Seq("x")
    ),
    TestQuery(
      """RETURN *""".stripMargin,
      E42I37,
      Seq.empty,
      compositionRestriction = NoCountOrExistsSubqueryBody
    ),
    TestQuery(
      """RETURN *, 1 AS x""".stripMargin,
      E42I37,
      Seq("x"),
      compositionRestriction = NoCountOrExistsSubqueryBody
    ),
    TestQuery(
      """MATCH () RETURN *""".stripMargin,
      E42I37,
      Seq.empty,
      compositionRestriction = NoCountOrExistsSubqueryBody
    ),
    TestQuery(
      """MATCH () RETURN *, 1 AS x""".stripMargin,
      E42I37,
      Seq("x"),
      compositionRestriction = NoCountOrExistsSubqueryBody
    ),
    TestQuery(
      """WITH *
        |RETURN *""".stripMargin,
      E42I37,
      Seq.empty,
      compositionRestriction = NoCountOrExistsSubqueryBody
    ),
    TestQuery(
      """MATCH ()
        |RETURN *
        |
        |NEXT
        |
        |RETURN x""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I37, E42N62("x"))),
      Seq("x"),
      compositionRestriction = NoCountOrExistsSubqueryBody
    ),
    TestQuery(
      """CALL () {
        |  MATCH () RETURN *
        |}""".stripMargin,
      E42I37,
      Seq.empty
    ),
    TestQuery(
      """CALL () {
        |  MATCH () RETURN *, 1 AS x
        |}""".stripMargin,
      E42I37,
      Seq.empty
    ),
    TestQuery(
      """DEFINE PROCEDURE foo() {
        |  RETURN *
        |}
        |CALL foo()
        |RETURN 1 AS x""".stripMargin,
      ignoreBeforeCypher25(E42I37),
      Seq("x")
    ),
    TestQuery(
      """DEFINE PROCEDURE foo(x) {
        |  RETURN *
        |}
        |CALL foo(1)
        |RETURN 1 AS x""".stripMargin,
      ignoreBeforeCypher25(E42I37),
      Seq("x")
    ),
    TestQuery(
      """DEFINE PROCEDURE foo() {
        |  RETURN *, 1 AS x
        |}
        |CALL foo(1)
        |RETURN x""".stripMargin,
      ignoreBeforeCypher25(E42I37),
      Seq("x")
    ),
    TestQuery(
      """DEFINE PROCEDURE foo(x) {
        |  RETURN *, x
        |}
        |CALL foo(1)
        |RETURN x""".stripMargin,
      ignoreBeforeCypher25(E42I37),
      Seq("x")
    )
  ) ++ (
    for {
      scalarSubquery <- Seq("EXISTS", "COUNT", "COLLECT")
    } yield Seq(
      TestQuery(
        s"""RETURN $scalarSubquery {
           |  CALL () {
           |    MATCH () RETURN *
           |  }
           |  RETURN 1 AS x
           |} AS x""".stripMargin,
        E42I37,
        Seq("x")
      ),
      TestQuery(
        s"""RETURN $scalarSubquery {
           |  DEFINE PROCEDURE foo() {
           |    RETURN *
           |  }
           |  CALL foo()
           |  RETURN 1 AS x
           |} AS x""".stripMargin,
        ignoreBeforeCypher25(E42I37),
        Seq("x")
      ),
      TestQuery(
        s"""RETURN $scalarSubquery {
           |  DEFINE PROCEDURE foo(x) {
           |    RETURN *
           |  }
           |  CALL foo(1)
           |  RETURN 1 AS x
           |} AS x""".stripMargin,
        ignoreBeforeCypher25(E42I37),
        Seq("x")
      ),
      TestQuery(
        s"""DEFINE FUNCTION bar() = $scalarSubquery {
           |  DEFINE PROCEDURE foo() {
           |    RETURN *
           |  }
           |  CALL foo()
           |  RETURN 1 AS x
           |}
           |RETURN bar() AS x""".stripMargin,
        ignoreBeforeCypher25(E42I37),
        Seq("x")
      ),
      TestQuery(
        s"""DEFINE FUNCTION bar() = $scalarSubquery {
           |  DEFINE PROCEDURE foo(x) {
           |    RETURN *
           |  }
           |  CALL foo(1)
           |  RETURN 1 AS x
           |}
           |RETURN bar() AS x""".stripMargin,
        ignoreBeforeCypher25(E42I37),
        Seq("x")
      ),
      TestQuery(
        s"""DEFINE FUNCTION bar() = $scalarSubquery {
           |  DEFINE PROCEDURE foo() {
           |    RETURN *, 1 AS x
           |  }
           |  CALL foo()
           |  RETURN x
           |}
           |RETURN bar() AS x""".stripMargin,
        ignoreBeforeCypher25(E42I37),
        Seq("x")
      ),
      TestQuery(
        s"""DEFINE FUNCTION bar() = $scalarSubquery {
           |  DEFINE PROCEDURE foo(x) {
           |    RETURN *, x
           |  }
           |  CALL foo(1)
           |  RETURN x
           |}
           |RETURN bar() AS x""".stripMargin,
        ignoreBeforeCypher25(E42I37),
        Seq("x")
      )
    )
  ).flatten ++ Seq(
    // Positive tests
    TestQuery(
      s"""WITH 1 AS x
         |RETURN *
         |
         |NEXT
         |
         |RETURN *
         |
         |NEXT
         |
         |WHEN true THEN RETURN x + 1 AS x
         |
         |NEXT
         |
         |RETURN *""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("x")
    )
  ) ++ (
    for {
      scalarSubquery <- Seq("EXISTS", "COUNT")
      // COLLECT does not work with RETURN * since it requires a single return item
    } yield Seq(
      TestQuery(
        s"""WITH 1 AS a
           |RETURN $scalarSubquery { RETURN * } AS x""".stripMargin,
        Passes,
        Seq("x")
      ),
      TestQuery(
        s"""MATCH (p:Person)
           |WHERE $scalarSubquery { MATCH (p) RETURN * } = 1
           |RETURN p.name AS name""".stripMargin,
        Passes,
        Seq("name")
      )
    )
  ).flatten
}
