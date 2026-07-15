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

import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.AggregationAnalysis
import org.neo4j.cypher.internal.frontend.scoping.E42N44
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

/**
 * Test for 42N44 - Inaccessible Variable
 */
class GQL_42N44_InaccessibleVariableTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  // Thrown by AggregationChecker
  override val checkersUnderTest: Seq[Transformer[BaseContext, BaseState, BaseState]] =
    Seq(AggregationAnalysis)

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests

    // Aggregation
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |RETURN SUM(x) AS s ORDER BY x ASCENDING""".stripMargin,
      E42N44("x", "RETURN"),
      Seq("s")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN n.name, SUM(n.age) ORDER BY n""".stripMargin,
      E42N44("n", "RETURN"),
      Seq("`n.name`", "`SUM(n.age)`")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN n.name, SUM(n.age) ORDER BY n.name, n""".stripMargin,
      E42N44("n", "RETURN"),
      Seq("`n.name`", "`SUM(n.age)`")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN n.name, SUM(n.age) ORDER BY n.name, n.age""".stripMargin,
      E42N44("n", "RETURN"),
      Seq("`n.name`", "`SUM(n.age)`")
    ),
    TestQuery(
      """MATCH (n)
        |CALL (n) {
        |  WITH n AS m
        |  RETURN SUM(n.age) AS ages ORDER BY m.name
        |}
        |RETURN n, ages""".stripMargin,
      E42N44("m", "RETURN"),
      Seq("n", "ages")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |WITH SUM(x) AS s ORDER BY x ASCENDING
        |RETURN s""".stripMargin,
      E42N44("x", "WITH"),
      Seq("s")
    ),
    TestQuery(
      """MATCH (n)
        |WITH n.name AS m, SUM(n.age) AS sum ORDER BY n
        |RETURN m, sum""".stripMargin,
      E42N44("n", "WITH"),
      Seq("m", "sum")
    ),
    TestQuery(
      """MATCH (n)
        |CALL (n) {
        |  WITH n AS m
        |  WITH SUM(n.age) AS ages ORDER BY m.name
        |  RETURN ages
        |}
        |RETURN n, ages""".stripMargin,
      E42N44("m", "WITH"),
      Seq("n", "ages")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |WITH SUM(x) AS s WHERE x = 1
        |RETURN s""".stripMargin,
      E42N44("x", "WITH"),
      Seq("s")
    ),
    TestQuery(
      """MATCH (n)
        |WITH n.name AS m, SUM(n.age) AS sum WHERE n.rate = 1
        |RETURN m, sum""".stripMargin,
      E42N44("n", "WITH"),
      Seq("m", "sum")
    ),
    TestQuery(
      """MATCH (n)
        |CALL (n) {
        |  WITH n AS m
        |  WITH SUM(n.age) AS ages WHERE m.name = "a"
        |  RETURN ages
        |}
        |RETURN n, ages""".stripMargin,
      E42N44("m", "WITH"),
      Seq("n", "ages")
    ),
    // Distinct
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |RETURN DISTINCT s ORDER BY x ASCENDING""".stripMargin,
      E42N44("x", "RETURN"),
      Seq("s")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN DISTINCT n.name ORDER BY n""".stripMargin,
      E42N44("n", "RETURN"),
      Seq("`n.name`")
    ),
    TestQuery(
      """MATCH (n)
        |CALL (n) {
        |  WITH n AS m
        |  RETURN DISTINCT n.age AS ages ORDER BY m.name
        |}
        |RETURN n, ages""".stripMargin,
      E42N44("m", "RETURN"),
      Seq("n", "ages")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |WITH DISTINCT s ORDER BY x ASCENDING
        |RETURN s""".stripMargin,
      E42N44("x", "WITH"),
      Seq("s")
    ),
    TestQuery(
      """MATCH (n)
        |WITH DISTINCT n.name AS m ORDER BY n
        |RETURN m""".stripMargin,
      E42N44("n", "WITH"),
      Seq("m")
    ),
    TestQuery(
      """MATCH (n)
        |CALL (n) {
        |  WITH n AS m
        |  WITH DISTINCT n.age AS ages ORDER BY m.name
        |  RETURN ages
        |}
        |RETURN n, ages""".stripMargin,
      E42N44("m", "WITH"),
      Seq("n", "ages")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |WITH DISTINCT s WHERE x = 1
        |RETURN s""".stripMargin,
      E42N44("x", "WITH"),
      Seq("s")
    ),
    TestQuery(
      """MATCH (n)
        |WITH DISTINCT n.name AS m WHERE n.age = 1
        |RETURN m""".stripMargin,
      E42N44("n", "WITH"),
      Seq("m")
    ),
    TestQuery(
      """MATCH (n)
        |CALL (n) {
        |  WITH n AS m
        |  WITH DISTINCT n.age AS ages WHERE m.name = "a"
        |  RETURN ages
        |}
        |RETURN n, ages""".stripMargin,
      E42N44("m", "WITH"),
      Seq("n", "ages")
    ),
    TestQuery(
      """MATCH p = (x)-[r:R]->(n:L)
        |WITH DISTINCT p
        |  WHERE COUNT{ MATCH (n) WITH x.p AS a } >= 0
        |RETURN p""".stripMargin,
      E42N44("x", "WITH"),
      Seq("p")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |WITH 1 AS x
        |RETURN a AS z, SUM(a / x) * 5 AS s
        |  ORDER BY EXISTS { CALL (x) { RETURN x + 1 AS r1 } RETURN 1 AS r}""".stripMargin,
      E42N44("x", "RETURN"),
      Seq("z", "s")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |WITH 1 AS x
        |RETURN a AS z, SUM(a / x) * 5 AS s
        |  ORDER BY EXISTS { RETURN EXISTS { RETURN x + 1 AS r1 } AS r}""".stripMargin,
      E42N44("x", "RETURN"),
      Seq("z", "s")
    ),
    TestQuery(
      """MATCH (a:A), (b:B)
        |WITH a.p AS ap, b.p + 1 AS bp, count(*) AS count
        |  ORDER BY a.p, 1 + b.p + 1
        |RETURN ap, bp""".stripMargin,
      E42N44("b", "WITH"),
      Seq("ap", "bp")
    ),
    TestQuery(
      "MATCH (p) WITH DISTINCT p.email AS mail ORDER BY p.name RETURN mail AS mail",
      E42N44("p", "WITH"),
      Seq("mail")
    ),
    TestQuery(
      "MATCH (p) WITH collect(p.email) AS mail ORDER BY p.name RETURN mail AS mail",
      E42N44("p", "WITH"),
      Seq("mail")
    ),
    TestQuery(
      "MATCH (p) RETURN DISTINCT p.email AS mail ORDER BY p.name",
      E42N44("p", "RETURN"),
      Seq("mail")
    ),
    TestQuery(
      "MATCH (p) RETURN collect(p.email) AS mail ORDER BY p.name",
      E42N44("p", "RETURN"),
      Seq("mail")
    ),
    TestQuery(
      "MATCH (p) WITH DISTINCT p.email AS mail WHERE p.name IS NOT NULL RETURN mail AS mail",
      E42N44("p", "WITH"),
      Seq("mail")
    ),
    TestQuery(
      "MATCH (p) WITH collect(p.email) AS mail WHERE p.name IS NOT NULL RETURN mail AS mail",
      E42N44("p", "WITH"),
      Seq("mail")
    ),

    // Positive tests
    TestQuery(
      """MATCH (a:A), (b:B)
        |WITH a.p AS ap, b.p + 1 AS bp, count(*) AS count
        |  ORDER BY a.p, b.p + 1
        |RETURN ap, bp""".stripMargin,
      Passes,
      Seq("ap", "bp")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN n, SUM(n.age) ORDER BY n""".stripMargin,
      Passes,
      Seq("n", "`SUM(n.age)`")
    ),
    TestQuery(
      """WITH 1 AS m
        |MATCH (n)
        |RETURN m AS w, n, SUM(n.age) ORDER BY m""".stripMargin,
      Passes,
      Seq("w", "n", "`SUM(n.age)`")
    ),
    TestQuery(
      """WITH 1 AS m
        |MATCH (n)
        |RETURN m AS w, n, SUM(n.age) ORDER BY w""".stripMargin,
      Passes,
      Seq("w", "n", "`SUM(n.age)`")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN *, SUM(n.age) ORDER BY n""".stripMargin,
      Passes,
      Seq("n", "`SUM(n.age)`")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN *, 1 AS n, SUM(n.age) ORDER BY n""".stripMargin,
      Passes,
      Seq("n", "`SUM(n.age)`")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN n, SUM(n.age) ORDER BY n.name""".stripMargin,
      Passes,
      Seq("n", "`SUM(n.age)`")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |RETURN SUM(x) AS x
        |  ORDER BY x ASCENDING""".stripMargin,
      Passes,
      Seq("x")
    ),
    TestQuery(
      """MATCH (n)
        |CALL (n) {
        |  RETURN n AS m, SUM(n.age) AS ages ORDER BY n.name
        |}
        |RETURN n, ages""".stripMargin,
      Passes,
      Seq("n", "ages")
    ),
    TestQuery(
      """MATCH (n)
        |CALL (n) {
        |  RETURN SUM(n.age) AS ages ORDER BY n.name
        |}
        |RETURN n, ages""".stripMargin,
      Passes,
      Seq("n", "ages")
    ),
    TestQuery(
      """WITH 1 AS x WHERE x = 1 RETURN x""".stripMargin,
      Passes,
      Seq("x")
    ),
    TestQuery(
      """WITH 1 AS x ORDER BY x RETURN x""".stripMargin,
      Passes,
      Seq("x")
    ),
    TestQuery(
      """RETURN 1 AS x ORDER BY x RETURN x""".stripMargin,
      Passes,
      Seq("x")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |RETURN SUM(x) AS s ORDER BY s ASCENDING""".stripMargin,
      Passes,
      Seq("s")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |RETURN SUM(x) AS s ORDER BY SUM(x) ASCENDING""".stripMargin,
      Passes,
      Seq("s")
    ),
    TestQuery(
      """UNWIND [1,2,3] AS b
        |UNWIND [0,1] AS a
        |FILTER a <> b
        |WITH a, SUM(b) AS sumB WHERE 1 + SUM(b) > 5
        |RETURN *""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "sumB")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN n.name, SUM(n.age) ORDER BY n.name""".stripMargin,
      Passes,
      Seq("`n.name`", "`SUM(n.age)`")
    ),
    TestQuery(
      """MATCH p = ()-[r:R]->(n:L)
        |WITH DISTINCT p
        |  WHERE COUNT{ MATCH (n) WITH p AS a } >= 0
        |RETURN p""".stripMargin,
      Passes,
      Seq("p")
    )
  )
}
