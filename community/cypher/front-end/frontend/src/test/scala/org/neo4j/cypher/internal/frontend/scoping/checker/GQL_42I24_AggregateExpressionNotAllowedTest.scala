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
import org.neo4j.cypher.internal.frontend.scoping.E42I24
import org.neo4j.cypher.internal.frontend.scoping.E42I79
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.differentOutcomeCypher25Onwards
import org.neo4j.cypher.internal.frontend.scoping.Versioned.passesCypher25Onwards

/**
 * Test for 42I24 - Aggregate expression not allowed in this context.
 */
class GQL_42I24_AggregateExpressionNotAllowedTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  // Thrown by AggregationChecker
  override val checkersUnderTest: Seq[Transformer[BaseContext, BaseState, BaseState]] =
    Seq(AggregationAnalysis)

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      """WITH 10 AS a
        |RETURN a
        |  ORDER BY sum(a) - a ASCENDING""".stripMargin,
      E42I24("sum(a) - a"),
      Seq("a")
    ),
    TestQuery(
      """WITH 10 AS a
        |WITH a
        |  ORDER BY sum(a) - a ASCENDING
        |RETURN a""".stripMargin,
      E42I24("sum(a) - a"),
      Seq("a")
    ),
    TestQuery(
      """WITH 10 AS a
        |UNWIND [1, 2, 3] AS x
        |RETURN a, SUM(x / a) + a * 5 AS s
        |  ORDER BY s * MAX(a * x) - a ASCENDING""".stripMargin,
      differentOutcomeCypher25Onwards(E42I79("s"), E42I24("MAX(a * x)")),
      Seq("a", "`SUM(x / a) + a * 5`")
    ),
    TestQuery(
      """MATCH (a:A)
        |WITH a, a.num + a.num2 AS sum
        |WITH a.num2 % 3 AS mod, min(sum) AS min
        |  ORDER BY sum(sum)
        |  LIMIT 2
        |RETURN mod, min""".stripMargin,
      passesCypher25Onwards(E42I24("sum(sum)")),
      Seq("mod", "min")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN n.name, sum(n.age) AS s ORDER BY max(n.age)""".stripMargin,
      passesCypher25Onwards(E42I24("max(n.age)")),
      Seq("`n.name`", "s")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |RETURN SUM(x) AS s ORDER BY SUM(x) + COUNT(*)""".stripMargin,
      passesCypher25Onwards(E42I24("count(*)")),
      Seq("s")
    ),

    // Positive tests

    TestQuery(
      """MATCH (n)
        |RETURN n.name, sum(n.age) AS s ORDER BY s""".stripMargin,
      Passes,
      Seq("`n.name`", "s")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN n.name, sum(n.age) AS s ORDER BY sum(n.age)""".stripMargin,
      Passes,
      Seq("`n.name`", "s")
    ),
    TestQuery(
      """MATCH (n)
        |RETURN n.name, sum(n.age) AS s ORDER BY n.name""".stripMargin,
      Passes,
      Seq("`n.name`", "s")
    ),
    TestQuery(
      """WITH 1 AS x ORDER BY x RETURN x""".stripMargin,
      Passes,
      Seq("x")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |WITH SUM(x) AS s ORDER BY s ASCENDING
        |RETURN s""".stripMargin,
      Passes,
      Seq("s")
    ),
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |WITH SUM(x) AS s WHERE s > 0
        |RETURN s""".stripMargin,
      Passes,
      Seq("s")
    )
  )
}
