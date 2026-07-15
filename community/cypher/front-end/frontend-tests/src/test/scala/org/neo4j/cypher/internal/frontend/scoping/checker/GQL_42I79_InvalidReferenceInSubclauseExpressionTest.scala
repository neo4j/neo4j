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
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

/**
 * Test for 42I79 - Invalid Reference To Implicitly Grouped Expressions
 */
class GQL_42I79_InvalidReferenceInSubclauseExpressionTest extends VariableCheckingWithLocalCallablesTestSuite {

  // Only thrown by Aggregation Analysis
  override val checkersUnderTest: Seq[Transformer[BaseContext, BaseState, BaseState]] = Seq(AggregationAnalysis)

  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = Seq(
    // Negative tests
    TestQuery(
      """WITH 10 AS a
        |UNWIND [1, 2, 3] AS x
        |RETURN a, SUM(x / a) + a * 5 AS s
        |  ORDER BY s * MAX(a * x) - a ASCENDING""".stripMargin,
      differentOutcomeCypher25Onwards(E42I79("s"), E42I24("MAX(a * x)")),
      Seq("a", "`SUM(x / a) + a * 5`")
    ),
    TestQuery(
      """WITH 10 AS a
        |UNWIND [1, 2, 3] AS x
        |RETURN a, x + 1 AS y, SUM(x / a) + a * 5 AS s
        |  ORDER BY y * MAX(a * x) - a ASCENDING""".stripMargin,
      differentOutcomeCypher25Onwards(E42I79("y"), E42I24("MAX(a * x)")),
      Seq("a", "y", "`SUM(x / a) + a * 5`")
    ),
    TestQuery(
      """WITH 10 AS a
        |UNWIND [1, 2, 3] AS x
        |RETURN a, x + 1 AS y, SUM(x / a) + a * 5 AS s
        |  ORDER BY 1 * MAX(s * a * x) - a ASCENDING""".stripMargin,
      differentOutcomeCypher25Onwards(E42I79("s"), E42I24("MAX((s * a) * x)")),
      Seq("a", "y", "`SUM(x / a) + a * 5`")
    ),

    // Positive tests
    TestQuery(
      """UNWIND [1, 2, 3] AS x
        |RETURN SUM(x) AS s ORDER BY s + SUM(x) ASCENDING""".stripMargin,
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
      """UNWIND [1,2,3] AS b
        |UNWIND [0,1] AS a
        |WITH a, SUM(b) AS sumB WHERE 1 + SUM(b) > 5
        |RETURN *""".stripMargin,
      Passes,
      Seq("a", "sumB")
    )
  )
}
