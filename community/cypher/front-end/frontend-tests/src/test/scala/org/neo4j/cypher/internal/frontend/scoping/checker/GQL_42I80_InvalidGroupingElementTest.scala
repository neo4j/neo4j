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
import org.neo4j.cypher.internal.frontend.scoping.E42I18
import org.neo4j.cypher.internal.frontend.scoping.E42I80
import org.neo4j.cypher.internal.frontend.scoping.E42I80Aggregation
import org.neo4j.cypher.internal.frontend.scoping.E42N44WithGroupBy
import org.neo4j.cypher.internal.frontend.scoping.Exactly
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

/**
 * Test for 42I80 - Invalid Grouping Element.
 */
class GQL_42I80_InvalidGroupingElementTest extends VariableCheckingWithLocalCallablesTestSuite {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  // Thrown by AggregationChecker
  override val checkersUnderTest: Seq[Transformer[BaseContext, BaseState, BaseState]] =
    Seq(AggregationAnalysis)

  override def testCases(): Seq[TestQuery] = Seq(
    TestQuery(
      """WITH {p: 1} AS a, 2 AS b
        |RETURN a.p AS a, sum(b) AS s
        |  GROUP BY a.p""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a.p", "a"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS c, 2 AS b
        |RETURN c AS a, sum(b) AS s
        |  GROUP BY a.p""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a.p", "a"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS b
        |RETURN x AS a, sum(b) AS s
        |  GROUP BY a + 0""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a + 0", "a"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS b
        |RETURN x AS a, sum(b) AS s
        |  GROUP BY toInteger(a)""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("toInteger(a)", "a"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS x, 2 AS b
        |RETURN x AS a, sum(b) AS s
        |  GROUP BY a['p']""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a[\"p\"]", "a"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS a, 2 AS b
        |RETURN a.p AS a, sum(b) AS s
        |  GROUP BY a.p + b""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a.p + b", "a"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS a, 2 AS b, 3 AS d
        |RETURN a.p AS a, d, sum(b) AS s
        |  GROUP BY d, a.p""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a.p", "a"))),
      Seq("a", "d", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS a, 2 AS b
        |WITH a.p AS a, sum(b) AS s
        |  GROUP BY a.p
        |RETURN a, s""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a.p", "a"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS b
        |WITH x AS a, sum(b) AS s
        |  GROUP BY toInteger(a)
        |RETURN a, s""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("toInteger(a)", "a"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS b
        |RETURN x AS a, sum(b) AS s
        |  GROUP BY a, s""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80Aggregation("s", "s"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS b
        |RETURN x AS a, sum(b) AS s
        |  GROUP BY a, s.x""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80Aggregation("s.x", "s"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH 1 AS s, 2 AS b
        |RETURN s AS x, sum(b) AS s
        |  GROUP BY s""".stripMargin,
      ignoreBeforeCypher25(E42I80Aggregation("s", "s")),
      Seq("x", "s")
    ),
    TestQuery(
      """WITH {p: {q: 1}} AS x, 2 AS b
        |RETURN x AS a, sum(b) AS s
        |  GROUP BY a.p.q""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("(a.p).q", "a"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS x, 2 AS b
        |RETURN x AS a, b + sum(b) AS s
        |  GROUP BY a.p""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a.p", "a"), E42I18("b"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS x, 2 AS b, 3 AS d
        |RETURN x AS a, sum(b) AS s
        |  GROUP BY a.p
        |  ORDER BY sum(b) + d""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a.p", "a"), E42I18("d"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS x, 2 AS b
        |RETURN x AS a, sum(b) AS s
        |  GROUP BY a.p
        |  ORDER BY b""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a.p", "a"), E42N44WithGroupBy("b", "RETURN"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS x, {q: 1} AS y, 2 AS b
        |RETURN x AS a, y AS g, sum(b) AS s
        |  GROUP BY a.p + g.q""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a.p + g.q", "a"))),
      Seq("a", "g", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS x, 2 AS b
        |RETURN x AS a, x AS d, sum(b) AS s
        |  GROUP BY a.p""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a.p", "a"), E42I18("x"))),
      Seq("a", "d", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS x, 3 AS e, 2 AS b
        |RETURN x + e AS a, sum(b) AS s
        |  GROUP BY a.p""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80("a.p", "a"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS b
        |RETURN x AS a, sum(b) AS s
        |  GROUP BY s.p, a.p""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80Aggregation("s.p", "s"), E42I80("a.p", "a"))),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH 1 AS x, 2 AS b
        |RETURN x AS a, sum(b) AS s
        |  GROUP BY s + a""".stripMargin,
      ignoreBeforeCypher25(Exactly(E42I80Aggregation("s + a", "s"))),
      Seq("a", "s")
    ),

    // Positive tests
    TestQuery(
      """WITH {p: 1} AS a, 2 AS b
        |RETURN a.p AS a, sum(b) AS s
        |  GROUP BY a""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS a, 2 AS b
        |RETURN a.p AS k, sum(b) AS s
        |  GROUP BY a.p""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("k", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS a, 2 AS b
        |RETURN a.p + 0 AS k, sum(b) AS s
        |  GROUP BY a.p + 0""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("k", "s")
    ),
    TestQuery(
      """WITH {p: 1} AS a, 2 AS b
        |WITH a.p AS a, sum(b) AS s
        |  GROUP BY a
        |RETURN a, s""".stripMargin,
      ignoreBeforeCypher25(Passes),
      Seq("a", "s")
    )
  )
}
