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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AddedInRewriteGeneral
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnAddedInRewrite
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowCurrentGraphTypeClause
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowSettingsClause
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.frontend.PlannerName
import org.neo4j.cypher.internal.frontend.helpers.TestContext
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExpandClauses
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.rewriting.AstRewritingTestSupport
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.ExpandShowWhere
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.NormalizeWithAndReturnClauses
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.RewriteShowQuery
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpandClausesTest extends CypherFunSuite with RewritePhaseTest with AstConstructionTestSupport {

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    ScopeSurveyor andThen ExpandClauses

  override val phaseTestConfig = PhaseTestConfig(
    excludedVersions = Set(CypherVersion.Cypher5),
    semanticFeatures = Seq(SemanticFeature.UseAsMultipleGraphsSelector)
  )

  private def withUpdate() = (expectedStatement: Statement) => {
    expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
      // The original/rewritten statement will have AddedInRewriteGeneral,
      case w: With =>
        w.copy(withType = AddedInRewriteGeneral())(w.position)
      case ri: ReturnItems => ri.copy(projectionType = FreeProjection)(ri.position)
    }))
  }

  test("NEXT query rewritten 1") {
    assertRewritten(
      """LET a = 1
        |RETURN a
        |
        |NEXT
        |
        |LET b = a + 1
        |RETURN a, b
        |
        |NEXT
        |
        |LET c = a + b + 1
        |RETURN b, c
        |
        |NEXT
        |
        |LET d = b + c
        |RETURN d""".stripMargin,
      """WITH 1 AS a
        |WITH a AS a
        |WITH a AS a, a + 1 AS b
        |WITH a AS a, b AS b
        |WITH a AS a, b AS b, (a + b) + 1 AS c
        |WITH b AS b, c AS c
        |WITH b AS b, c AS c, b + c AS d
        |RETURN d AS d""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 2") {
    assertRewritten(
      """LET a = 1
        |RETURN a
        |
        |NEXT
        |
        |FINISH""".stripMargin,
      """WITH 1 AS a
        |WITH a AS a
        |FINISH""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 2b") {
    assertRewritten(
      """LET a = 1
        |RETURN a
        |
        |NEXT
        |
        |CREATE ()""".stripMargin,
      """WITH 1 AS a
        |WITH a AS a
        |CREATE ()""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query WHEN FINISH") {
    assertRewritten(
      """FINISH
        |
        |NEXT
        |
        |WHEN true THEN FINISH
        |ELSE FINISH""".stripMargin,
      """WITH count(NULL) AS `  UNNAMED0`
        |WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED1`
        |CALL (`  UNNAMED1`) {
        |  WITH `  UNNAMED1` AS `  UNNAMED1`
        |    WHERE `  UNNAMED1` = 0
        |  CALL () {
        |    FINISH
        |  }
        |  FINISH
        |  UNION ALL
        |  WITH `  UNNAMED1` AS `  UNNAMED1`
        |    WHERE `  UNNAMED1` = 1
        |  CALL () {
        |    FINISH
        |  }
        |  FINISH
        |}
        |FINISH""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 3") {
    assertRewritten(
      """LET a = 1
        |RETURN a
        |
        |NEXT
        |
        |FINISH
        |
        |NEXT
        |
        |LET b = 1
        |RETURN b""".stripMargin,
      """WITH 1 AS a
        |WITH a AS a
        |WITH count(NULL) AS `  UNNAMED0`
        |WITH 1 AS b
        |RETURN b AS b""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 4") {
    assertRewritten(
      """FINISH
        |
        |NEXT
        |
        |LET a = 1
        |RETURN a
        |
        |NEXT
        |
        |LET b = a + 1
        |RETURN a, b""".stripMargin,
      """WITH count(NULL) AS `  UNNAMED0`
        |WITH 1 AS a
        |WITH a AS a
        |WITH a AS a, a + 1 AS b
        |RETURN a AS a, b AS b""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 4b") {
    assertRewritten(
      """FINISH
        |
        |NEXT
        |
        |LET a = 1
        |WITH *, 2 AS a
        |RETURN a
        |
        |NEXT
        |
        |LET b = a + 1
        |RETURN a, b""".stripMargin,
      """WITH count(NULL) AS `  UNNAMED0`
        |WITH 1 AS a
        |WITH 2 AS a
        |WITH a AS a
        |WITH a AS a, a + 1 AS b
        |RETURN a AS a, b AS b""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 5") {
    assertRewritten(
      """LET a = 1
        |CALL (a) {
        |  RETURN 1 as b
        |
        |  NEXT
        |
        |  RETURN b + a as c
        |}
        |RETURN c""".stripMargin,
      """WITH 1 AS a
        |CALL (a) {
        |  WITH 1 AS b
        |  RETURN b + a AS c
        |}
        |RETURN c AS c""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 6") {
    assertRewritten(
      """RETURN 1 AS a
        |UNION
        |RETURN 2 AS a
        |
        |NEXT
        |
        |RETURN a + 1 AS b
        |UNION
        |RETURN a + 2 AS b""".stripMargin,
      """CALL () {
        |  RETURN 1 AS a
        |  UNION
        |  RETURN 2 AS a
        |}
        |WITH count(*) AS `  UNNAMED1`, collect([a]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS a
        |  RETURN a + 1 AS b
        |  UNION
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS a
        |  RETURN a + 2 AS b
        |}
        |RETURN b AS b""".stripMargin,
      additionalExpectedAstUpdates = withUpdate()
    )
  }

  test("NEXT query rewritten 7") {
    assertRewritten(
      """LET x = 1, y = 2
        |RETURN x
        |
        |NEXT
        |
        |RETURN 1 + x AS a
        |UNION
        |LET y = 3
        |RETURN 2 + y AS a
        |
        |NEXT
        |
        |RETURN a + 1 AS b
        |UNION
        |RETURN a + 2 AS b""".stripMargin,
      """WITH 1 AS x, 2 AS y
        |WITH x AS x
        |WITH count(*) AS `  UNNAMED1`, collect([x]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  RETURN 1 + x AS a
        |  UNION
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  WITH 3 AS y
        |  RETURN 2 + y AS a
        |}
        |WITH count(*) AS `  UNNAMED4`, collect([a]) AS `  UNNAMED3`
        |CALL (`  UNNAMED3`,`  UNNAMED4`) {
        |  UNWIND range(0, `  UNNAMED4` - 1) AS `  UNNAMED5`
        |  WITH (`  UNNAMED3`[`  UNNAMED5`])[0] AS a
        |  RETURN a + 1 AS b
        |  UNION
        |  UNWIND range(0, `  UNNAMED4` - 1) AS `  UNNAMED5`
        |  WITH (`  UNNAMED3`[`  UNNAMED5`])[0] AS a
        |  RETURN a + 2 AS b
        |}
        |RETURN b AS b""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 8") {
    assertRewritten(
      """{
        |  RETURN 1 AS a
        |
        |  NEXT
        |
        |  RETURN a + 1 AS b
        |}
        |
        |NEXT
        |
        |RETURN b + 1 AS c""".stripMargin,
      """WITH 1 AS a
        |WITH a + 1 AS b
        |RETURN b + 1 AS c""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 9") {
    assertRewritten(
      """MATCH (n)
        |RETURN n
        |
        |NEXT
        |
        |WHEN n.x > 2 THEN
        |  RETURN "large number" AS msg
        |WHEN n.x > 1 THEN
        |  RETURN "small number" AS msg
        |ELSE
        |  RETURN "tiny number" AS msg
        |
        |NEXT
        |
        |RETURN collect(msg) AS messages""".stripMargin,
      """MATCH (n)
        |WITH n AS n
        |WITH n AS n, CASE
        |  WHEN n.x > 2 THEN 0
        |  WHEN n.x > 1 THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |CALL (n,`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    RETURN "large number" AS msg
        |  }
        |  RETURN msg AS msg
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    RETURN "small number" AS msg
        |  }
        |  RETURN msg AS msg
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    RETURN "tiny number" AS msg
        |  }
        |  RETURN msg AS msg
        |}
        |RETURN collect(msg) AS messages""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 10") {
    assertRewritten(
      """LET a = 1
        |LET x = EXISTS {
        |  RETURN a + 1 AS b
        |
        |  NEXT
        |
        |  RETURN a + b AS c
        |
        |  NEXT
        |
        |  RETURN a + c AS d
        |}
        |RETURN a, x""".stripMargin,
      """WITH 1 AS a
        |WITH a AS a, EXISTS {
        |  WITH a + 1 AS b
        |  WITH a + b AS c
        |  RETURN a + c AS d
        |} AS x
        |RETURN a AS a, x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 11") {
    assertRewritten(
      """LET x = 1, y = 2
        |RETURN x, y
        |
        |NEXT
        |
        |RETURN 1 + x AS a
        |UNION
        |RETURN 2 + y AS a
        |
        |NEXT
        |
        |RETURN a + 1 AS b
        |UNION
        |RETURN a + 2 AS b
        |
        |NEXT
        |
        |RETURN *""".stripMargin,
      """WITH 1 AS x, 2 AS y
        |WITH x AS x, y AS y
        |WITH count(*) AS `  UNNAMED2`, collect([x]) AS `  UNNAMED0`, collect([y]) AS `  UNNAMED1`
        |CALL (`  UNNAMED0`,`  UNNAMED1`,`  UNNAMED2`) {
        |  UNWIND range(0, `  UNNAMED2` - 1) AS `  UNNAMED3`
        |  WITH (`  UNNAMED0`[`  UNNAMED3`])[0] AS x, (`  UNNAMED1`[`  UNNAMED3`])[0] AS y
        |  RETURN 1 + x AS a
        |  UNION
        |  UNWIND range(0, `  UNNAMED2` - 1) AS `  UNNAMED3`
        |  WITH (`  UNNAMED0`[`  UNNAMED3`])[0] AS x, (`  UNNAMED1`[`  UNNAMED3`])[0] AS y
        |  RETURN 2 + y AS a
        |}
        |WITH count(*) AS `  UNNAMED5`, collect([a]) AS `  UNNAMED4`
        |CALL (`  UNNAMED4`,`  UNNAMED5`) {
        |  UNWIND range(0, `  UNNAMED5` - 1) AS `  UNNAMED6`
        |  WITH (`  UNNAMED4`[`  UNNAMED6`])[0] AS a
        |  RETURN a + 1 AS b
        |  UNION
        |  UNWIND range(0, `  UNNAMED5` - 1) AS `  UNNAMED6`
        |  WITH (`  UNNAMED4`[`  UNNAMED6`])[0] AS a
        |  RETURN a + 2 AS b
        |}
        |RETURN b AS b""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 12") {
    assertRewritten(
      """LET x = 1, y = 2
        |RETURN x, y
        |
        |NEXT
        |
        |RETURN 1 + x AS a
        |UNION
        |RETURN 2 + y AS a
        |
        |NEXT
        |
        |LET b = a + 1
        |RETURN *
        |UNION
        |LET b = a + 2
        |RETURN *
        |
        |NEXT
        |
        |RETURN *""".stripMargin,
      """WITH 1 AS x, 2 AS y
        |WITH x AS x, y AS y
        |WITH count(*) AS `  UNNAMED2`, collect([x]) AS `  UNNAMED0`, collect([y]) AS `  UNNAMED1`
        |CALL (`  UNNAMED0`,`  UNNAMED1`,`  UNNAMED2`) {
        |  UNWIND range(0, `  UNNAMED2` - 1) AS `  UNNAMED3`
        |  WITH (`  UNNAMED0`[`  UNNAMED3`])[0] AS x, (`  UNNAMED1`[`  UNNAMED3`])[0] AS y
        |  RETURN 1 + x AS a
        |  UNION
        |  UNWIND range(0, `  UNNAMED2` - 1) AS `  UNNAMED3`
        |  WITH (`  UNNAMED0`[`  UNNAMED3`])[0] AS x, (`  UNNAMED1`[`  UNNAMED3`])[0] AS y
        |  RETURN 2 + y AS a
        |}
        |WITH count(*) AS `  UNNAMED5`, collect([a]) AS `  UNNAMED4`
        |CALL (`  UNNAMED4`,`  UNNAMED5`) {
        |  UNWIND range(0, `  UNNAMED5` - 1) AS `  UNNAMED6`
        |  WITH (`  UNNAMED4`[`  UNNAMED6`])[0] AS a
        |  WITH a AS a, a + 1 AS b
        |  RETURN a AS `  UNNAMED7`, b AS `  UNNAMED8`
        |  UNION
        |  UNWIND range(0, `  UNNAMED5` - 1) AS `  UNNAMED6`
        |  WITH (`  UNNAMED4`[`  UNNAMED6`])[0] AS a
        |  WITH a AS a, a + 2 AS b
        |  RETURN a AS `  UNNAMED7`, b AS `  UNNAMED8`
        |}
        |WITH `  UNNAMED7` AS a, `  UNNAMED8` AS b
        |RETURN a AS a, b AS b""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 13") {
    assertRewritten(
      """LET a = 1
        |RETURN a
        |
        |NEXT
        |
        |LET b = a + 1
        |RETURN a, b
        |UNION
        |LET b = a + 2
        |RETURN a, b
        |
        |NEXT
        |
        |LET c = b + 1
        |RETURN a, b, c""".stripMargin,
      """WITH 1 AS a
        |WITH a AS a
        |WITH count(*) AS `  UNNAMED1`, collect([a]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS a
        |  WITH a AS a, a + 1 AS b
        |  RETURN a AS `  UNNAMED3`, b AS `  UNNAMED4`
        |  UNION
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS a
        |  WITH a AS a, a + 2 AS b
        |  RETURN a AS `  UNNAMED3`, b AS `  UNNAMED4`
        |}
        |WITH `  UNNAMED4` AS b, `  UNNAMED3` AS a
        |WITH a AS a, b AS b, b + 1 AS c
        |RETURN a AS a, b AS b, c AS c""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 14") {
    assertRewritten(
      """USE neo1
        |MATCH (n:L1)
        |RETURN *
        |
        |NEXT
        |
        |MATCH (m:L1)
        |RETURN *
        |
        |NEXT
        |
        |MATCH(o:L2)
        |RETURN n.x + m.x + o.x""".stripMargin,
      """CALL () {
        |  USE `neo1`
        |  MATCH (n:L1)
        |  RETURN n AS n
        |}
        |MATCH (m:L1)
        |WITH m AS m, n AS n
        |MATCH (o:L2)
        |RETURN (n.x + m.x) + o.x AS `n.x + m.x + o.x`""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 15") {
    assertRewritten(
      """USE neo1
        |MATCH (n:L1)
        |RETURN *
        |
        |NEXT
        |
        |USE neo2
        |MATCH (m:L1)
        |RETURN *
        |
        |NEXT
        |
        |MATCH(o:L2)
        |RETURN n.x + m.x + o.x""".stripMargin,
      """CALL () {
        |  USE `neo1`
        |  MATCH (n:L1)
        |  RETURN n AS n
        |}
        |CALL (n) {
        |  USE `neo2`
        |  MATCH (m:L1)
        |  RETURN m AS `  UNNAMED1`, n AS `  UNNAMED0`
        |}
        |WITH `  UNNAMED0` AS n, `  UNNAMED1` AS m
        |MATCH (o:L2)
        |RETURN (n.x + m.x) + o.x AS `n.x + m.x + o.x`""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 16") {
    assertRewritten(
      """USE neo1
        |MATCH (n:L1)
        |RETURN *
        |
        |NEXT
        |
        |USE neo2
        |MATCH (m:L1)
        |RETURN *
        |
        |NEXT
        |
        |USE neo1
        |MATCH(o:L2)
        |RETURN n.x + m.x + o.x""".stripMargin,
      """CALL () {
        |  USE `neo1`
        |  MATCH (n:L1)
        |  RETURN n AS n
        |}
        |CALL (n) {
        |  USE `neo2`
        |  MATCH (m:L1)
        |  RETURN m AS `  UNNAMED1`, n AS `  UNNAMED0`
        |}
        |WITH `  UNNAMED0` AS n, `  UNNAMED1` AS m
        |CALL (n,m) {
        |  USE `neo1`
        |  MATCH (o:L2)
        |  RETURN (n.x + m.x) + o.x AS `n.x + m.x + o.x`
        |}
        |RETURN `n.x + m.x + o.x` AS `n.x + m.x + o.x`""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 17") {
    assertRewritten(
      """RETURN 1 AS a
        |
        |NEXT
        |
        |WITH *, 3 AS x
        |RETURN *, 2 AS b ORDER BY a, 4
        |
        |NEXT
        |
        |{ RETURN DISTINCT * }""".stripMargin,
      """WITH 1 AS a
        |WITH a AS a, 3 AS x
        |WITH a AS a, x AS x, 2 AS b
        |  ORDER BY a ASCENDING, 4 ASCENDING
        |RETURN DISTINCT a AS a, b AS b, x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 18") {
    assertRewritten(
      """RETURN 1 AS a
        |
        |NEXT
        |
        |WITH *, 3 AS x
        |RETURN *, 2 AS b ORDER BY a, 4
        |
        |NEXT
        |
        |{ RETURN DISTINCT * UNION RETURN DISTINCT * UNION RETURN DISTINCT * }""".stripMargin,
      """WITH 1 AS a
        |WITH a AS a, 3 AS x
        |WITH a AS a, x AS x, 2 AS b
        |  ORDER BY a ASCENDING, 4 ASCENDING
        |WITH count(*) AS `  UNNAMED3`, collect([a]) AS `  UNNAMED0`, collect([x]) AS `  UNNAMED1`, collect([b]) AS `  UNNAMED2`
        |CALL (`  UNNAMED0`,`  UNNAMED1`,`  UNNAMED2`,`  UNNAMED3`) {
        |  UNWIND range(0, `  UNNAMED3` - 1) AS `  UNNAMED4`
        |  WITH (`  UNNAMED0`[`  UNNAMED4`])[0] AS a, (`  UNNAMED1`[`  UNNAMED4`])[0] AS x, (`  UNNAMED2`[`  UNNAMED4`])[0] AS b
        |  RETURN DISTINCT a AS `  UNNAMED5`, b AS `  UNNAMED7`, x AS `  UNNAMED6`
        |  UNION
        |  UNWIND range(0, `  UNNAMED3` - 1) AS `  UNNAMED4`
        |  WITH (`  UNNAMED0`[`  UNNAMED4`])[0] AS a, (`  UNNAMED1`[`  UNNAMED4`])[0] AS x, (`  UNNAMED2`[`  UNNAMED4`])[0] AS b
        |  RETURN DISTINCT a AS `  UNNAMED5`, b AS `  UNNAMED7`, x AS `  UNNAMED6`
        |  UNION
        |  UNWIND range(0, `  UNNAMED3` - 1) AS `  UNNAMED4`
        |  WITH (`  UNNAMED0`[`  UNNAMED4`])[0] AS a, (`  UNNAMED1`[`  UNNAMED4`])[0] AS x, (`  UNNAMED2`[`  UNNAMED4`])[0] AS b
        |  RETURN DISTINCT a AS `  UNNAMED5`, b AS `  UNNAMED7`, x AS `  UNNAMED6`
        |}
        |RETURN `  UNNAMED5` AS a, `  UNNAMED6` AS x, `  UNNAMED7` AS b""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten 19") {
    assertRewritten(
      """RETURN 1 AS a
        |
        |NEXT
        |
        |WHEN a = 1 THEN RETURN 2 AS a
        |ELSE RETURN 3 AS a""".stripMargin,
      """WITH 1 AS a
        |WITH a AS a, CASE
        |  WHEN a = 1 THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (a,`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    RETURN 2 AS `  UNNAMED1`
        |  }
        |  RETURN `  UNNAMED1` AS `  UNNAMED1`
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    RETURN 3 AS `  UNNAMED1`
        |  }
        |  RETURN `  UNNAMED1` AS `  UNNAMED1`
        |}
        |RETURN `  UNNAMED1` AS a""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query with aggregation rewritten 1") {
    assertRewritten(
      """
      UNWIND [1,2,3] AS a
      RETURN a

      NEXT

      {
        UNWIND [1,2,3] AS x
        RETURN a + COUNT(x) AS x
        UNION ALL
        UNWIND [1,2,3] AS x
        RETURN a + SUM(x) AS x

        NEXT

        RETURN x
      }
      """.stripMargin,
      """UNWIND [1, 2, 3] AS a
        |WITH a AS a
        |WITH count(*) AS `  UNNAMED1`, collect([a]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS a
        |  UNWIND [1, 2, 3] AS x
        |  RETURN a + COUNT(x) AS x
        |  UNION ALL
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS a
        |  UNWIND [1, 2, 3] AS x
        |  RETURN a + SUM(x) AS x
        |}
        |WITH x AS x
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query with aggregation rewritten 2") {
    assertRewritten(
      """
      UNWIND [1,2,3] AS x
      RETURN x

      NEXT

      RETURN COUNT(x) AS x
      UNION ALL
      RETURN SUM(x) AS x

      NEXT

      RETURN COUNT(x) AS x
      UNION ALL
      RETURN SUM(x) AS x

      NEXT

      RETURN x
      """.stripMargin,
      """UNWIND [1, 2, 3] AS x
        |WITH x AS x
        |WITH count(*) AS `  UNNAMED1`, collect([x]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  RETURN COUNT(x) AS `  UNNAMED3`
        |  UNION ALL
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  RETURN SUM(x) AS `  UNNAMED3`
        |}
        |WITH count(*) AS `  UNNAMED5`, collect([`  UNNAMED3`]) AS `  UNNAMED4`
        |CALL (`  UNNAMED4`,`  UNNAMED5`) {
        |  UNWIND range(0, `  UNNAMED5` - 1) AS `  UNNAMED6`
        |  WITH (`  UNNAMED4`[`  UNNAMED6`])[0] AS x
        |  RETURN COUNT(x) AS `  UNNAMED7`
        |  UNION ALL
        |  UNWIND range(0, `  UNNAMED5` - 1) AS `  UNNAMED6`
        |  WITH (`  UNNAMED4`[`  UNNAMED6`])[0] AS x
        |  RETURN SUM(x) AS `  UNNAMED7`
        |}
        |WITH `  UNNAMED7` AS x
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query with aggregation rewritten 3") {
    assertRewritten(
      """
      UNWIND [1,2,3] AS a
      RETURN a

      NEXT

      {
        UNWIND [1,2,3] AS x
        RETURN COUNT(x) AS x
        UNION ALL
        UNWIND [1,2,3] AS x
        RETURN SUM(x) AS x

        NEXT

        RETURN x
      }
      """.stripMargin,
      """UNWIND [1, 2, 3] AS a
        |WITH a AS a
        |WITH count(*) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  UNWIND [1, 2, 3] AS x
        |  RETURN COUNT(x) AS x
        |  UNION ALL
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  UNWIND [1, 2, 3] AS x
        |  RETURN SUM(x) AS x
        |}
        |WITH x AS x
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Should not wrap on inner aggregation in subquery expression") {
    assertRewritten(
      """
      UNWIND [1,2,3] AS x
      RETURN x

      NEXT

      RETURN x
      UNION ALL
      RETURN COUNT{ RETURN sum(x) } AS x""".stripMargin,
      """UNWIND [1, 2, 3] AS x
        |WITH x AS x
        |CALL (x) {
        |  RETURN x AS `  UNNAMED0`
        |  UNION ALL
        |  RETURN COUNT { RETURN sum(x) AS `sum(x)` } AS `  UNNAMED0`
        |}
        |RETURN `  UNNAMED0` AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten with USE in UNION") {
    assertRewritten(
      """USE mega
        |MATCH (p0:Person)
        |WITH * ORDER BY p0.name ASC
        |RETURN head(collect(p0.name)) AS name0 ORDER BY name0
        |
        |NEXT
        |
        |USE mega
        |MATCH (p1:Person) WHERE name0 = p1.name
        |RETURN name0, p1.name
        |UNION
        |USE mega
        |MATCH (p1:Person) WHERE name0 = p1.name
        |RETURN name0, p1.name""".stripMargin,
      """CALL () {
        |  USE `mega`
        |  MATCH (p0:Person)
        |  WITH p0 AS p0
        |    ORDER BY p0.name ASCENDING
        |  RETURN head(collect(p0.name)) AS name0
        |    ORDER BY name0 ASCENDING
        |}
        |WITH count(*) AS `  UNNAMED1`, collect([name0]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  USE `mega`
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS name0
        |  MATCH (p1:Person)
        |    WHERE name0 = p1.name
        |  RETURN name0 AS `  UNNAMED3`, p1.name AS `  UNNAMED4`
        |  UNION
        |  USE `mega`
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS name0
        |  MATCH (p1:Person)
        |    WHERE name0 = p1.name
        |  RETURN name0 AS `  UNNAMED3`, p1.name AS `  UNNAMED4`
        |}
        |RETURN `  UNNAMED3` AS name0, `  UNNAMED4` AS `p1.name`""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten with USE in UNION ALL") {
    assertRewritten(
      """USE mega
        |MATCH (p0:Person)
        |WITH * ORDER BY p0.name ASC
        |RETURN head(collect(p0.name)) AS name0 ORDER BY name0
        |
        |NEXT
        |
        |USE mega
        |MATCH (p1:Person) WHERE name0 = p1.name
        |RETURN name0, p1.name
        |UNION ALL
        |USE mega
        |MATCH (p1:Person) WHERE name0 = p1.name
        |RETURN name0, p1.name""".stripMargin,
      """CALL () {
        |  USE `mega`
        |  MATCH (p0:Person)
        |  WITH p0 AS p0
        |    ORDER BY p0.name ASCENDING
        |  RETURN head(collect(p0.name)) AS name0
        |    ORDER BY name0 ASCENDING
        |}
        |CALL (name0) {
        |  USE `mega`
        |  MATCH (p1:Person)
        |    WHERE name0 = p1.name
        |  RETURN name0 AS `  UNNAMED0`, p1.name AS `  UNNAMED1`
        |  UNION ALL
        |  USE `mega`
        |  MATCH (p1:Person)
        |    WHERE name0 = p1.name
        |  RETURN name0 AS `  UNNAMED0`, p1.name AS `  UNNAMED1`
        |}
        |RETURN `  UNNAMED0` AS name0, `  UNNAMED1` AS `p1.name`""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query rewritten with UNION ALL") {
    assertRewritten(
      """UNWIND [1, 2, 3] AS x
        |RETURN x
        |
        |NEXT
        |
        |WITH x, x + 1 AS y
        |RETURN x, y
        |UNION ALL
        |WITH x, x + 1 AS y
        |RETURN x, y""".stripMargin,
      """UNWIND [1, 2, 3] AS x
        |WITH x AS x
        |CALL (x) {
        |  WITH x AS x, x + 1 AS y
        |  RETURN x AS `  UNNAMED0`, y AS `  UNNAMED1`
        |  UNION ALL
        |  WITH x AS x, x + 1 AS y
        |  RETURN x AS `  UNNAMED0`, y AS `  UNNAMED1`
        |}
        |RETURN `  UNNAMED0` AS x, `  UNNAMED1` AS y""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Expand all clauses and queries") {
    assertRewritten(
      """UNWIND [1, 2] AS x
        |RETURN x
        |
        |NEXT
        |
        |{
        |  WITH *, 3 AS b, 4 AS z
        |  RETURN *
        |  UNION
        |  WITH *, 3 AS b
        |  CALL (*) {
        |    WHEN b > 3 THEN {
        |      WITH *, 3 AS c
        |      RETURN x + c AS z
        |    }
        |    ELSE {
        |      RETURN x + 3 AS z
        |    }
        |  }
        |  RETURN *
        |}""".stripMargin,
      """UNWIND [1, 2] AS x
        |WITH x AS x
        |WITH count(*) AS `  UNNAMED1`, collect([x]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  WITH x AS x, 3 AS b, 4 AS z
        |  RETURN b AS `  UNNAMED4`, x AS `  UNNAMED3`, z AS `  UNNAMED5`
        |  UNION
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  WITH x AS x, 3 AS b
        |  CALL (b,x) {
        |    WITH b, x, CASE
        |  WHEN b > 3 THEN 0
        |  ELSE 1
        |END AS `  UNNAMED6`
        |    CALL (b,x,`  UNNAMED6`) {
        |      WITH `  UNNAMED6` AS `  UNNAMED6`
        |        WHERE `  UNNAMED6` = 0
        |      CALL (x) {
        |        WITH x AS x, 3 AS c
        |        RETURN x + c AS z
        |      }
        |      RETURN z AS z
        |      UNION ALL
        |      WITH `  UNNAMED6` AS `  UNNAMED6`
        |        WHERE `  UNNAMED6` = 1
        |      CALL (x) {
        |        RETURN x + 3 AS z
        |      }
        |      RETURN z AS z
        |    }
        |    RETURN z AS z
        |  }
        |  RETURN b AS `  UNNAMED4`, x AS `  UNNAMED3`, z AS `  UNNAMED5`
        |}
        |RETURN `  UNNAMED3` AS x, `  UNNAMED4` AS b, `  UNNAMED5` AS z""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("rewrites * in return") {
    assertRewritten(
      "match (n) return *",
      "match (n) return n",
      excludedVersions = Set()
    )

    assertRewritten(
      "match (n),(c) return *",
      "match (n),(c) return c,n",
      excludedVersions = Set()
    )

    assertRewritten(
      "match (n)-->(c) return *",
      "match (n)-->(c) return c,n",
      excludedVersions = Set()
    )

    assertRewritten(
      "match (n)-[r]->(c) return *",
      "match (n)-[r]->(c) return c,n,r",
      excludedVersions = Set()
    )

    assertRewritten(
      "create (n) return *",
      "create (n) return n",
      excludedVersions = Set()
    )

    assertRewritten(
      "match p = shortestPath((a)-[r*]->(x)) return *",
      "match p = shortestPath((a)-[r*]->(x)) return a,p,r,x",
      excludedVersions = Set()
    )

    assertRewritten(
      "match p=(a:Start)-->(b) return *",
      "match p=(a:Start)-->(b) return a, b, p",
      excludedVersions = Set()
    )

  }

  test("drops empty with * in single query") {
    assertRewritten(
      "WITH * RETURN 1 AS x",
      "RETURN 1 AS x"
    )
  }

  test("drops empty with * with incoming in single query") {
    assertRewritten(
      "WITH 1 AS x WITH * RETURN 1 AS x",
      "WITH 1 AS x RETURN 1 AS x"
    )
  }

  test("keeps with * with skip with incoming in single query") {
    assertRewritten(
      "WITH 1 AS x WITH * SKIP 0 RETURN 1 AS x",
      "WITH 1 AS x WITH x SKIP 0 RETURN 1 AS x",
      excludedVersions = Set()
    )
  }

  test("keeps with * with limit with incoming in single query") {
    assertRewritten(
      "WITH 1 AS x WITH * LIMIT 0 RETURN 1 AS x",
      "WITH 1 AS x WITH x LIMIT 0 RETURN 1 AS x",
      excludedVersions = Set()
    )
  }

  test("keeps with * with order by with incoming in single query") {
    assertRewritten(
      "WITH 1 AS x WITH * ORDER BY 0 RETURN 1 AS x",
      "WITH 1 AS x WITH x ORDER BY 0 RETURN 1 AS x",
      excludedVersions = Set()
    )
  }

  test("keeps with * with where with incoming in single query") {
    assertRewritten(
      "WITH 1 AS x WITH * WHERE true RETURN 1 AS x",
      "WITH 1 AS x WITH x WHERE true RETURN 1 AS x",
      excludedVersions = Set()
    )
  }

  test("drops empty with * in subquery call") {
    assertRewritten(
      "WITH 1 AS a CALL (*) { WITH * RETURN 1 AS x } RETURN *",
      "WITH 1 AS a CALL () { RETURN 1 AS x } RETURN a, x"
    )

    assertRewritten(
      "WITH 1 AS a CALL (*) { WITH 1 AS y WITH * RETURN 1 + y AS x } RETURN *",
      "WITH 1 AS a CALL () { WITH 1 AS y RETURN 1 + y AS x } RETURN a, x"
    )

    assertRewritten(
      "WITH 1 AS a CALL (*) { WITH 1 AS y WITH * RETURN 1 + y AS x } RETURN *",
      "WITH 1 AS a CALL () { WITH 1 AS y WITH y AS y RETURN 1 + y AS x } RETURN a, x",
      excludedVersions = Set(CypherVersion.Cypher25)
    )

    assertRewritten(
      "WITH 1 AS a CALL { WITH * WITH DISTINCT * RETURN 1 AS x } RETURN *",
      "WITH 1 AS a CALL { WITH a AS a WITH DISTINCT a AS a RETURN 1 AS x } RETURN a, x",
      excludedVersions = Set()
    )
    assertRewritten(
      "WITH 1 AS a CALL { WITH * WITH * RETURN 1 AS x } RETURN *",
      "WITH 1 AS a CALL { RETURN 1 AS x } RETURN a, x"
    )

    assertRewritten(
      "WITH 1 AS a CALL { WITH * WITH * RETURN 1 AS x } RETURN *",
      "WITH 1 AS a CALL { WITH a AS a WITH a AS a RETURN 1 AS x } RETURN a, x",
      excludedVersions = Set(CypherVersion.Cypher25)
    )
  }

  test("drops empty with * in subquery expression") {
    assertRewritten(
      "RETURN EXISTS { WITH * } AS y",
      "RETURN EXISTS { RETURN 1 AS `  UNNAMED0` } AS y"
    )

    assertRewritten(
      "RETURN EXISTS { WITH * UNION WITH * } AS y",
      "RETURN EXISTS { RETURN 1 AS `  UNNAMED0` UNION RETURN 1 AS `  UNNAMED1`} AS y",
      invalidSemantics = true
    )

    assertRewritten(
      "RETURN COUNT { WITH * } AS y",
      "RETURN COUNT { RETURN 1 AS `  UNNAMED0` } AS y"
    )

    assertRewritten(
      "RETURN COUNT { WITH * UNION WITH * } AS y",
      "RETURN COUNT { RETURN 1 AS `  UNNAMED0` UNION RETURN 1 AS `  UNNAMED1`} AS y",
      invalidSemantics = true
    )
  }

  test("rewrites * in return in subquery expression") {

    assertRewritten(
      "MATCH (a) WHERE EXISTS { MATCH (b) RETURN * } RETURN a",
      "MATCH (a) WHERE EXISTS { MATCH (b) RETURN b } RETURN a",
      excludedVersions = Set()
    )

    assertRewritten(
      "MATCH (a) WHERE COUNT { MATCH (b) RETURN * } = 3 RETURN a",
      "MATCH (a) WHERE COUNT { MATCH (b) RETURN b } = 3 RETURN a",
      excludedVersions = Set()
    )

    assertRewritten(
      "MATCH (a)-[r]->(b) WHERE EXISTS { MATCH (c)-[r1]->(d) RETURN * } RETURN a",
      "MATCH (a)-[r]->(b) WHERE EXISTS { MATCH (c)-[r1]->(d) RETURN c, d, r1 } RETURN a",
      excludedVersions = Set()
    )

    assertRewritten(
      "MATCH (a)-[r]->(b) WHERE COUNT { MATCH (c)-[r1]->(d) RETURN * } = 3 RETURN a",
      "MATCH (a)-[r]->(b) WHERE COUNT { MATCH (c)-[r1]->(d) RETURN c, d, r1 } = 3 RETURN a",
      excludedVersions = Set()
    )

  }

  test("rewrites * in importing with") {
    assertRewritten(
      """WITH 1 AS x
        |CALL {
        |  WITH *
        |  WITH x, 2 AS y
        |  CALL {
        |    WITH *
        |    WITH x + y AS z
        |    WITH *
        |    RETURN *
        |  }
        |  WITH *
        |  RETURN *
        |}
        |RETURN *""".stripMargin,
      """WITH 1 AS x
        |CALL {
        |  WITH x AS x
        |  WITH x AS x, 2 AS y
        |  CALL {
        |    WITH x AS x, y AS y
        |    WITH x + y AS z
        |    RETURN z AS z
        |  }
        |  RETURN x AS x, y AS y, z AS z
        |}
        |RETURN x AS x, y AS y, z AS z""".stripMargin,
      invalidSemantics = true
    )
  }

  test("rewrites * in importing with 2") {
    assertRewritten(
      """UNWIND [1, 2, 3] AS i
        |CALL {
        |  WITH *
        |  WITH *
        |  RETURN 1 AS z
        |}
        |RETURN *""".stripMargin,
      """UNWIND [1, 2, 3] AS i
        |CALL {
        |  RETURN 1 AS z
        |}
        |RETURN i AS i, z AS z""".stripMargin
    )
  }

  test("rewrites * in subquery") {
    assertRewritten(
      "match (n) call (*) { return n as res} return res",
      "match (n) call (n) { return n as res} return res",
      excludedVersions = Set()
    )

    assertRewritten(
      "match (n)-[r]-(m) call (*) { return n as res} return res",
      "match (n)-[r]-(m) call (n) { return n as res} return res",
      excludedVersions = Set()
    )

    assertRewritten(
      "match (n)-[r]-(m) call (n) { match (a) call (*) { return n as res} return res } return res",
      "match (n)-[r]-(m) call (n) { match (a) call (n) { return n as res} return res } return res",
      excludedVersions = Set()
    )

    assertRewritten(
      "match (n)-[r]-(m) call (n) { with 1 AS a call (*) { return n as res} return res } return res",
      "match (n)-[r]-(m) call (n) { with 1 AS a call (n) { return n as res} return res } return res",
      excludedVersions = Set()
    )

    assertRewritten(
      "match (n)-[r]-(m) call (*) { match (a) call (*) { return n as res} return res } return res",
      "match (n)-[r]-(m) call (n) { match (a) call (n) { return n as res} return res } return res",
      excludedVersions = Set()
    )

    assertRewritten(
      "match (n)-[r]-(m) call (*) { match (a) call (*) { return n as res} return res } return res",
      "match (n)-[r]-(m) call (n) { match (a) call (n) { return n as res} return res } return res",
      excludedVersions = Set()
    )

    assertRewritten(
      "with 1 as a, 2 as b, 3 as c match(n) with n as m, a, c call(*){return 1 as d} return a, c, d, m",
      "with 1 as a, 2 as b, 3 as c match(n) with n as m, a, c call(){return 1 as d} return a, c, d, m",
      excludedVersions = Set()
    )

    assertRewritten(
      "with 1 as a, 2 as b, 3 as c match(n) with n as m, a, c call(*){with 7 as b call(*){return 'hello' as res} return 1 as d} return a, c, d, m",
      "with 1 as a, 2 as b, 3 as c match(n) with n as m, a, c call(){with 7 as b call(){return 'hello' as res} return 1 as d} return a, c, d, m",
      excludedVersions = Set()
    )

    assertRewritten(
      "call (*) { with 1 as x return * } return *",
      "call () { with 1 as x return x } return x",
      excludedVersions = Set()
    )

    assertRewritten(
      "with 1 as a, 2 as b call (*) { with 1 as x return * } return *",
      "with 1 as a, 2 as b call () { with 1 as x return x } return a, b, x",
      excludedVersions = Set()
    )

    // This query is invalid as it is returning variables already declared in outer scope
    // This is handled elsewhere.
    assertRewritten(
      "with 1 as x call (*) { with 2 as y with x, y return * } return *",
      "with 1 as x call (x) { with 2 as y with x, y return y } return x, y",
      excludedVersions = Set()
    )

    assertRewritten(
      "with 1 as x call (*) { with 2 as y return * } return *",
      "with 1 as x call () { with 2 as y return y } return x, y",
      excludedVersions = Set()
    )

    assertRewritten(
      "call (*) { call (*) { call (*) { with 1 as x return * } return * } return * } return *",
      "call () { call () { call () { with 1 as x return x } return x } return x } return x",
      excludedVersions = Set()
    )
  }

  test("rewrites * in with") {
    assertRewritten(
      "match (n) with * return n",
      "match (n) with n return n",
      excludedVersions = Set(CypherVersion.Cypher25)
    )

    assertRewritten(
      "match (n) with * return n",
      "match (n) return n"
    )

    assertRewritten(
      "match (n),(c) with * return n",
      "match (n),(c) with c,n return n",
      excludedVersions = Set(CypherVersion.Cypher25)
    )

    assertRewritten(
      "match (n),(c) with * return n",
      "match (n),(c) return n"
    )

    assertRewritten(
      "match (n)-->(c) with * return n",
      "match (n)-->(c) with c,n return n",
      excludedVersions = Set(CypherVersion.Cypher25)
    )

    assertRewritten(
      "match (n)-->(c) with * return n",
      "match (n)-->(c) return n"
    )

    assertRewritten(
      "match (n)-[r]->(c) with * return n",
      "match (n)-[r]->(c) with c,n,r return n",
      excludedVersions = Set(CypherVersion.Cypher25)
    )

    assertRewritten(
      "match (n)-[r]->(c) with * return n",
      "match (n)-[r]->(c) return n"
    )

    assertRewritten(
      "match (n)-[r]->(c) with *, r.pi as x return n",
      "match (n)-[r]->(c) with c, n, r, r.pi as x return n",
      excludedVersions = Set(CypherVersion.Cypher25)
    )

    assertRewritten(
      "match (n)-[r]->(c) with *, r.pi as x return n",
      "match (n)-[r]->(c) with n, r, r.pi as x return n"
    )

    assertRewritten(
      "create (n) with * return n",
      "create (n) with n return n",
      excludedVersions = Set(CypherVersion.Cypher25)
    )

    assertRewritten(
      "create (n) with * return n",
      "create (n) return n"
    )

    assertRewritten(
      "match p = shortestPath((a)-[r*]->(x)) with * return p",
      "match p = shortestPath((a)-[r*]->(x)) with a,p,r,x return p",
      excludedVersions = Set(CypherVersion.Cypher25)
    )

    assertRewritten(
      "match p = shortestPath((a)-[r*]->(x)) with * return p",
      "match p = shortestPath((a)-[r*]->(x)) return p"
    )
  }

  test("symbol shadowing should be taken into account") {
    assertRewritten(
      "match (a),(x),(y) with a match (b) return *",
      "match (a),(x),(y) with a match (b) return a, b",
      excludedVersions = Set()
    )
  }

  test("keeps listed items during expand") {
    assertRewritten(
      "MATCH (n) WITH *, 1 AS b RETURN *",
      "MATCH (n) WITH n, 1 AS b RETURN b, n",
      excludedVersions = Set()
    )
  }

  test("braces") {
    assertRewritten("{ RETURN 1 AS x }", "RETURN 1 AS x")
  }

  test("nested braces") {
    assertRewritten("{ { { RETURN 1 AS x } } }", "RETURN 1 AS x")
  }

  test("braces with use") {
    assertRewritten("USE graph { RETURN 1 AS x }", "USE graph RETURN 1 AS x")
  }

  test("nested braces with use ") {
    assertRewritten(
      "USE graph { { USE innerGraph { RETURN 1 AS x } } }",
      "USE innerGraph RETURN 1 AS x"
    )
  }

  test("nested braces with use on all") {
    assertRewritten(
      "USE graph { USE innerGraph { USE innerInnerGraph { RETURN 1 AS x } } }",
      "USE innerInnerGraph RETURN 1 AS x"
    )
  }

  test("when in top level braces") {
    assertRewritten(
      """USE graph { WHEN true THEN RETURN 1 AS x } """.stripMargin,
      """WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when in top level braces with inner use") {
    assertRewritten(
      """USE graph { WHEN true THEN USE otherGraph RETURN 1 AS x } """.stripMargin,
      """WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    USE `otherGraph`
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when in top level braces with inner use tlb") {
    assertRewritten(
      """USE graph { WHEN true THEN USE otherGraph { RETURN 1 AS x } } """.stripMargin,
      """WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    USE `otherGraph`
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when in tlb with top level braces as arguments with inner union") {
    assertRewritten(
      """
        |{
        |   WHEN false THEN { RETURN 1 AS x UNION RETURN 2 AS x }
        |   WHEN false THEN { RETURN 1 AS x UNION RETURN 2 AS x }
        |   ELSE { RETURN 3 AS x UNION RETURN 3 AS x }
        | }
        | """.stripMargin,
      """WITH CASE
        |  WHEN false THEN 0
        |  WHEN false THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    CALL () {
        |      RETURN 1 AS x
        |      UNION
        |      RETURN 2 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    CALL () {
        |      RETURN 1 AS x
        |      UNION
        |      RETURN 2 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    CALL () {
        |      RETURN 3 AS x
        |      UNION
        |      RETURN 3 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when in tlb with top level braces as arguments with inner union all") {
    assertRewritten(
      """
        |{
        |   WHEN false THEN { RETURN 1 AS x UNION ALL RETURN 2 AS x }
        |   WHEN false THEN { RETURN 1 AS x UNION RETURN 2 AS x }
        |   ELSE { RETURN 3 AS x UNION ALL RETURN 3 AS x }
        | }
        | """.stripMargin,
      """WITH CASE
        |  WHEN false THEN 0
        |  WHEN false THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    CALL () {
        |      RETURN 1 AS x
        |      UNION ALL
        |      RETURN 2 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    CALL () {
        |      RETURN 1 AS x
        |      UNION
        |      RETURN 2 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    CALL () {
        |      RETURN 3 AS x
        |      UNION ALL
        |      RETURN 3 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when in tlb with top level braces as arguments with inner union with return all") {
    assertRewritten(
      """
        |{
        |   WHEN false THEN { RETURN 1 AS x UNION RETURN 2 AS x }
        |   WHEN false THEN { RETURN 1 AS x UNION WITH 2 AS x RETURN * }
        |   ELSE { RETURN 3 AS x UNION RETURN 3 AS x }
        | }
        | """.stripMargin,
      """WITH CASE
        |  WHEN false THEN 0
        |  WHEN false THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    CALL () {
        |      RETURN 1 AS x
        |      UNION
        |      RETURN 2 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    CALL () {
        |      RETURN 1 AS x
        |      UNION
        |      WITH 2 AS x
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    CALL () {
        |      RETURN 3 AS x
        |      UNION
        |      RETURN 3 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when with top level braces as arguments with inner union") {
    assertRewritten(
      """
        | WHEN false THEN { RETURN 1 AS x UNION RETURN 2 AS x }
        | WHEN false THEN { RETURN 1 AS x UNION RETURN 2 AS x }
        | ELSE { RETURN 3 AS x UNION RETURN 3 AS x }""".stripMargin,
      """WITH CASE
        |  WHEN false THEN 0
        |  WHEN false THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    CALL () {
        |      RETURN 1 AS x
        |      UNION
        |      RETURN 2 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    CALL () {
        |      RETURN 1 AS x
        |      UNION
        |      RETURN 2 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    CALL () {
        |      RETURN 3 AS x
        |      UNION
        |      RETURN 3 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when with top level braces as arguments") {
    assertRewritten(
      """WHEN false THEN { RETURN 1 AS x }
        |WHEN false THEN { RETURN 1 AS x }
        |ELSE { RETURN 3 AS x }""".stripMargin,
      """WITH CASE
        |  WHEN false THEN 0
        |  WHEN false THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    RETURN 3 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when with top level braces and graph as arguments") {
    assertRewritten(
      """
        |   WHEN false THEN USE innerGraph { RETURN 1 AS x }
        |   WHEN false THEN USE innerGraph { RETURN 2 AS x }
        |   ELSE { RETURN 3 AS x }""".stripMargin,
      """WITH CASE
        |  WHEN false THEN 0
        |  WHEN false THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    USE `innerGraph`
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    USE `innerGraph`
        |    RETURN 2 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    RETURN 3 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when in top level braces with top level braces as arguments") {
    assertRewritten(
      """
        |USE graph {
        |   WHEN false THEN USE innerGraph { RETURN 1 AS x }
        |   WHEN false THEN USE otherInnerGraph { RETURN 2 AS x }
        |   ELSE { RETURN 3 AS x }
        |}""".stripMargin,
      """WITH CASE
        |  WHEN false THEN 0
        |  WHEN false THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    USE `innerGraph`
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    USE `otherInnerGraph`
        |    RETURN 2 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    RETURN 3 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("union with top level braces as arguments") {
    assertRewritten(
      """
        | { RETURN 1 AS x }
        |UNION
        | { RETURN 2 AS x }""".stripMargin,
      """RETURN 1 AS x
        |UNION
        |RETURN 2 AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("union all and union") {
    assertRewritten(
      " { { RETURN 1 AS x UNION RETURN 2 AS x } UNION ALL { RETURN 3 AS x UNION RETURN 4 AS x } }",
      " CALL () { RETURN 1 AS x UNION RETURN 2 AS x } RETURN x AS x UNION ALL CALL () { RETURN 3 AS x UNION RETURN 4 AS x } RETURN x AS x"
    )
  }

  test("union and union all") {
    assertRewritten(
      " { { RETURN 1 AS x UNION ALL RETURN 2 AS x } UNION { RETURN 3 AS x UNION ALL RETURN 4 AS x } }",
      " CALL () { RETURN 1 AS x UNION ALL RETURN 2 AS x } RETURN x AS x UNION CALL () { RETURN 3 AS x UNION ALL RETURN 4 AS x } RETURN x AS x"
    )
  }

  test("nested union all and union") {
    assertRewritten(
      """ {
        |   {
        |      {
        |         RETURN 1 AS x
        |         UNION ALL
        |         RETURN 2 AS x
        |      }
        |      UNION
        |      RETURN 3 AS x
        |   }
        |   UNION ALL
        |   {
        |      RETURN 4 AS x
        |      UNION
        |      RETURN 5 AS x
        |   }
        | }""".stripMargin,
      """ CALL () {
        |   CALL () {
        |      RETURN 1 AS x
        |      UNION ALL
        |      RETURN 2 AS x
        |   }
        |   RETURN x AS x
        |   UNION
        |   RETURN 3 AS x
        |}
        |RETURN x AS x
        |UNION ALL
        |CALL () {
        |   RETURN 4 AS x
        |   UNION
        |   RETURN 5 AS x
        |}
        |RETURN x AS x""".stripMargin
    )
  }

  test("nested union and union all") {
    assertRewritten(
      """{ { RETURN 1 AS x UNION ALL RETURN 2 AS x }
        |UNION
        |{
        |  RETURN 3 AS x
        |  UNION ALL
        |  { RETURN 4 AS x UNION RETURN 5 AS x  }
        |} }""".stripMargin,
      """CALL () { RETURN 1 AS x UNION ALL RETURN 2 AS x } RETURN x AS x
        |UNION
        |CALL () {
        |  RETURN 3 AS x
        |  UNION ALL
        |  CALL () { RETURN 4 AS x UNION RETURN 5 AS x }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin
    )
  }

  test("union multiple arms") {
    assertRewritten(
      """{ RETURN 1 AS x }
        |UNION
        |{ RETURN 2 AS x }
        |UNION
        |{ RETURN 3 AS x }
        |UNION
        |{ RETURN 4 AS x }""".stripMargin,
      """RETURN 1 AS x
        |UNION
        |RETURN 2 AS x
        |UNION
        |RETURN 3 AS x
        |UNION
        |RETURN 4 AS x""".stripMargin
    )
  }

  test("wrapped union") {
    assertRewritten(
      """{ RETURN 1 AS x
        |UNION
        | RETURN 2 AS x }""".stripMargin,
      """RETURN 1 AS x
        |UNION
        |RETURN 2 AS x""".stripMargin
    )
  }

  test("wrapped union with use") {
    assertRewritten(
      """USE graph { RETURN 1 AS x
        |UNION
        | RETURN 2 AS x }""".stripMargin,
      """USE graph
        |RETURN 1 AS x
        |UNION
        |USE graph
        |RETURN 2 AS x""".stripMargin
    )
  }

  test("wrapped union multiple arms") {
    assertRewritten(
      """{ { RETURN 1 AS x }
        |UNION
        |{ RETURN 2 AS x }
        |UNION
        |{ RETURN 3 AS x }
        |UNION
        |{ RETURN 4 AS x } }""".stripMargin,
      """RETURN 1 AS x
        |UNION
        |RETURN 2 AS x
        |UNION
        |RETURN 3 AS x
        |UNION
        |RETURN 4 AS x""".stripMargin
    )
  }

  test("union no return") {
    assertRewritten(
      """{ CREATE (n) }
        |UNION
        |{ CREATE (n) }""".stripMargin,
      """CREATE (n)
        |UNION
        |CREATE (n)""".stripMargin
    )
  }

  test("union multiple arms no return") {
    assertRewritten(
      """{ CREATE (n) }
        |UNION
        |{ CREATE (n) }
        |UNION
        |{ CREATE (n) }
        |UNION
        |{ CREATE (n) }""".stripMargin,
      """CREATE (n)
        |UNION
        |CREATE (n)
        |UNION
        |CREATE (n)
        |UNION
        |CREATE (n)""".stripMargin
    )
  }

  test("union with use") {
    assertRewritten(
      """USE graphLeft { RETURN 1 AS x }
        |UNION
        |USE graphRight { RETURN 2 AS x }""".stripMargin,
      """USE `graphLeft`
        |RETURN 1 AS x
        |UNION
        |USE `graphRight`
        |RETURN 2 AS x""".stripMargin
    )
  }

  test("union multiple arms with some use") {
    assertRewritten(
      """USE graphLeft { RETURN 1 AS x }
        |UNION
        |{ RETURN 2 AS x }
        |UNION
        |USE graphLeft { RETURN 3 AS x }
        |UNION { RETURN 4 AS x }""".stripMargin,
      """USE `graphLeft`
        |RETURN 1 AS x
        |UNION
        |RETURN 2 AS x
        |UNION
        |USE `graphLeft`
        |RETURN 3 AS x
        |UNION
        |RETURN 4 AS x""".stripMargin
    )
  }

  test("wrapped union multiple arms with outerUse") {
    assertRewritten(
      """USE outerGraph {
        |   USE innerGraph { RETURN 1 AS x }
        |   UNION
        |   USE innerGraph { RETURN 2 AS x }
        |   UNION
        |   USE innerGraph { RETURN 3 AS x }
        |   UNION
        |   USE innerGraph { RETURN 4 AS x }
        |}""".stripMargin,
      """USE `innerGraph`
        |RETURN 1 AS x
        |UNION
        |USE `innerGraph`
        |RETURN 2 AS x
        |UNION
        |USE `innerGraph`
        |RETURN 3 AS x
        |UNION
        |USE `innerGraph`
        |RETURN 4 AS x""".stripMargin
    )
  }

  test("wrapped union multiple different arms with outerUse") {
    assertRewritten(
      """USE outerGraph {
        |   USE innerGraph { RETURN 1 AS x }
        |   UNION
        |   USE innerGraph { RETURN 2 AS x }
        |   UNION
        |   { RETURN 3 AS x }
        |   UNION
        |   RETURN 4 AS x
        |}""".stripMargin,
      """USE `innerGraph`
        |RETURN 1 AS x
        |UNION
        |USE `innerGraph`
        |RETURN 2 AS x
        |UNION
        |USE `outerGraph`
        |RETURN 3 AS x
        |UNION
        |USE `outerGraph`
        |RETURN 4 AS x""".stripMargin
    )
  }

  test("union with use and no return") {
    assertRewritten(
      """USE leftGraph { CREATE (n) }
        |UNION
        |USE rightGraph { CREATE (n) }""".stripMargin,
      """USE `leftGraph`
        |CREATE (n)
        |UNION
        |USE `rightGraph`
        |CREATE (n)""".stripMargin
    )
  }

  test("union with nested use in single query within union") {
    assertRewritten(
      """USE leftGraph {
        |MATCH (n) RETURN n
        |UNION ALL
        |USE rightGraph MATCH (n) RETURN n}
    """.stripMargin,
      """USE leftGraph
        |MATCH (n) RETURN n
        |UNION ALL
        |USE rightGraph
        |MATCH (n) RETURN n
        |""".stripMargin
    )
  }

  test("nested unions #1") {
    assertRewritten(
      """
        | { RETURN 1 AS x UNION RETURN 2 AS x }
        |UNION
        | { RETURN 3 AS x UNION RETURN 4 AS x }""".stripMargin,
      """
        | CALL () {
        |    RETURN 1 AS x
        |    UNION
        |    RETURN 2 AS x
        | }
        | RETURN x AS x
        |UNION
        | CALL () {
        |    RETURN 3 AS x
        |    UNION
        |    RETURN 4 AS x
        | }
        | RETURN x AS x""".stripMargin
    )
  }

  test("nested unions #2") {
    assertRewritten(
      """
        | { RETURN 1 AS x UNION RETURN 2 AS x UNION RETURN 3 AS x }
        |UNION
        | { { { { RETURN 3 AS x UNION RETURN 4 AS x } } } }""".stripMargin,
      """
        | CALL () {
        |    RETURN 1 AS x
        |    UNION
        |    RETURN 2 AS x
        |    UNION
        |    RETURN 3 AS x
        | }
        | RETURN x AS x
        |UNION
        | CALL () {
        |    RETURN 3 AS x
        |    UNION
        |    RETURN 4 AS x
        | }
        | RETURN x AS x""".stripMargin
    )
  }

  test("nested unions #3") {
    assertRewritten(
      """
        | { RETURN 1 AS x UNION { RETURN 10 AS x UNION ALL RETURN 10 AS x } UNION RETURN 3 AS x }
        |UNION
        | { { { { RETURN 3 AS x UNION RETURN 4 AS x } } } }""".stripMargin,
      """
        | CALL () {
        |    RETURN 1 AS x
        |    UNION
        |    CALL () {
        |       RETURN 10 AS x
        |       UNION ALL
        |       RETURN 10 AS x
        |    }
        |    RETURN x AS x
        |    UNION
        |    RETURN 3 AS x
        | }
        | RETURN x AS x
        |UNION
        | CALL () {
        |    RETURN 3 AS x
        |    UNION
        |    RETURN 4 AS x
        | }
        | RETURN x AS x""".stripMargin
    )
  }

  test("nested unions #4") {
    assertRewritten(
      """
        |USE outerGraph {
        | { RETURN 1 AS x UNION USE innerGraph { RETURN 10 AS x UNION ALL RETURN 10 AS x } UNION RETURN 3 AS x }
        |UNION
        | RETURN 3 AS x UNION RETURN 4 AS x
        |}""".stripMargin,
      """
        | CALL () {
        |    USE outerGraph
        |    RETURN 1 AS x
        |    UNION
        |    CALL () {
        |       USE innerGraph
        |       RETURN 10 AS x
        |       UNION ALL
        |       USE innerGraph
        |       RETURN 10 AS x
        |    }
        |    RETURN x AS x
        |    UNION
        |    USE outerGraph
        |    RETURN 3 AS x
        | }
        | RETURN x AS x
        |UNION
        | USE outerGraph
        | RETURN 3 AS x
        | UNION
        | USE outerGraph
        | RETURN 4 AS x""".stripMargin
    )
  }

  test("expand return items") {
    assertRewritten(
      """{
        |  WHEN true THEN {
        |    WITH 1 AS x
        |    RETURN *, 2 AS y
        |  }
        |}
        |UNION
        |RETURN 1 AS x, 2 AS y""".stripMargin,
      """WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    WITH 1 AS x
        |    RETURN x AS x, 2 AS y
        |  }
        |  RETURN x AS x, y AS y
        |}
        |RETURN x AS x, y AS y
        |UNION
        |RETURN 1 AS x, 2 AS y""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Importing with after foreach") {
    assertRewritten(
      """WITH 1 AS v1
        |FOREACH ( w1 IN [] |
        |  CREATE ()
        |)
        |WITH *, 1 AS v2
        |CALL {
        |  WITH *
        |  RETURN 1 AS v3
        |}
        |RETURN 2 AS v4""".stripMargin,
      """WITH 1 AS v1
        |FOREACH ( w1 IN [] |
        |  CREATE ()
        |)
        |WITH 1 AS v2
        |CALL {
        |  RETURN 1 AS v3
        |}
        |RETURN 2 AS v4""".stripMargin
    )
  }

  test("Importing with after foreach correlated") {
    assertRewritten(
      """WITH 1 AS v1
        |FOREACH ( w1 IN [] |
        |  CREATE ()
        |)
        |WITH *, 1 AS v2
        |CALL {
        |  WITH *
        |  RETURN 1 + v1 AS v3
        |}
        |RETURN 2 AS v4""".stripMargin,
      """WITH 1 AS v1
        |FOREACH ( w1 IN [] |
        |  CREATE ()
        |)
        |WITH v1 AS v1, 1 AS v2
        |CALL {
        |  WITH v1 AS v1
        |  RETURN 1 + v1 AS v3
        |}
        |RETURN 2 AS v4""".stripMargin
    )
  }

  test("Importing with after foreach correlated nested") {
    assertRewritten(
      """WITH 1 AS v1
        |FOREACH ( w1 IN [] |
        |  CREATE ()
        |)
        |WITH *, 1 AS v2
        |CALL {
        |  WITH *
        |  CALL {
        |    WITH *
        |    RETURN 1 + v1 AS v3
        |  }
        |  RETURN v3
        |}
        |RETURN 2 AS v4""".stripMargin,
      """WITH 1 AS v1
        |FOREACH ( w1 IN [] |
        |  CREATE ()
        |)
        |WITH v1 AS v1, 1 AS v2
        |CALL {
        |  WITH v1 AS v1
        |  CALL {
        |  WITH v1 AS v1
        |  RETURN 1 + v1 AS v3
        |  }
        |  RETURN v3 AS v3
        |}
        |RETURN 2 AS v4""".stripMargin
    )
  }

  test("when single branch rewritten") {
    assertRewritten(
      """WHEN true THEN RETURN 1 AS x""",
      """WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when unit single branch rewritten") {
    assertRewritten(
      """WHEN true THEN CREATE ()""",
      """WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    CREATE ()
        |  }
        |  FINISH
        |}
        |FINISH""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when in subquery and return all rewritten") {
    assertRewritten(
      """
        CALL (*) {
             WHEN true THEN
               CALL (*) {
                 RETURN 4 AS `x`
               }
               RETURN *
         }
         RETURN *""".stripMargin,
      """CALL () {
        |  WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |  CALL (`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL () {
        |      CALL () {
        |        RETURN 4 AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when single branch with else rewritten") {
    assertRewritten(
      """
        |   WHEN false THEN RETURN 1 AS x
        |   ELSE RETURN 2 AS x""".stripMargin,
      """WITH CASE
        |  WHEN false THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    RETURN 2 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when with simple subquery expression predicate") {
    assertRewritten(
      """
        |WHEN false THEN RETURN 1 AS x
        |WHEN EXISTS { RETURN 1 } THEN RETURN 2 AS x
        |ELSE RETURN 3 AS x""".stripMargin,
      """WITH CASE
        |  WHEN false THEN 0
        |  WHEN EXISTS { RETURN 1 AS `1` } THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    RETURN 2 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    RETURN 3 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when multiple branches with else rewritten") {
    assertRewritten(
      """WHEN false THEN RETURN 1 AS x
        |WHEN false THEN RETURN 2 AS x
        |WHEN false THEN RETURN 3 AS x
        |WHEN false THEN RETURN 4 AS x
        |ELSE RETURN 5 AS x""".stripMargin,
      """WITH CASE
        |  WHEN false THEN 0
        |  WHEN false THEN 1
        |  WHEN false THEN 2
        |  WHEN false THEN 3
        |  ELSE 4
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    RETURN 2 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    RETURN 3 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 3
        |  CALL () {
        |    RETURN 4 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 4
        |  CALL () {
        |    RETURN 5 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when enclosed in union in subquery") {
    assertRewritten(
      """MATCH (n)
        |CALL (*) {
        |  {
        |    WHEN false THEN RETURN 1 AS x
        |    ELSE RETURN n.prop AS x
        |  }
        |  UNION
        |  {
        |    WHEN false THEN RETURN 1 AS x
        |    ELSE RETURN n.prop AS x
        |  }
        |}
        |RETURN *""".stripMargin,
      """MATCH (n)
        |CALL (n) {
        |  WITH n AS n, CASE
        |  WHEN false THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |  CALL (n,`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL () {
        |      RETURN 1 AS x
        |    }
        |    RETURN x AS x
        |    UNION ALL
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 1
        |    CALL (n) {
        |      RETURN n.prop AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |  UNION
        |  WITH n AS n, CASE
        |  WHEN false THEN 0
        |  ELSE 1
        |END AS `  UNNAMED1`
        |  CALL (n,`  UNNAMED1`) {
        |    WITH `  UNNAMED1` AS `  UNNAMED1`
        |      WHERE `  UNNAMED1` = 0
        |    CALL () {
        |      RETURN 1 AS x
        |    }
        |    RETURN x AS x
        |    UNION ALL
        |    WITH `  UNNAMED1` AS `  UNNAMED1`
        |      WHERE `  UNNAMED1` = 1
        |    CALL (n) {
        |      RETURN n.prop AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN n AS n, x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when in subquery rewritten") {
    assertRewritten(
      """LET x = 1
        |CALL (x) {
        |   WHEN x < 0 THEN RETURN 1 + x AS y
        |   WHEN x < 1 THEN RETURN 2 + x AS y
        |   ELSE RETURN 3 + x AS y
        |}
        |RETURN y""".stripMargin,
      """WITH 1 AS x
        |CALL (x) {
        |  WITH x, CASE
        |  WHEN x < 0 THEN 0
        |  WHEN x < 1 THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |  CALL (x,`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL (x) {
        |      RETURN 1 + x AS y
        |    }
        |    RETURN y AS y
        |    UNION ALL
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 1
        |    CALL (x) {
        |      RETURN 2 + x AS y
        |    }
        |    RETURN y AS y
        |    UNION ALL
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 2
        |    CALL (x) {
        |      RETURN 3 + x AS y
        |    }
        |    RETURN y AS y
        |  }
        |  RETURN y AS y
        |}
        |RETURN y AS y""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when in subquery expression rewritten") {
    assertRewritten(
      """LET x = 1, b = 2
        |RETURN EXISTS {
        |   WHEN x < 0 THEN RETURN 1 + x AS y
        |   WHEN b < 1 THEN RETURN 2 AS y
        |   ELSE RETURN 3 + x AS y
        |} AS res""".stripMargin,
      """WITH 1 AS x, 2 AS b
        |RETURN EXISTS {
        |  WITH x, b, CASE
        |    WHEN x < 0 THEN 0
        |    WHEN b < 1 THEN 1
        |    ELSE 2
        |  END AS `  UNNAMED0`
        |  CALL (x,b,`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL (x) {
        |      RETURN 1 + x AS y
        |    }
        |    RETURN y AS y
        |    UNION ALL
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 1
        |    CALL () {
        |      RETURN 2 AS y
        |    }
        |    RETURN y AS y
        |    UNION ALL
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 2
        |    CALL (x) {
        |      RETURN 3 + x AS y
        |    }
        |    RETURN y AS y
        |  }
        |  RETURN y AS y
        |} AS res""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("when example with params rewritten") {
    assertRewritten(
      """
        |   WHEN $param < 1 THEN RETURN 1 AS res
        |   WHEN $param > 1 THEN RETURN 2 AS res
        |   ELSE RETURN 3 AS res""".stripMargin,
      """WITH CASE
        |  WHEN $param < 1 THEN 0
        |  WHEN $param > 1 THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    RETURN 1 AS res
        |  }
        |  RETURN res AS res
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    RETURN 2 AS res
        |  }
        |  RETURN res AS res
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    RETURN 3 AS res
        |  }
        |  RETURN res AS res
        |}
        |RETURN res AS res""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("chained subquery when") {
    assertRewritten(
      """CALL () {
        |  WHEN true THEN MATCH (n) RETURN n
        |}
        |CALL (n) {
        |  WHEN n.age > 40 THEN RETURN "old" AS x
        |  ELSE RETURN "young" AS x
        |}
        |RETURN x""".stripMargin,
      """CALL () {
        |  WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |  CALL (`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL () {
        |      MATCH (n)
        |      RETURN n AS n
        |    }
        |    RETURN n AS n
        |  }
        |  RETURN n AS n
        |}
        |CALL (n) {
        |  WITH n, CASE
        |  WHEN n.age > 40 THEN 0
        |  ELSE 1
        |END AS `  UNNAMED1`
        |  CALL (n,`  UNNAMED1`) {
        |    WITH `  UNNAMED1` AS `  UNNAMED1`
        |      WHERE `  UNNAMED1` = 0
        |    CALL () {
        |      RETURN "old" AS x
        |    }
        |    RETURN x AS x
        |    UNION ALL
        |    WITH `  UNNAMED1` AS `  UNNAMED1`
        |      WHERE `  UNNAMED1` = 1
        |    CALL () {
        |      RETURN "young" AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("By-table semantics in NEXT SUBQUERY NEXT UNION") {
    assertRewritten(
      """UNWIND [1, 2] AS x
        |RETURN x
        |
        |NEXT
        |
        |CALL (x) {
        |  LET z = 1
        |  RETURN *
        |
        |  NEXT
        |
        |  RETURN *, COUNT(x) AS y
        |  UNION ALL
        |  RETURN *, COUNT(x) AS y
        |}
        |RETURN *""".stripMargin,
      """UNWIND [1, 2] AS x
        |WITH x AS x
        |CALL (x) {
        |  WITH 1 AS z
        |  WITH z AS z
        |  WITH count(*) AS `  UNNAMED1`, collect([z]) AS `  UNNAMED0`
        |  CALL (x,`  UNNAMED0`,`  UNNAMED1`) {
        |    UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |    WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS z
        |    RETURN z AS `  UNNAMED3`, COUNT(x) AS `  UNNAMED4`
        |    UNION ALL
        |    UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |    WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS z
        |    RETURN z AS `  UNNAMED3`, COUNT(x) AS `  UNNAMED4`
        |  }
        |  RETURN `  UNNAMED3` AS z, `  UNNAMED4` AS y
        |}
        |RETURN x AS x, y AS y, z AS z""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("By-table semantics in NEXT WHEN") {
    assertRewritten(
      """UNWIND [1, 2] AS x
        |RETURN x
        |
        |NEXT
        |
        |WHEN true THEN {
        |  RETURN x, COUNT(x) AS y
        |  UNION ALL
        |  RETURN x, COUNT(x) AS y
        |}""".stripMargin,
      """UNWIND [1, 2] AS x
        |WITH x AS x
        |WITH x AS x, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (x,`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL (x) {
        |    CALL (x) {
        |      RETURN x AS `  UNNAMED1`, COUNT(x) AS `  UNNAMED2`
        |      UNION ALL
        |      RETURN x AS `  UNNAMED1`, COUNT(x) AS `  UNNAMED2`
        |    }
        |    RETURN `  UNNAMED1` AS `  UNNAMED1`, `  UNNAMED2` AS `  UNNAMED2`
        |  }
        |  RETURN `  UNNAMED1` AS `  UNNAMED1`, `  UNNAMED2` AS `  UNNAMED2`
        |}
        |RETURN `  UNNAMED1` AS x, `  UNNAMED2` AS y""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("By-table semantics in NEXT TLB NEXT") {
    assertRewritten(
      """UNWIND [1, 2, 3] AS a
        |RETURN a
        |
        |NEXT
        |
        |{
        |  UNWIND [1, 2, 3] AS x
        |  RETURN COUNT(x) AS x
        |  UNION ALL
        |  UNWIND [1, 2, 3] AS x
        |  RETURN SUM(x) AS x
        |
        |  NEXT
        |
        |  RETURN x
        |}""".stripMargin,
      """UNWIND [1, 2, 3] AS a
        |WITH a AS a
        |WITH count(*) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  UNWIND [1, 2, 3] AS x
        |  RETURN COUNT(x) AS x
        |  UNION ALL
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  UNWIND [1, 2, 3] AS x
        |  RETURN SUM(x) AS x
        |}
        |WITH x AS x
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("By-table semantics with flattenable NEXT and TLB") {
    assertRewritten(
      """{
        |  UNWIND [1,2,3] AS x
        |  RETURN COUNT(x) AS x
        |  UNION ALL
        |  UNWIND [1,2,3] AS x
        |  RETURN SUM(x) AS x
        |
        |  NEXT
        |  {
        |    RETURN COLLECT(x) AS coll
        |  }
        |}
        |NEXT
        |
        |RETURN coll""".stripMargin,
      """CALL () {
        |  UNWIND [1, 2, 3] AS x
        |  RETURN COUNT(x) AS x
        |  UNION ALL
        |  UNWIND [1, 2, 3] AS x
        |  RETURN SUM(x) AS x
        |}
        |WITH COLLECT(x) AS coll
        |RETURN coll AS coll""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Should always end with return") {
    assertRewritten(
      """UNWIND [1, 2] AS x
        |RETURN x
        |
        |NEXT
        |
        |{
        |   WITH *, 3 AS b, 4 AS z
        |   RETURN *
        |  UNION
        |   WITH *, 3 AS b
        |   CALL(*) {
        |    WHEN b > 3 THEN {
        |      WITH *, 3 AS c
        |      RETURN x + c AS z
        |    }
        |    ELSE {
        |      RETURN x + 3 AS z
        |    }
        |  }
        |  RETURN *
        |}""".stripMargin,
      """UNWIND [1, 2] AS x
        |WITH x AS x
        |WITH count(*) AS `  UNNAMED1`, collect([x]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  WITH x AS x, 3 AS b, 4 AS z
        |  RETURN b AS `  UNNAMED4`, x AS `  UNNAMED3`, z AS `  UNNAMED5`
        |  UNION
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  WITH x AS x, 3 AS b
        |  CALL (b,x) {
        |    WITH b, x, CASE
        |  WHEN b > 3 THEN 0
        |  ELSE 1
        |END AS `  UNNAMED6`
        |    CALL (b,x,`  UNNAMED6`) {
        |      WITH `  UNNAMED6` AS `  UNNAMED6`
        |        WHERE `  UNNAMED6` = 0
        |      CALL (x) {
        |        WITH x AS x, 3 AS c
        |        RETURN x + c AS z
        |      }
        |      RETURN z AS z
        |      UNION ALL
        |      WITH `  UNNAMED6` AS `  UNNAMED6`
        |        WHERE `  UNNAMED6` = 1
        |      CALL (x) {
        |        RETURN x + 3 AS z
        |      }
        |      RETURN z AS z
        |    }
        |    RETURN z AS z
        |  }
        |  RETURN b AS `  UNNAMED4`, x AS `  UNNAMED3`, z AS `  UNNAMED5`
        |}
        |RETURN `  UNNAMED3` AS x, `  UNNAMED4` AS b, `  UNNAMED5` AS z""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Passing return star in TLB") {
    assertRewritten(
      """UNWIND [1, 2] AS x
        |RETURN x
        |
        |NEXT
        |
        |{
        |  RETURN *
        |}
        |
        |NEXT
        |
        |RETURN COUNT(x)
        |UNION ALL
        |RETURN COUNT(x)""".stripMargin,
      """UNWIND [1, 2] AS x
        |WITH x AS x
        |WITH x AS x
        |WITH count(*) AS `  UNNAMED1`, collect([x]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  RETURN COUNT(x) AS `COUNT(x)`
        |  UNION ALL
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  RETURN COUNT(x) AS `COUNT(x)`
        |}
        |RETURN `COUNT(x)` AS `COUNT(x)`""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("No propagation through scope change") {
    assertRewritten(
      """UNWIND [1, 2] AS x
        |RETURN x
        |
        |NEXT
        |
        |CALL (x) {
        |  RETURN 1 AS z
        |
        |  NEXT
        |
        |  RETURN COUNT(x) AS y
        |  UNION ALL
        |  RETURN COUNT(x) AS y
        |}
        |RETURN *""".stripMargin,
      """UNWIND [1, 2] AS x
        |WITH x AS x
        |CALL (x) {
        |  WITH 1 AS z
        |  WITH count(*) AS `  UNNAMED0`
        |  CALL (x,`  UNNAMED0`) {
        |    UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |    RETURN COUNT(x) AS y
        |    UNION ALL
        |    UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |    RETURN COUNT(x) AS y
        |  }
        |  RETURN y AS y
        |}
        |RETURN x AS x, y AS y""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Include all explicits") {
    assertRewritten(
      """UNWIND [1, 2] AS x
        |RETURN x AS x
        |
        |NEXT
        |
        |WHEN true THEN {
        |  RETURN 1 AS z
        |
        |  NEXT
        |
        |  RETURN x AS x, COUNT(x) AS y
        |  UNION ALL
        |  RETURN x AS x, COUNT(x) AS y
        |}""".stripMargin,
      """UNWIND [1, 2] AS x
        |WITH x AS x
        |WITH x AS x, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (x,`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL (x) {
        |    WITH 1 AS z
        |    WITH count(*) AS `  UNNAMED3`
        |    CALL (x,`  UNNAMED3`) {
        |      UNWIND range(0, `  UNNAMED3` - 1) AS `  UNNAMED4`
        |      RETURN x AS `  UNNAMED1`, COUNT(x) AS `  UNNAMED2`
        |      UNION ALL
        |      UNWIND range(0, `  UNNAMED3` - 1) AS `  UNNAMED4`
        |      RETURN x AS `  UNNAMED1`, COUNT(x) AS `  UNNAMED2`
        |    }
        |    RETURN `  UNNAMED1` AS `  UNNAMED1`, `  UNNAMED2` AS `  UNNAMED2`
        |  }
        |  RETURN `  UNNAMED1` AS `  UNNAMED1`, `  UNNAMED2` AS `  UNNAMED2`
        |}
        |RETURN `  UNNAMED1` AS x, `  UNNAMED2` AS y""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Include all explicits 2 ") {
    assertRewritten(
      """UNWIND [1, 2] AS x
        |RETURN x
        |
        |NEXT
        |
        |WHEN true THEN {
        |  RETURN x, COUNT(x) AS y
        |  UNION ALL
        |  RETURN x, COUNT(x) AS y
        |}""".stripMargin,
      """UNWIND [1, 2] AS x
        |WITH x AS x
        |WITH x AS x, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (x,`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL (x) {
        |    CALL (x) {
        |      RETURN x AS `  UNNAMED1`, COUNT(x) AS `  UNNAMED2`
        |      UNION ALL
        |      RETURN x AS `  UNNAMED1`, COUNT(x) AS `  UNNAMED2`
        |    }
        |    RETURN `  UNNAMED1` AS `  UNNAMED1`, `  UNNAMED2` AS `  UNNAMED2`
        |  }
        |  RETURN `  UNNAMED1` AS `  UNNAMED1`, `  UNNAMED2` AS `  UNNAMED2`
        |}
        |RETURN `  UNNAMED1` AS x, `  UNNAMED2` AS y""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Include all explicits 3") {
    assertRewritten(
      """UNWIND [1, 2] AS x
        |RETURN x AS x
        |
        |NEXT
        |
        |WHEN true THEN {
        |  RETURN 1 AS z
        |
        |  NEXT
        |
        |  RETURN 9 AS x, COUNT(x) AS y
        |  UNION ALL
        |  RETURN 10 AS x, COUNT(x) AS y
        |}""".stripMargin,
      """UNWIND [1, 2] AS x
        |WITH x AS x
        |WITH x AS x, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (x,`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL (x) {
        |    WITH 1 AS z
        |    WITH count(*) AS `  UNNAMED3`
        |    CALL (x,`  UNNAMED3`) {
        |      UNWIND range(0, `  UNNAMED3` - 1) AS `  UNNAMED4`
        |      RETURN 9 AS `  UNNAMED1`, COUNT(x) AS `  UNNAMED2`
        |      UNION ALL
        |      UNWIND range(0, `  UNNAMED3` - 1) AS `  UNNAMED4`
        |      RETURN 10 AS `  UNNAMED1`, COUNT(x) AS `  UNNAMED2`
        |    }
        |    RETURN `  UNNAMED1` AS `  UNNAMED1`, `  UNNAMED2` AS `  UNNAMED2`
        |  }
        |  RETURN `  UNNAMED1` AS `  UNNAMED1`, `  UNNAMED2` AS `  UNNAMED2`
        |}
        |RETURN `  UNNAMED1` AS x, `  UNNAMED2` AS y""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("CREATE NEXT MATCH") {
    assertRewritten(
      """CREATE (:A {x: 1})-[:R]->(:B)
        |
        |NEXT
        |
        |MATCH (n:A)
        |RETURN n.x AS x""".stripMargin,
      """CREATE (:A {x: 1})-[:R]->(:B)
        |WITH count(NULL) AS `  UNNAMED0`
        |MATCH (n:A)
        |RETURN n.x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WHEN FINISH NEXT FINISH") {
    assertRewritten(
      """WHEN true THEN FINISH
        |ELSE FINISH
        |
        |NEXT
        |
        |FINISH
        |""".stripMargin,
      """WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    FINISH
        |  }
        |  FINISH
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    FINISH
        |  }
        |  FINISH
        |}
        |FINISH""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )

  }

  test("WHEN NEXT FINISH NEXT CALL WHEN EXPAND") {
    assertRewritten(
      """|WHEN false THEN RETURN 1 AS x
         |WHEN EXISTS { MATCH (:Person)-[:LOVES]->(x) RETURN x AS x } THEN RETURN 2 AS x
         |ELSE RETURN 3 AS x
         |
         |NEXT
         |
         |FINISH
         |
         |NEXT
         |
         |WITH 1 AS y
         |CALL (*) {
         |  WHEN true THEN CALL (*) {
         |    RETURN (4) + (y) AS x
         |  }
         |  RETURN *
         |  WHEN true THEN CALL (*) {
         |    RETURN (5) + (y) AS x
         |  }
         |  RETURN *
         |  ELSE CALL (*) {
         |    RETURN (6) + (y) AS x
         |  }
         |  RETURN *
         |}
         |RETURN *""".stripMargin,
      """WITH CASE
        |  WHEN false THEN 0
        |  WHEN EXISTS {
        |  MATCH (:Person)-[:LOVES]->(x)
        |  RETURN x AS x
        |} THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    RETURN 2 AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 2
        |  CALL () {
        |    RETURN 3 AS x
        |  }
        |  RETURN x AS x
        |}
        |WITH count(NULL) AS `  UNNAMED1`
        |WITH 1 AS y
        |CALL (y) {
        |  WITH y, CASE
        |  WHEN true THEN 0
        |  WHEN true THEN 1
        |  ELSE 2
        |END AS `  UNNAMED2`
        |  CALL (y,`  UNNAMED2`) {
        |    WITH `  UNNAMED2` AS `  UNNAMED2`
        |      WHERE `  UNNAMED2` = 0
        |    CALL (y) {
        |      CALL (y) {
        |        RETURN 4 + y AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |    UNION ALL
        |    WITH `  UNNAMED2` AS `  UNNAMED2`
        |      WHERE `  UNNAMED2` = 1
        |    CALL (y) {
        |      CALL (y) {
        |        RETURN 5 + y AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |    UNION ALL
        |    WITH `  UNNAMED2` AS `  UNNAMED2`
        |      WHERE `  UNNAMED2` = 2
        |    CALL (y) {
        |      CALL (y) {
        |        RETURN 6 + y AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x, y AS y""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Correct columns") {
    assertRewritten(
      """WHEN true THEN {
        |  {
        |    WHEN true THEN RETURN 9 AS x
        |  }
        |  UNION
        |  RETURN 5 AS x
        |}
        |ELSE RETURN 2 AS x""".stripMargin,
      """WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    CALL () {
        |      WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED1`
        |      CALL (`  UNNAMED1`) {
        |        WITH `  UNNAMED1` AS `  UNNAMED1`
        |          WHERE `  UNNAMED1` = 0
        |        CALL () {
        |          RETURN 9 AS x
        |        }
        |        RETURN x AS x
        |      }
        |      RETURN x AS x
        |      UNION
        |      RETURN 5 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |  UNION ALL
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 1
        |  CALL () {
        |    RETURN 2 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Return * after procedure call") {
    assertRewritten(
      """CALL db.labels() YIELD label
        |WITH count(*) AS c
        |CALL db.labels() YIELD label
        |RETURN *""".stripMargin,
      """CALL db.labels() YIELD label
        |WITH count(*) AS c
        |CALL db.labels() YIELD label
        |RETURN c, label""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Let expansion should not expand constant") {
    assertRewritten(
      """LET a = 1
        |CALL (a) {
        |  LET b = 2
        |  RETURN b
        |  UNION
        |  LET b = 2
        |  RETURN b
        |
        |  NEXT
        |
        |  RETURN a + b AS x
        |}
        |RETURN a, x""".stripMargin,
      """WITH 1 AS a
        |CALL (a) {
        |  CALL () {
        |    WITH 2 AS b
        |    RETURN b AS b
        |    UNION
        |    WITH 2 AS b
        |    RETURN b AS b
        |  }
        |  RETURN a + b AS x
        |}
        |RETURN a AS a, x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Let expansion should not expand constant subquery") {
    assertRewritten(
      """LET a = 1
        |RETURN EXISTS {
        |  LET b = 2
        |  RETURN b
        |  UNION
        |  LET b = 2
        |  RETURN b
        |
        |  NEXT
        |
        |  RETURN a + b AS x
        |} AS x""".stripMargin,
      """WITH 1 AS a
        |RETURN EXISTS {
        |  CALL () {
        |    WITH 2 AS b
        |    RETURN b AS b
        |    UNION
        |    WITH 2 AS b
        |    RETURN b AS b
        |  }
        |  RETURN a + b AS x
        |} AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("With star in NEXT non-reference") {
    assertRewritten(
      """RETURN 5 AS y
        |
        |NEXT
        |
        |WITH *
        |RETURN 6 AS x
        |UNION
        |RETURN 0 AS x""".stripMargin,
      """WITH 5 AS y
        |WITH count(*) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  RETURN 6 AS x
        |  UNION
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  RETURN 0 AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("With star in NEXT reference") {
    assertRewritten(
      """RETURN 5 AS y
        |
        |NEXT
        |
        |WITH *
        |RETURN y + 6 AS x
        |UNION
        |RETURN 0 AS x""".stripMargin,
      """WITH 5 AS y
        |WITH count(*) AS `  UNNAMED1`, collect([y]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS y
        |  RETURN y + 6 AS x
        |  UNION
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS y
        |  RETURN 0 AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("With star in NEXT descoped") {
    assertRewritten(
      """RETURN 5 AS y
        |
        |NEXT
        |
        |WITH 2 AS z
        |WITH *
        |WITH 1 AS y
        |RETURN y + 6 AS x
        |UNION
        |RETURN 0 AS x""".stripMargin,
      """WITH 5 AS y
        |WITH count(*) AS `  UNNAMED1`, collect([y]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS y
        |  WITH 2 AS z
        |  WITH 1 AS y
        |  RETURN y + 6 AS x
        |  UNION
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS y
        |  RETURN 0 AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("With star in NEXT with return item") {
    assertRewritten(
      """RETURN 5 AS y
        |
        |NEXT
        |
        |WITH *, 1 AS z
        |RETURN z + 6 AS x
        |UNION
        |RETURN 0 AS x""".stripMargin,
      """WITH 5 AS y
        |WITH count(*) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  WITH 1 AS z
        |  RETURN z + 6 AS x
        |  UNION
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  RETURN 0 AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("With star in NEXT with distinct") {
    assertRewritten(
      """RETURN 5 AS y
        |
        |NEXT
        |
        |WITH DISTINCT *
        |RETURN 6 AS x
        |UNION
        |RETURN 0 AS x""".stripMargin,
      """WITH 5 AS y
        |WITH count(*) AS `  UNNAMED1`, collect([y]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS y
        |  WITH DISTINCT y AS y
        |  RETURN 6 AS x
        |  UNION
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS y
        |  RETURN 0 AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("With star in NEXT with aggregation") {
    assertRewritten(
      """RETURN 5 AS y
        |
        |NEXT
        |
        |WITH *, count(*) AS cnt
        |RETURN 6 AS x
        |UNION
        |RETURN 0 AS x""".stripMargin,
      """WITH 5 AS y
        |WITH count(*) AS `  UNNAMED1`, collect([y]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS y
        |  WITH y AS y, count(*) AS cnt
        |  RETURN 6 AS x
        |  UNION
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS y
        |  RETURN 0 AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("With star pass through") {
    assertRewritten(
      """WITH 1 AS x
        |CALL {
        |  WITH x
        |  WITH x, 2 AS y
        |  WITH *, y
        |  RETURN y - x + 1 AS y
        |}
        |RETURN *""".stripMargin,
      """WITH 1 AS x
        |CALL {
        |  WITH x AS x
        |  WITH x AS x, 2 AS y
        |  WITH x AS x, y AS y
        |  RETURN (y - x) + 1 AS y
        |}
        |RETURN x AS x, y AS y""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting") {
    assertRewritten(
      """WITH true AS a
        |CALL (*) {
        |    WHEN false THEN {
        |        RETURN 7 AS x
        |        UNION
        |        RETURN 0 AS x
        |
        |      NEXT
        |
        |      RETURN 6 AS x
        |  }
        |}
        |RETURN x AS x""".stripMargin,
      """WITH true AS a
        |CALL () {
        |  WITH CASE
        |  WHEN false THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |  CALL (`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL () {
        |      CALL () {
        |        RETURN 7 AS x
        |        UNION
        |        RETURN 0 AS x
        |      }
        |      RETURN 6 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 2") {
    assertRewritten(
      """WITH true AS a
        |CALL (*) {
        |    WHEN a THEN {
        |      WHEN EXISTS { RETURN 5 AS x } THEN RETURN 7 AS x
        |      ELSE RETURN 6 AS x
        |    }
        |}
        |RETURN x AS x""".stripMargin,
      """WITH true AS a
        |CALL (a) {
        |  WITH a, CASE
        |  WHEN a THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |  CALL (a,`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL () {
        |      WITH CASE
        |  WHEN EXISTS { RETURN 5 AS x } THEN 0
        |  ELSE 1
        |END AS `  UNNAMED1`
        |      CALL (`  UNNAMED1`) {
        |        WITH `  UNNAMED1` AS `  UNNAMED1`
        |          WHERE `  UNNAMED1` = 0
        |        CALL () {
        |          RETURN 7 AS x
        |        }
        |        RETURN x AS x
        |        UNION ALL
        |        WITH `  UNNAMED1` AS `  UNNAMED1`
        |          WHERE `  UNNAMED1` = 1
        |        CALL () {
        |          RETURN 6 AS x
        |        }
        |        RETURN x AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 3") {
    assertRewritten(
      """WITH true AS a
        |CALL (*) {
        |        RETURN 5 AS x
        |
        |        NEXT
        |
        |          {
        |            WHEN a THEN RETURN 2 AS x
        |          }
        |          UNION
        |          RETURN 9 AS x
        |}
        |RETURN x AS x""".stripMargin,
      """WITH true AS a
        |CALL (a) {
        |  WITH 5 AS x
        |  WITH count(*) AS `  UNNAMED0`
        |  CALL (a,`  UNNAMED0`) {
        |    UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |    WITH a, CASE
        |  WHEN a THEN 0
        |  ELSE 1
        |END AS `  UNNAMED3`
        |    CALL (a,`  UNNAMED3`) {
        |      WITH `  UNNAMED3` AS `  UNNAMED3`
        |        WHERE `  UNNAMED3` = 0
        |      CALL () {
        |        RETURN 2 AS `  UNNAMED2`
        |      }
        |      RETURN `  UNNAMED2` AS `  UNNAMED2`
        |    }
        |    RETURN `  UNNAMED2` AS `  UNNAMED2`
        |    UNION
        |    UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |    RETURN 9 AS `  UNNAMED2`
        |  }
        |  RETURN `  UNNAMED2` AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 4") {
    assertRewritten(
      """{
        |  RETURN 0 AS x
        |
        |  NEXT
        |
        |  RETURN 3 AS x
        |}
        |UNION
        |RETURN 4 AS x
        |
        |NEXT
        |
        |RETURN 5 AS x""".stripMargin,
      """CALL () {
        |  WITH 0 AS x
        |  RETURN 3 AS x
        |  UNION
        |  RETURN 4 AS x
        |}
        |RETURN 5 AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 5") {
    assertRewritten(
      """{
        |  RETURN 0 AS x
        |
        |  NEXT
        |
        |  RETURN x + 1 AS x
        |}
        |UNION
        |RETURN 4 AS x
        |
        |NEXT
        |
        |RETURN 5 AS x""".stripMargin,
      """CALL () {
        |  WITH 0 AS x
        |  RETURN x + 1 AS x
        |  UNION
        |  RETURN 4 AS x
        |}
        |RETURN 5 AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 6") {
    assertRewritten(
      """RETURN 7 AS x
        |
        |NEXT
        |
        |WHEN true THEN  {
        |  {
        |    RETURN 5 AS y
        |
        |    NEXT
        |
        |    RETURN x + y + 1 AS x
        |  }
        |  UNION
        |  RETURN x + 2 AS x
        |}""".stripMargin,
      """WITH 7 AS x
        |WITH x AS x, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (x,`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL (x) {
        |    CALL (x) {
        |      WITH 5 AS y
        |      RETURN (x + y) + 1 AS `  UNNAMED1`
        |      UNION
        |      RETURN x + 2 AS `  UNNAMED1`
        |    }
        |    RETURN `  UNNAMED1` AS `  UNNAMED1`
        |  }
        |  RETURN `  UNNAMED1` AS `  UNNAMED1`
        |}
        |RETURN `  UNNAMED1` AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 7") {
    assertRewritten(
      """WITH true AS a
        |CALL (*) {
        |    WHEN true THEN {
        |      {
        |        RETURN 8 AS y
        |
        |        NEXT
        |
        |        RETURN 7 AS z
        |      }
        |
        |      NEXT
        |
        |      RETURN 0 AS x
        |      UNION
        |      RETURN CASE WHEN a THEN 5 END AS x
        |    }
        |}
        |RETURN x AS x""".stripMargin,
      """WITH true AS a
        |CALL (a) {
        |  WITH a, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |  CALL (a,`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL (a) {
        |      WITH 8 AS y
        |      WITH 7 AS z
        |      WITH count(*) AS `  UNNAMED1`
        |      CALL (a,`  UNNAMED1`) {
        |        UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |        RETURN 0 AS x
        |        UNION
        |        UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |        RETURN CASE
        |  WHEN a THEN 5
        |END AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // This fails semantic analysis but passes variableChecker and rewrites to valid Cypher
  test("NEXT query nesting 8") {
    assertRewritten(
      """WITH true AS a
        |CALL (*) {
        |    CALL (*) {
        |      RETURN CASE WHEN a THEN 8 END AS x
        |    }
        |    RETURN 2 AS x
        |
        |    NEXT
        |
        |    RETURN 1 AS x
        |}
        |RETURN x AS x""".stripMargin,
      """WITH true AS a
        |CALL (a) {
        |  CALL (a) {
        |    RETURN CASE WHEN a THEN 8 END AS x
        |  }
        |  WITH 2 AS x
        |  RETURN 1 AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 9") {
    assertRewritten(
      """WITH true AS a
        |CALL (*) {
        |
        |    RETURN 1 AS x
        |
        |    NEXT
        |
        |      {
        |        WHEN false THEN {
        |          WITH *
        |          RETURN 2 AS x
        |        }
        |      }
        |      UNION
        |      RETURN 8 AS x
        |}
        |RETURN x AS x""".stripMargin,
      """WITH true AS a
        |CALL () {
        |  WITH 1 AS x
        |  WITH count(*) AS `  UNNAMED0`
        |  CALL (`  UNNAMED0`) {
        |    UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |    WITH CASE
        |  WHEN false THEN 0
        |  ELSE 1
        |END AS `  UNNAMED3`
        |    CALL (`  UNNAMED3`) {
        |      WITH `  UNNAMED3` AS `  UNNAMED3`
        |        WHERE `  UNNAMED3` = 0
        |      CALL () {
        |        RETURN 2 AS `  UNNAMED2`
        |      }
        |      RETURN `  UNNAMED2` AS `  UNNAMED2`
        |    }
        |    RETURN `  UNNAMED2` AS `  UNNAMED2`
        |    UNION
        |    UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |    RETURN 8 AS `  UNNAMED2`
        |  }
        |  RETURN `  UNNAMED2` AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 10") {
    assertRewritten(
      """WITH true AS a
        |CALL (*) {
        |      RETURN 1 AS b
        |
        |      NEXT
        |
        |      {
        |        RETURN CASE   WHEN a THEN 6 END AS y
        |
        |        NEXT
        |
        |        RETURN 3 AS x
        |        UNION
        |        RETURN 4 AS x
        |      }
        |}
        |RETURN 1 AS x""".stripMargin,
      """WITH true AS a
        |CALL (a) {
        |  WITH 1 AS b
        |  WITH CASE WHEN a THEN 6 END AS y
        |  WITH count(*) AS `  UNNAMED0`
        |  CALL (`  UNNAMED0`) {
        |    UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |    RETURN 3 AS x
        |    UNION
        |    UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |    RETURN 4 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN 1 AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 11") {
    assertRewritten(
      """WITH true AS a
        |CALL (*) {
        |      WHEN a THEN {
        |              RETURN 5 AS b
        |              UNION
        |              RETURN 4 AS b
        |
        |            NEXT
        |
        |              RETURN 6 AS x
        |              UNION
        |              RETURN 4 AS x
        |      }
        |}
        |RETURN x AS x""".stripMargin,
      """WITH true AS a
        |CALL (a) {
        |  WITH a, CASE
        |  WHEN a THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |  CALL (a,`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL () {
        |      CALL () {
        |        RETURN 5 AS b
        |        UNION
        |        RETURN 4 AS b
        |      }
        |      WITH count(*) AS `  UNNAMED1`
        |      CALL (`  UNNAMED1`) {
        |        UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |        RETURN 6 AS x
        |        UNION
        |        UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |        RETURN 4 AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 12") {
    assertRewritten(
      """WITH true AS a
        |CALL (*) {
        |      RETURN 5 AS y
        |
        |      NEXT
        |
        |      {
        |          RETURN 7 AS b
        |          UNION
        |          RETURN 8 AS b
        |
        |        NEXT
        |
        |          RETURN 2 AS b
        |          UNION
        |          RETURN 4 AS b
        |      }
        |
        |    NEXT
        |
        |    RETURN 4 AS x
        |}
        |RETURN x AS x""".stripMargin,
      """WITH true AS a
        |CALL () {
        |  WITH 5 AS y
        |  WITH count(*) AS `  UNNAMED0`
        |  CALL (`  UNNAMED0`) {
        |    UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |    RETURN 7 AS b
        |    UNION
        |    UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |    RETURN 8 AS b
        |  }
        |  WITH count(*) AS `  UNNAMED2`
        |  CALL (`  UNNAMED2`) {
        |    UNWIND range(0, `  UNNAMED2` - 1) AS `  UNNAMED3`
        |    RETURN 2 AS b
        |    UNION
        |    UNWIND range(0, `  UNNAMED2` - 1) AS `  UNNAMED3`
        |    RETURN 4 AS b
        |  }
        |  RETURN 4 AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 13") {
    assertRewritten(
      """WITH 1 AS a
        |CALL (*) {
        |        RETURN 2 AS x
        |        UNION
        |        RETURN 3 AS x
        |
        |        NEXT
        |
        |        RETURN x + 4 AS x
        |        UNION
        |        RETURN x + a AS x
        |
        |        NEXT
        |
        |        RETURN x + a AS x
        |        UNION
        |        RETURN x + 7 AS x
        |}
        |RETURN x AS x""".stripMargin,
      """WITH 1 AS a
        |CALL (a) {
        |  CALL () {
        |    RETURN 2 AS x
        |    UNION
        |    RETURN 3 AS x
        |  }
        |  WITH count(*) AS `  UNNAMED1`, collect([x]) AS `  UNNAMED0`
        |  CALL (a,`  UNNAMED0`,`  UNNAMED1`) {
        |    UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |    WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |    RETURN x + 4 AS `  UNNAMED3`
        |    UNION
        |    UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |    WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |    RETURN x + a AS `  UNNAMED3`
        |  }
        |  WITH count(*) AS `  UNNAMED5`, collect([`  UNNAMED3`]) AS `  UNNAMED4`
        |  CALL (a,`  UNNAMED4`,`  UNNAMED5`) {
        |    UNWIND range(0, `  UNNAMED5` - 1) AS `  UNNAMED6`
        |    WITH (`  UNNAMED4`[`  UNNAMED6`])[0] AS x
        |    RETURN x + a AS `  UNNAMED7`
        |    UNION
        |    UNWIND range(0, `  UNNAMED5` - 1) AS `  UNNAMED6`
        |    WITH (`  UNNAMED4`[`  UNNAMED6`])[0] AS x
        |    RETURN x + 7 AS `  UNNAMED7`
        |  }
        |  RETURN `  UNNAMED7` AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 14") {
    assertRewritten(
      """WITH 1 AS a
        |CALL (*) {
        |        RETURN 2 AS x
        |        UNION
        |        RETURN 3 AS x
        |
        |        NEXT
        |
        |        FINISH
        |
        |        NEXT
        |
        |        RETURN a + 1 AS x
        |        UNION
        |        RETURN a + 7 AS x
        |}
        |RETURN x AS x""".stripMargin,
      """WITH 1 AS a
        |CALL (a) {
        |  CALL () {
        |    RETURN 2 AS x
        |    UNION
        |    RETURN 3 AS x
        |  }
        |  WITH count(NULL) AS `  UNNAMED0`
        |  CALL (a) {
        |    RETURN a + 1 AS x
        |    UNION
        |    RETURN a + 7 AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 15") {
    assertRewritten(
      """WITH 1 AS a
        |CALL (*) {
        |        WHEN true THEN {
        |          RETURN 2 AS x
        |          UNION
        |          RETURN 3 + a AS x
        |        }
        |
        |        NEXT
        |
        |        RETURN x + 4 AS x
        |        UNION
        |        RETURN x + a AS x
        |
        |        NEXT
        |
        |        WHEN true THEN {
        |          RETURN 5 AS x
        |          UNION
        |          RETURN x + a AS x
        |        }
        |
        |        NEXT
        |
        |        RETURN x + 6 AS x
        |        UNION
        |        RETURN x + a AS x
        |
        |        NEXT
        |
        |        WHEN true THEN {
        |          RETURN 7 AS x
        |          UNION
        |          RETURN x + a AS x
        |        }
        |}
        |RETURN x AS x""".stripMargin,
      """WITH 1 AS a
        |CALL (a) {
        |  WITH a AS a, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |  CALL (a,`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL (a) {
        |      CALL (a) {
        |        RETURN 2 AS x
        |        UNION
        |        RETURN 3 + a AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |  }
        |  WITH count(*) AS `  UNNAMED2`, collect([x]) AS `  UNNAMED1`
        |  CALL (a,`  UNNAMED1`,`  UNNAMED2`) {
        |    UNWIND range(0, `  UNNAMED2` - 1) AS `  UNNAMED3`
        |    WITH (`  UNNAMED1`[`  UNNAMED3`])[0] AS x
        |    RETURN x + 4 AS `  UNNAMED4`
        |    UNION
        |    UNWIND range(0, `  UNNAMED2` - 1) AS `  UNNAMED3`
        |    WITH (`  UNNAMED1`[`  UNNAMED3`])[0] AS x
        |    RETURN x + a AS `  UNNAMED4`
        |  }
        |  WITH `  UNNAMED4` AS x, a AS a
        |  WITH x AS x, a AS a, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED5`
        |  CALL (x,a,`  UNNAMED5`) {
        |    WITH `  UNNAMED5` AS `  UNNAMED5`
        |      WHERE `  UNNAMED5` = 0
        |    CALL (x,a) {
        |      CALL (x,a) {
        |        RETURN 5 AS `  UNNAMED6`
        |        UNION
        |        RETURN x + a AS `  UNNAMED6`
        |      }
        |      RETURN `  UNNAMED6` AS `  UNNAMED6`
        |    }
        |    RETURN `  UNNAMED6` AS `  UNNAMED6`
        |  }
        |  WITH `  UNNAMED6` AS x
        |  WITH count(*) AS `  UNNAMED8`, collect([x]) AS `  UNNAMED7`
        |  CALL (a,`  UNNAMED7`,`  UNNAMED8`) {
        |    UNWIND range(0, `  UNNAMED8` - 1) AS `  UNNAMED9`
        |    WITH (`  UNNAMED7`[`  UNNAMED9`])[0] AS x
        |    RETURN x + 6 AS `  UNNAMED10`
        |    UNION
        |    UNWIND range(0, `  UNNAMED8` - 1) AS `  UNNAMED9`
        |    WITH (`  UNNAMED7`[`  UNNAMED9`])[0] AS x
        |    RETURN x + a AS `  UNNAMED10`
        |  }
        |  WITH `  UNNAMED10` AS x, a AS a
        |  WITH x AS x, a AS a, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED11`
        |  CALL (x,a,`  UNNAMED11`) {
        |    WITH `  UNNAMED11` AS `  UNNAMED11`
        |      WHERE `  UNNAMED11` = 0
        |    CALL (x,a) {
        |      CALL (x,a) {
        |        RETURN 7 AS `  UNNAMED12`
        |        UNION
        |        RETURN x + a AS `  UNNAMED12`
        |      }
        |      RETURN `  UNNAMED12` AS `  UNNAMED12`
        |    }
        |    RETURN `  UNNAMED12` AS `  UNNAMED12`
        |  }
        |  RETURN `  UNNAMED12` AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 16") {
    assertRewritten(
      """WITH 1 AS a
        |CALL (*) {
        |        WHEN true THEN {
        |          RETURN 2 AS x
        |          UNION
        |          RETURN 3 + a AS x
        |        }
        |
        |        NEXT
        |
        |        RETURN x + 4 AS y
        |        UNION
        |        RETURN x + a AS y
        |
        |        NEXT
        |
        |        WHEN true THEN {
        |          RETURN 2 AS x, y
        |          UNION
        |          RETURN 1 + a AS x, y
        |        }
        |}
        |RETURN x, y""".stripMargin,
      """WITH 1 AS a
        |CALL (a) {
        |  WITH a AS a, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |  CALL (a,`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL (a) {
        |      CALL (a) {
        |        RETURN 2 AS x
        |        UNION
        |        RETURN 3 + a AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |  }
        |  WITH count(*) AS `  UNNAMED2`, collect([x]) AS `  UNNAMED1`
        |  CALL (a,`  UNNAMED1`,`  UNNAMED2`) {
        |    UNWIND range(0, `  UNNAMED2` - 1) AS `  UNNAMED3`
        |    WITH (`  UNNAMED1`[`  UNNAMED3`])[0] AS x
        |    RETURN x + 4 AS y
        |    UNION
        |    UNWIND range(0, `  UNNAMED2` - 1) AS `  UNNAMED3`
        |    WITH (`  UNNAMED1`[`  UNNAMED3`])[0] AS x
        |    RETURN x + a AS y
        |  }
        |  WITH y AS y, a AS a, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED4`
        |  CALL (y,a,`  UNNAMED4`) {
        |    WITH `  UNNAMED4` AS `  UNNAMED4`
        |      WHERE `  UNNAMED4` = 0
        |    CALL (y,a) {
        |      CALL (y,a) {
        |        RETURN 2 AS `  UNNAMED5`, y AS `  UNNAMED6`
        |        UNION
        |        RETURN 1 + a AS `  UNNAMED5`, y AS `  UNNAMED6`
        |      }
        |      RETURN `  UNNAMED5` AS `  UNNAMED5`, `  UNNAMED6` AS `  UNNAMED6`
        |    }
        |    RETURN `  UNNAMED5` AS `  UNNAMED5`, `  UNNAMED6` AS `  UNNAMED6`
        |  }
        |  RETURN `  UNNAMED5` AS x, `  UNNAMED6` AS y
        |}
        |RETURN x AS x, y AS y""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 17") {
    assertRewritten(
      """WITH true AS a, 2 AS b
        |CALL (*) {
        |    RETURN 7 AS y
        |
        |    NEXT
        |
        |    RETURN 2 AS y, 3 AS z
        |
        |    NEXT
        |
        |    WHEN a THEN { RETURN 6 + b + z AS x }
        |}
        |RETURN x AS x""".stripMargin,
      """WITH true AS a, 2 AS b
        |CALL (a,b) {
        |  WITH 7 AS y
        |  WITH 2 AS y, 3 AS z
        |  WITH a, b, z AS z, CASE
        |  WHEN a THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |  CALL (a,b,z,`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL (b,z) {
        |      RETURN (6 + b) + z AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 18") {
    assertRewritten(
      """RETURN 1 AS y
        |
        |NEXT
        |
        |{
        |  RETURN 4 AS x
        |  UNION ALL
        |  RETURN 1 AS x
        |}
        |UNION
        |RETURN 9 AS x""".stripMargin,
      """WITH 1 AS y
        |WITH count(*) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  CALL () {
        |    RETURN 4 AS x
        |    UNION ALL
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |  UNION
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  RETURN 9 AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 19") {
    assertRewritten(
      """RETURN 3 AS y
        |
        |NEXT
        |
        |WHEN true THEN {
        |  RETURN 7 AS z
        |
        |  NEXT
        |
        |  {
        |    RETURN 9 AS q
        |
        |      NEXT
        |
        |      WHEN true THEN { RETURN 0 AS y }
        |  }
        |}
        |
        |NEXT
        |
        |RETURN 3 AS x""".stripMargin,
      """WITH 3 AS y
        |WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  WITH `  UNNAMED0` AS `  UNNAMED0`
        |    WHERE `  UNNAMED0` = 0
        |  CALL () {
        |    WITH 7 AS z
        |    WITH 9 AS q
        |    WITH CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED2`
        |    CALL (`  UNNAMED2`) {
        |      WITH `  UNNAMED2` AS `  UNNAMED2`
        |        WHERE `  UNNAMED2` = 0
        |      CALL () {
        |        RETURN 0 AS `  UNNAMED1`
        |      }
        |      RETURN `  UNNAMED1` AS `  UNNAMED1`
        |    }
        |    WITH `  UNNAMED1` AS `  UNNAMED1`
        |    RETURN `  UNNAMED1` AS `  UNNAMED1`
        |  }
        |  RETURN `  UNNAMED1` AS `  UNNAMED1`
        |}
        |WITH `  UNNAMED1` AS y
        |RETURN 3 AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 20") {
    assertRewritten(
      """RETURN 1 AS x
        |
        |NEXT
        |
        |RETURN 2 AS x
        |UNION
        |{
        |  RETURN 3 AS y
        |
        |  NEXT
        |
        |  {
        |    RETURN 4 AS z
        |
        |    NEXT
        |
        |    RETURN 5 AS x
        |  }
        |}""".stripMargin,
      """WITH 1 AS x
        |WITH count(*) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  RETURN 2 AS `  UNNAMED2`
        |  UNION
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  WITH 3 AS y
        |  WITH 4 AS z
        |  WITH 5 AS `  UNNAMED2`
        |  RETURN `  UNNAMED2` AS `  UNNAMED2`
        |}
        |RETURN `  UNNAMED2` AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 21") {
    assertRewritten(
      """WITH true AS a
        |    CALL (*) {
        |      {
        |      WHEN a THEN {
        |        RETURN 6 AS x
        |          UNION
        |        CALL (*) {
        |          RETURN 0 AS x
        |        }
        |        RETURN *
        |        }
        |      WHEN
        |        EXISTS {
        |          {
        |          RETURN
        |            CASE
        |              WHEN a THEN 2
        |            END AS x
        |            UNION
        |          CALL (*) {
        |            {
        |            WHEN a THEN
        |              RETURN
        |                CASE
        |                  WHEN a THEN 2
        |                END AS x
        |            WHEN false THEN
        |              RETURN
        |                CASE
        |                  WHEN a THEN 8
        |                END AS x
        |            WHEN
        |              EXISTS {
        |                RETURN 6 AS x
        |              }
        |              THEN
        |              RETURN 3 AS x
        |            ELSE
        |              RETURN 7 AS x
        |            }
        |          }
        |          RETURN *
        |          }
        |        }
        |        THEN
        |        CALL (*) {
        |          CALL (*) {
        |            RETURN 6 AS x
        |          }
        |          RETURN *
        |        }
        |        RETURN *
        |      ELSE
        |        RETURN
        |          CASE
        |            WHEN a THEN 4
        |          END AS x }
        |    }
        |    RETURN x AS x""".stripMargin,
      """WITH true AS a
        |CALL (a) {
        |  WITH a AS a, CASE
        |  WHEN a THEN 0
        |  WHEN EXISTS {
        |  RETURN CASE
        |    WHEN a THEN 2
        |  END AS x
        |  UNION
        |  CALL (a) {
        |    WITH a AS a, CASE
        |    WHEN a THEN 0
        |    WHEN false THEN 1
        |    WHEN EXISTS { RETURN 6 AS x } THEN 2
        |    ELSE 3
        |  END AS `  UNNAMED1`
        |    CALL (a,`  UNNAMED1`) {
        |      WITH `  UNNAMED1` AS `  UNNAMED1`
        |        WHERE `  UNNAMED1` = 0
        |      CALL (a) {
        |        RETURN CASE
        |    WHEN a THEN 2
        |  END AS x
        |      }
        |      RETURN x AS x
        |      UNION ALL
        |      WITH `  UNNAMED1` AS `  UNNAMED1`
        |        WHERE `  UNNAMED1` = 1
        |      CALL (a) {
        |        RETURN CASE
        |    WHEN a THEN 8
        |  END AS x
        |      }
        |      RETURN x AS x
        |      UNION ALL
        |      WITH `  UNNAMED1` AS `  UNNAMED1`
        |        WHERE `  UNNAMED1` = 2
        |      CALL () {
        |        RETURN 3 AS x
        |      }
        |      RETURN x AS x
        |      UNION ALL
        |      WITH `  UNNAMED1` AS `  UNNAMED1`
        |        WHERE `  UNNAMED1` = 3
        |      CALL () {
        |        RETURN 7 AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |} THEN 1
        |  ELSE 2
        |END AS `  UNNAMED0`
        |  CALL (a,`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL () {
        |      CALL () {
        |        RETURN 6 AS x
        |        UNION
        |        CALL () {
        |          RETURN 0 AS x
        |        }
        |        RETURN x AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |    UNION ALL
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 1
        |    CALL () {
        |      CALL () {
        |        CALL () {
        |          RETURN 6 AS x
        |        }
        |        RETURN x AS x
        |      }
        |      RETURN x AS x
        |    }
        |    RETURN x AS x
        |    UNION ALL
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 2
        |    CALL (a) {
        |      RETURN CASE
        |  WHEN a THEN 4
        |END AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 22") {
    assertRewritten(
      """RETURN 5 AS x1
        |
        |NEXT
        |
        |RETURN 9 AS x1
        |UNION
        |{
        |  RETURN 7 AS x1
        |}
        |
        |NEXT
        |
        |{
        |  RETURN *, count(*) AS x2
        |
        |  NEXT
        |
        |  RETURN 4 AS x
        |}""".stripMargin,
      """WITH 5 AS x1
        |WITH count(*) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  RETURN 9 AS `  UNNAMED2`
        |  UNION
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  RETURN 7 AS `  UNNAMED2`
        |}
        |WITH `  UNNAMED2` AS x1
        |WITH x1 AS x1, count(*) AS x2
        |WITH 4 AS x
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 23") {
    assertRewritten(
      """RETURN 2 AS u1
        |
        |NEXT
        |
        |RETURN 6 AS u1
        |UNION
        |RETURN 9 AS u1
        |
        |NEXT
        |
        |{
        |  WHEN true THEN WITH *
        |    WHERE true = true
        |  RETURN 1 AS u2
        |}
        |UNION
        |WITH * WHERE true = true
        |RETURN 2 AS u2
        |
        |NEXT
        |
        |RETURN 9 AS x""".stripMargin,
      """WITH 2 AS u1
        |WITH count(*) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  RETURN 6 AS `  UNNAMED2`
        |  UNION
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  RETURN 9 AS `  UNNAMED2`
        |}
        |WITH count(*) AS `  UNNAMED4`, collect([`  UNNAMED2`]) AS `  UNNAMED3`
        |CALL (`  UNNAMED3`,`  UNNAMED4`) {
        |  UNWIND range(0, `  UNNAMED4` - 1) AS `  UNNAMED5`
        |  WITH (`  UNNAMED3`[`  UNNAMED5`])[0] AS u1
        |  WITH u1 AS u1, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED6`
        |  CALL (u1,`  UNNAMED6`) {
        |    WITH `  UNNAMED6` AS `  UNNAMED6`
        |      WHERE `  UNNAMED6` = 0
        |    CALL (u1) {
        |      WITH u1
        |        WHERE true = true
        |      RETURN 1 AS u2
        |    }
        |    RETURN u2 AS u2
        |  }
        |  RETURN u2 AS u2
        |  UNION
        |  UNWIND range(0, `  UNNAMED4` - 1) AS `  UNNAMED5`
        |  WITH (`  UNNAMED3`[`  UNNAMED5`])[0] AS u1
        |  WITH u1 AS u1
        |    WHERE true = true
        |  RETURN 2 AS u2
        |}
        |RETURN 9 AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 24") {
    assertRewritten(
      """RETURN 8 AS u1
        |
        |NEXT
        |
        |RETURN 5 AS u1
        |UNION
        |RETURN 3 AS u1
        |
        |NEXT
        |
        |RETURN 2 AS x, u1
        |UNION
        |{
        |    {
        |      WHEN true THEN
        |        WITH *
        |          WHERE true = true
        |        RETURN 0 AS x, u1
        |    }
        |    UNION
        |    WITH *
        |      WHERE true = true
        |    RETURN 3 AS x, u1
        |}""".stripMargin,
      """WITH 8 AS u1
        |WITH count(*) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`) {
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  RETURN 5 AS `  UNNAMED2`
        |  UNION
        |  UNWIND range(0, `  UNNAMED0` - 1) AS `  UNNAMED1`
        |  RETURN 3 AS `  UNNAMED2`
        |}
        |WITH count(*) AS `  UNNAMED4`, collect([`  UNNAMED2`]) AS `  UNNAMED3`
        |CALL (`  UNNAMED3`,`  UNNAMED4`) {
        |  UNWIND range(0, `  UNNAMED4` - 1) AS `  UNNAMED5`
        |  WITH (`  UNNAMED3`[`  UNNAMED5`])[0] AS u1
        |  RETURN 2 AS `  UNNAMED6`, u1 AS `  UNNAMED7`
        |  UNION
        |  UNWIND range(0, `  UNNAMED4` - 1) AS `  UNNAMED5`
        |  WITH (`  UNNAMED3`[`  UNNAMED5`])[0] AS u1
        |  CALL (u1) {
        |    WITH u1 AS u1, CASE
        |  WHEN true THEN 0
        |  ELSE 1
        |END AS `  UNNAMED8`
        |    CALL (u1,`  UNNAMED8`) {
        |      WITH `  UNNAMED8` AS `  UNNAMED8`
        |        WHERE `  UNNAMED8` = 0
        |      CALL (u1) {
        |        WITH u1 AS u1
        |          WHERE true = true
        |        RETURN 0 AS `  UNNAMED6`, u1 AS `  UNNAMED7`
        |      }
        |      RETURN `  UNNAMED6` AS `  UNNAMED6`, `  UNNAMED7` AS `  UNNAMED7`
        |    }
        |    RETURN `  UNNAMED6` AS `  UNNAMED6`, `  UNNAMED7` AS `  UNNAMED7`
        |    UNION
        |    WITH u1 AS u1
        |      WHERE true = true
        |    RETURN 3 AS `  UNNAMED6`, u1 AS `  UNNAMED7`
        |  }
        |  RETURN `  UNNAMED6` AS `  UNNAMED6`, `  UNNAMED7` AS `  UNNAMED7`
        |}
        |RETURN `  UNNAMED6` AS x, `  UNNAMED7` AS u1""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT query nesting 25") {
    assertRewritten(
      """UNWIND [1, 2] AS x
        |RETURN x
        |
        |NEXT
        |
        |{
        |  WITH *, 3 AS b, 4 AS z
        |  RETURN *
        |  UNION
        |  WITH *, 3 AS b
        |  CALL(*) {
        |    WITH *, 3 AS c
        |    RETURN x + c AS z
        |  }
        |  RETURN *
        |
        |  NEXT
        |
        |  WHEN z > 0 THEN RETURN b, z
        |  ELSE RETURN b, z
        |}""".stripMargin,
      """UNWIND [1, 2] AS x
        |WITH x AS x
        |WITH count(*) AS `  UNNAMED1`, collect([x]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  WITH x AS x, 3 AS b, 4 AS z
        |  RETURN b AS `  UNNAMED4`, x AS `  UNNAMED3`, z AS `  UNNAMED5`
        |  UNION
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS x
        |  WITH x AS x, 3 AS b
        |  CALL (x) {
        |    WITH x AS x, 3 AS c
        |    RETURN x + c AS z
        |  }
        |  RETURN b AS `  UNNAMED4`, x AS `  UNNAMED3`, z AS `  UNNAMED5`
        |}
        |WITH `  UNNAMED5` AS z, `  UNNAMED4` AS b
        |WITH z AS z, b AS b, CASE
        |  WHEN z > 0 THEN 0
        |  ELSE 1
        |END AS `  UNNAMED6`
        |CALL (z,b,`  UNNAMED6`) {
        |  WITH `  UNNAMED6` AS `  UNNAMED6`
        |    WHERE `  UNNAMED6` = 0
        |  CALL (b,z) {
        |    RETURN b AS `  UNNAMED7`, z AS `  UNNAMED8`
        |  }
        |  RETURN `  UNNAMED7` AS `  UNNAMED7`, `  UNNAMED8` AS `  UNNAMED8`
        |  UNION ALL
        |  WITH `  UNNAMED6` AS `  UNNAMED6`
        |    WHERE `  UNNAMED6` = 1
        |  CALL (b,z) {
        |    RETURN b AS `  UNNAMED7`, z AS `  UNNAMED8`
        |  }
        |  RETURN `  UNNAMED7` AS `  UNNAMED7`, `  UNNAMED8` AS `  UNNAMED8`
        |}
        |WITH `  UNNAMED7` AS b, `  UNNAMED8` AS z
        |RETURN b AS b, z AS z""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT push variables correctly RETURN") {
    assertRewritten(
      """RETURN 7 AS n
        |
        |NEXT
        |
        |RETURN 1 AS v
        |
        |NEXT
        |
        |RETURN DISTINCT *
        |""".stripMargin,
      """|WITH 7 AS n
         |WITH 1 AS v
         |RETURN DISTINCT v AS v""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT push variables correctly WITH") {
    assertRewritten(
      """RETURN 7 AS n
        |
        |NEXT
        |
        |{
        |  RETURN 1 AS v
        |
        |  NEXT
        |
        |  WITH DISTINCT *
        |  RETURN 8 AS x
        |}""".stripMargin,
      """WITH 7 AS n
        |WITH 1 AS v
        |WITH DISTINCT v AS v
        |WITH 8 AS x
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("NEXT with USE and USE") {
    assertRewritten(
      """USE myGraph0
        |MATCH (p0:Person)
        |RETURN DISTINCT p0.name AS name0
        |
        |NEXT
        |
        |USE myGraph1
        |MATCH (p1:Person)
        |RETURN DISTINCT name0, p1.name AS name1""".stripMargin,
      """CALL () {
        |  USE myGraph0
        |  MATCH (p0:Person)
        |  RETURN DISTINCT p0.name AS name0
        |}
        |WITH count(*) AS `  UNNAMED1`, collect([name0]) AS `  UNNAMED0`
        |CALL (`  UNNAMED0`,`  UNNAMED1`) {
        |  USE myGraph1
        |  UNWIND range(0, `  UNNAMED1` - 1) AS `  UNNAMED2`
        |  WITH (`  UNNAMED0`[`  UNNAMED2`])[0] AS name0
        |  MATCH (p1:Person)
        |  RETURN DISTINCT name0 AS `  UNNAMED3`, p1.name AS `  UNNAMED4`
        |}
        |RETURN `  UNNAMED3` AS name0, `  UNNAMED4` AS name1""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("Passing through with star on top level Cypher 5") {
    assertRewritten(
      """MATCH (n)
        |CREATE (m)
        |WITH *
        |MATCH (o)
        |RETURN m AS m, n AS n, o AS o""".stripMargin,
      """MATCH (n)
        |CREATE (m)
        |WITH m AS m, n AS n
        |MATCH (o)
        |RETURN m AS m, n AS n, o AS o""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate(),
      excludedVersions = Set(CypherVersion.Cypher25)
    )
  }

  test("Passing through with star on top level Cypher 25") {
    assertRewritten(
      """MATCH (n)
        |CREATE (m)
        |WITH *
        |MATCH (o)
        |RETURN m AS m, n AS n, o AS o""".stripMargin,
      """MATCH (n)
        |CREATE (m)
        |MATCH (o)
        |RETURN m AS m, n AS n, o AS o""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // TODO checks these recorded scopes in the scope surveyor
  test("Importing with and use clause orderings") {
    assertRewritten(
      """UNWIND [3, 2, 1, 0, 4, 5] AS i
        |CALL {
        |  USE `composite.remoteGraph1`
        |  WITH i AS i
        |  WITH *, i / i AS a
        |  CREATE (n:Number {value: i})
        |  RETURN n.value * 10 AS j
        |} IN TRANSACTIONS OF 2 ROWS ON ERROR BREAK
        |RETURN j AS j""".stripMargin,
      """UNWIND [3, 2, 1, 0, 4, 5] AS i
        |CALL {
        |  USE `composite.remoteGraph1`
        |  WITH i AS i
        |  WITH i AS i, i / i AS a
        |  CREATE (n:Number {value: i})
        |  RETURN n.value * 10 AS j
        |} IN TRANSACTIONS OF 2 ROWS ON ERROR BREAK
        |RETURN j AS j""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate(),
      excludedVersions = Set.empty
    )

    assertRewritten(
      """UNWIND [3, 2, 1, 0, 4, 5] AS i
        |CALL {
        |  USE `composite.remoteGraph1`
        |  WITH 5 AS i
        |  WITH *, i / i AS a
        |  CREATE (n:Number {value: i})
        |  RETURN n.value * 10 AS j
        |} IN TRANSACTIONS OF 2 ROWS ON ERROR BREAK
        |RETURN j AS j""".stripMargin,
      """UNWIND [3, 2, 1, 0, 4, 5] AS i
        |CALL {
        |  USE `composite.remoteGraph1`
        |  WITH 5 AS i
        |  WITH i AS i, i / i AS a
        |  CREATE (n:Number {value: i})
        |  RETURN n.value * 10 AS j
        |} IN TRANSACTIONS OF 2 ROWS ON ERROR BREAK
        |RETURN j AS j""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate(),
      excludedVersions = Set.empty
    )

    assertRewritten(
      """UNWIND [3, 2, 1, 0, 4, 5] AS i
        |CALL {
        |  WITH i AS i
        |  USE `composite.remoteGraph1`
        |  WITH *, i / i AS a
        |  CREATE (n:Number {value: i})
        |  RETURN n.value * 10 AS j
        |} IN TRANSACTIONS OF 2 ROWS ON ERROR BREAK
        |RETURN j AS j""".stripMargin,
      """UNWIND [3, 2, 1, 0, 4, 5] AS i
        |CALL {
        |  WITH i AS i
        |  USE `composite.remoteGraph1`
        |  WITH i AS i, i / i AS a
        |  CREATE (n:Number {value: i})
        |  RETURN n.value * 10 AS j
        |} IN TRANSACTIONS OF 2 ROWS ON ERROR BREAK
        |RETURN j AS j""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate(),
      excludedVersions = Set.empty
    )

    assertRewritten(
      """UNWIND [3, 2, 1, 0, 4, 5] AS i
        |CALL {
        |  WITH i
        |  WITH *, i / i AS a
        |  CREATE (n:Number {value: i})
        |  RETURN n.value * 10 AS j
        |} IN TRANSACTIONS OF 2 ROWS ON ERROR BREAK
        |RETURN j AS j""".stripMargin,
      """UNWIND [3, 2, 1, 0, 4, 5] AS i
        |CALL {
        |  WITH i AS i
        |  WITH i AS i, i / i AS a
        |  CREATE (n:Number {value: i})
        |  RETURN n.value * 10 AS j
        |} IN TRANSACTIONS OF 2 ROWS ON ERROR BREAK
        |RETURN j AS j""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate(),
      excludedVersions = Set.empty
    )
  }

  test("Handle uneven union importing with") {
    assertRewritten(
      """WITH
        |  [] AS oneWayDemandList,
        |  [] AS updatedOnewayEquipments,
        |  [e IN [] | e['equipmentTrimId']] AS updatedTrims
        |CALL {
        |  WITH oneWayDemandList, updatedOnewayEquipments
        |  WITH * WHERE true = true
        |  RETURN 1 AS y
        |    UNION
        |  WITH updatedOnewayEquipments
        |  WITH * WHERE true = true
        |  RETURN 1 AS y
        |}
        |RETURN
        |  COLLECT {
        |    RETURN 1 AS e
        |  } AS res""".stripMargin,
      """WITH [] AS oneWayDemandList, [] AS updatedOnewayEquipments, [e IN [] | e["equipmentTrimId"]] AS updatedTrims
        |CALL {
        |  WITH oneWayDemandList AS oneWayDemandList, updatedOnewayEquipments AS updatedOnewayEquipments
        |  WITH oneWayDemandList AS oneWayDemandList, updatedOnewayEquipments AS updatedOnewayEquipments
        |    WHERE true = true
        |  RETURN 1 AS y
        |  UNION
        |  WITH updatedOnewayEquipments AS updatedOnewayEquipments
        |  WITH updatedOnewayEquipments AS updatedOnewayEquipments
        |    WHERE true = true
        |  RETURN 1 AS y
        |}
        |RETURN COLLECT { RETURN 1 AS e } AS res""".stripMargin
    )
  }

  test("Skip unused imports") {
    assertRewritten(
      """WITH 1 AS a
        |CALL (*) {
        |  RETURN 8 AS x
        |    UNION
        |  {
        |  WHEN
        |    EXISTS {
        |      {
        |      RETURN 4 AS x
        |        UNION
        |      RETURN 9 AS x
        |      }
        |        UNION
        |      CALL (*) {
        |        RETURN 1 AS x
        |      }
        |      RETURN *
        |    }
        |    THEN
        |    RETURN 2 AS x
        |  }
        |}
        |RETURN x AS x""".stripMargin,
      """WITH 1 AS a
        |CALL () {
        |  RETURN 8 AS x
        |  UNION
        |  WITH CASE
        |  WHEN EXISTS {
        |  CALL () {
        |    RETURN 4 AS x
        |    UNION
        |    RETURN 9 AS x
        |  }
        |  RETURN x AS x
        |  UNION
        |  CALL () {
        |    RETURN 1 AS x
        |  }
        |  RETURN x AS x
        |} THEN 0
        |  ELSE 1
        |END AS `  UNNAMED0`
        |  CALL (`  UNNAMED0`) {
        |    WITH `  UNNAMED0` AS `  UNNAMED0`
        |      WHERE `  UNNAMED0` = 0
        |    CALL () {
        |      RETURN 2 AS x
        |    }
        |    RETURN x AS x
        |  }
        |  RETURN x AS x
        |}
        |RETURN x AS x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("uses the position of the clause for variables in new return items") {
    // This is quite important. If the position of a variable in new return item is a previous declaration,
    // that can destroy scoping. In a query like
    // MATCH (owner)
    // WITH HEAD(COLLECT(42)) AS sortValue, owner
    // RETURN *
    // owner would not be scoped/namespaced correctly after having done `isolateAggregation`.
    val wizz = "WITH 1 AS foo "
    val expressionPos = InputPosition(wizz.length, 1, wizz.length + 1)

    val query = s"${wizz}RETURN *"
    val plannerName = new PlannerName {
      override def name: String = "fake"
      override def toTextOutput: String = "fake"
      override def version: String = "fake"
    }
    val testState =
      InitialState(query, plannerName, new AnonymousVariableNameGenerator)
    val testContext = TestContext(cypherVersion = CypherVersion.Cypher25)
    val scopeState = ScopeSurveyor.process(Parse.process(testState, testContext), testContext)
    val rewritten = ExpandClauses.process(scopeState, testContext)

    val returnItem = rewritten.statement().asInstanceOf[Query].asInstanceOf[SingleQuery]
      .clauses.last.asInstanceOf[Return].returnItems.items.head.asInstanceOf[AliasedReturnItem]
    returnItem.expression.position should equal(expressionPos)
    returnItem.variable.position.offset should equal(expressionPos.offset)
  }

}

class ExpandCommandClauseTest extends CypherFunSuite with AstRewritingTestSupport {

  test("should rewrite show commands properly 1") {
    assertRewrite(
      "SHOW FUNCTIONS YIELD *",
      """SHOW FUNCTIONS
        |YIELD name, category, description, signature, isBuiltIn, argumentDescription, returnDescription, aggregating, rolesExecution, rolesBoostedExecution, isDeprecated, deprecatedBy
        |RETURN name, category, description, signature, isBuiltIn, argumentDescription, returnDescription, aggregating, rolesExecution, rolesBoostedExecution, isDeprecated, deprecatedBy""".stripMargin,
      showCommandReturnAddedInRewrite = true,
      moveYieldToWith = true
    )

  }

  test("should rewrite show commands properly 2") {
    assertRewrite(
      "SHOW FUNCTIONS YIELD * RETURN *",
      """SHOW FUNCTIONS
        |YIELD name, category, description, signature, isBuiltIn, argumentDescription, returnDescription, aggregating, rolesExecution, rolesBoostedExecution, isDeprecated, deprecatedBy
        |RETURN name, category, description, signature, isBuiltIn, argumentDescription, returnDescription, aggregating, rolesExecution, rolesBoostedExecution, isDeprecated, deprecatedBy""".stripMargin,
      moveYieldToWith = true
    )
  }

  test("should rewrite show commands properly 3") {
    assertRewrite(
      "SHOW TRANSACTIONS YIELD * RETURN *",
      """SHOW TRANSACTIONS
        |YIELD database, transactionId, currentQueryId, outerTransactionId, connectionId, clientAddress, username, metaData, currentQuery, parameters, planner, runtime,
        |indexes, startTime, currentQueryStartTime, protocol, requestUri, status, currentQueryStatus, statusDetails, resourceInformation, activeLockCount, currentQueryActiveLockCount,
        |elapsedTime, cpuTime, waitTime, idleTime, currentQueryElapsedTime, currentQueryCpuTime, currentQueryWaitTime, currentQueryIdleTime, currentQueryAllocatedBytes, allocatedDirectBytes,
        |estimatedUsedHeapMemory, pageHits, pageFaults, currentQueryPageHits, currentQueryPageFaults, initializationStackTrace
        |RETURN database, transactionId, currentQueryId, outerTransactionId, connectionId, clientAddress, username, metaData, currentQuery, parameters, planner, runtime,
        |indexes, startTime, currentQueryStartTime, protocol, requestUri, status, currentQueryStatus, statusDetails, resourceInformation, activeLockCount, currentQueryActiveLockCount,
        |elapsedTime, cpuTime, waitTime, idleTime, currentQueryElapsedTime, currentQueryCpuTime, currentQueryWaitTime, currentQueryIdleTime, currentQueryAllocatedBytes, allocatedDirectBytes,
        |estimatedUsedHeapMemory, pageHits, pageFaults, currentQueryPageHits, currentQueryPageFaults, initializationStackTrace""".stripMargin,
      moveYieldToWith = true
    )
  }

  test("should rewrite show commands properly 4") {
    assertRewrite(
      "TERMINATE TRANSACTIONS 'db-transaction-123' YIELD *",
      """TERMINATE TRANSACTIONS 'db-transaction-123'
        |YIELD transactionId, username, message
        |RETURN transactionId, username, message""".stripMargin,
      showCommandReturnAddedInRewrite = true,
      moveYieldToWith = true
    )
  }

  test("should rewrite show commands properly 5") {
    assertRewrite(
      "SHOW USERS YIELD *",
      """SHOW USERS
        |YIELD user, roles, passwordChangeRequired, suspended, home""".stripMargin
    )
  }

  test("should rewrite show commands properly 6") {
    assertRewrite(
      "SHOW USERS WITH AUTH YIELD * RETURN *",
      """SHOW USERS WITH AUTH
        |YIELD user, roles, passwordChangeRequired, suspended, home, provider, auth
        |RETURN user, roles, passwordChangeRequired, suspended, home, provider, auth""".stripMargin
    )
  }

  private def assertRewrite(
    originalQuery: String,
    expectedQuery: String,
    showCommandReturnAddedInRewrite: Boolean = false,
    moveYieldToWith: Boolean = false
  ): Unit = {
    val original = prepRewrite(originalQuery, rewriteShowCommand = true)
    val expected = prepRewrite(expectedQuery)
    val expectedUpdatedReturn =
      if (showCommandReturnAddedInRewrite) {
        updateClauses(
          expected,
          clauses => {
            // update `addedInRewrite` flag on the return
            val ret = clauses.last.asInstanceOf[Return]
            val newRet = ret.copy(returnType = ReturnAddedInRewrite)(ret.position)
            clauses.dropRight(1) :+ newRet
          }
        )
      } else expected
    val expectedUpdatedYield =
      if (moveYieldToWith) {
        updateClauses(
          expectedUpdatedReturn,
          clauses => {
            // show and terminate commands parses YIELD as WITH *
            clauses.flatMap {
              case s: ShowTransactionsClause =>
                Seq[Clause](s.copy(yieldAll = true, yieldItems = List.empty, yieldWith = None)(s.position)) ++
                  s.yieldWith.map(rewriteWithForShowCommands)

              case t: TerminateTransactionsClause =>
                Seq[Clause](t.copy(yieldAll = true, yieldItems = List.empty, yieldWith = None)(t.position)) ++
                  t.yieldWith.map(rewriteWithForShowCommands)

              case s: ShowSettingsClause =>
                Seq[Clause](s.copy(yieldAll = true, yieldItems = List.empty, yieldWith = None)(s.position)) ++
                  s.yieldWith.map(rewriteWithForShowCommands)

              case s: ShowFunctionsClause =>
                Seq[Clause](s.copy(yieldAll = true, yieldItems = List.empty, yieldWith = None)(s.position)) ++
                  s.yieldWith.map(rewriteWithForShowCommands)

              case s: ShowProceduresClause =>
                Seq[Clause](s.copy(yieldAll = true, yieldItems = List.empty, yieldWith = None)(s.position)) ++
                  s.yieldWith.map(rewriteWithForShowCommands)

              case s: ShowConstraintsClause =>
                Seq[Clause](s.copy(yieldAll = true, yieldItems = List.empty, yieldWith = None)(s.position)) ++
                  s.yieldWith.map(rewriteWithForShowCommands)

              case s: ShowIndexesClause =>
                Seq[Clause](s.copy(yieldAll = true, yieldItems = List.empty, yieldWith = None)(s.position)) ++
                  s.yieldWith.map(rewriteWithForShowCommands)

              case s: ShowCurrentGraphTypeClause =>
                Seq[Clause](s.copy(yieldAll = true, yieldItems = List.empty, yieldWith = None)(s.position)) ++
                  s.yieldWith.map(rewriteWithForShowCommands)

              case c => Seq(c)
            }
          }
        )
      } else expectedUpdatedReturn

    val transformer = ScopeSurveyor andThen ExpandClauses
    val plannerName = new PlannerName {
      override def name: String = "fake"
      override def toTextOutput: String = "fake"
      override def version: String = "fake"
    }

    val initialState =
      InitialState(originalQuery, plannerName, new AnonymousVariableNameGenerator, maybeStatement = Some(original))
    val result = transformer.transform(initialState, TestContext(cypherVersion = CypherVersion.Cypher25)).statement()

    assert(result === expectedUpdatedYield)
  }

  private def rewriteWithForShowCommands(w: With): With = {
    val returnItems = w.returnItems.defaultOrderOnColumns.map(c =>
      c.map(v => aliasedReturnItem(varFor(v)))
    ).getOrElse(List.empty)
    w.copy(returnItems =
      w.returnItems.copy(
        FreeProjection,
        items = returnItems,
        defaultOrderOnColumns = None
      )(w.returnItems.position)
    )(w.position)
  }

  private def prepRewrite(q: String, rewriteShowCommand: Boolean = false) = {
    val exceptionFactory = Neo4jCypherExceptionFactory(q, None)
    val rewriter =
      if (rewriteShowCommand)
        inSequence(
          NormalizeWithAndReturnClauses(exceptionFactory),
          RewriteShowQuery.instance,
          ExpandShowWhere.instance
        )
      else
        inSequence(NormalizeWithAndReturnClauses(exceptionFactory))
    parse(q, exceptionFactory).endoRewrite(rewriter)
  }

  private def updateClauses(statement: Statement, updateClauses: Seq[Clause] => Seq[Clause]): Statement = {
    val query = statement.asInstanceOf[Query]
    val singleQuery = query.asInstanceOf[SingleQuery]
    val clauses = singleQuery.clauses
    val newClauses = updateClauses(clauses)
    singleQuery.copy(newClauses)(singleQuery.position)
  }
}
