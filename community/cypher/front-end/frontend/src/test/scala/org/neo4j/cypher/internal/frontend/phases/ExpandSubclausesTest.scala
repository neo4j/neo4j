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
import org.neo4j.cypher.internal.ast.*
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.GroupByClause
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UseAsMultipleGraphsSelector
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExpandSubclauses
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.util.*
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ExpandSubclausesTest extends CypherFunSuite with RewritePhaseTest with AstConstructionTestSupport {

  override def rewriterPhaseUnderTest: Transformer[BaseContext, BaseState, BaseState] =
    ScopeSurveyor andThen ExpandSubclauses

  override val phaseTestConfig = PhaseTestConfig(
    excludedVersions = Set(CypherVersion.Cypher5),
    semanticFeatures = Seq(UseAsMultipleGraphsSelector, GroupByClause)
  )

  private def withUpdate() = (expectedStatement: Statement) => {
    expectedStatement.endoRewrite(bottomUp(Rewriter.lift {
      // The original/rewritten statement will have AddedInRewriteGeneral,
      case w: With =>
        w.copy(withType = AddedInRewriteGeneral())(w.position)
      case ri: ReturnItems => ri.copy(projectionType = FreeProjection)(ri.position)
    }))
  }

  // 1. GROUP BY without aggregation collapses to DISTINCT.

  test("RETURN: GROUP BY without aggregation becomes RETURN DISTINCT") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |RETURN a, b
        |  GROUP BY a, b""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |RETURN DISTINCT a, b""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: GROUP BY without aggregation becomes WITH DISTINCT") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |WITH a, b GROUP BY a, b
        |RETURN a, b""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |WITH DISTINCT a, b
        |RETURN a, b""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // 2. Aggregation with all GROUP BY keys already projected: drop GROUP BY / DISTINCT, no extra WITH.

  test("RETURN: aggregation with all GROUP BY keys projected drops GROUP BY") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |RETURN a, b, count(*) AS cnt GROUP BY a, b""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |RETURN a, b, count(*) AS cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN DISTINCT: all keys projected drops DISTINCT and GROUP BY") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |RETURN DISTINCT a, b, count(*) AS cnt GROUP BY a, b""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |RETURN a, b, count(*) AS cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: aggregation with all GROUP BY keys projected drops GROUP BY") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |WITH a, b, count(*) AS cnt GROUP BY a, b
        |RETURN a, b, cnt""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |WITH a, b, count(*) AS cnt
        |RETURN a, b, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH DISTINCT: all keys projected drops DISTINCT and GROUP BY") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |WITH DISTINCT a, b, count(*) AS cnt GROUP BY a, b
        |RETURN a, b, cnt""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |WITH a, b, count(*) AS cnt
        |RETURN a, b, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // 3. Aggregation with a grouping key that is not projected: insert an inner grouping WITH.

  test("RETURN DISTINCT: extra GROUP BY key inserts a grouping WITH") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b, 3 AS c
        |RETURN DISTINCT a, b, count(*) AS cnt GROUP BY a, b, c""".stripMargin,
      """WITH 1 AS a, 2 AS b, 3 AS c
        |WITH a, b, c, count(*) AS cnt
        |RETURN a, b, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN DISTINCT: extra GROUP BY expression key inserts grouping WITH with generated alias") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b, 3 AS c
        |RETURN DISTINCT a, b, count(*) AS cnt GROUP BY a, b, toInteger(c + 1) * b""".stripMargin,
      """WITH 1 AS a, 2 AS b, 3 AS c
        |WITH a, b, toInteger(c + 1) * b AS `  UNNAMED0`, count(*) AS cnt
        |RETURN a, b, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH DISTINCT: extra GROUP BY key inserts a grouping WITH") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b, 3 AS c
        |WITH DISTINCT a, b, count(*) AS cnt GROUP BY a, b, c
        |RETURN a, b, cnt""".stripMargin,
      """WITH 1 AS a, 2 AS b, 3 AS c
        |WITH a, b, c, count(*) AS cnt
        |WITH a, b, cnt
        |RETURN a, b, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH DISTINCT: extra GROUP BY expression key inserts grouping WITH with generated alias") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b, 3 AS c
        |WITH DISTINCT a, b, count(*) AS cnt GROUP BY a, b, toInteger(c + 1) * b
        |RETURN a, b, cnt""".stripMargin,
      """WITH 1 AS a, 2 AS b, 3 AS c
        |WITH a, b, toInteger(c + 1) * b AS `  UNNAMED0`, count(*) AS cnt
        |WITH a, b, cnt
        |RETURN a, b, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: non-aggregating item over an inset complex grouping key uses the hoisted alias") {
    assertRewritten(
      """WITH {p: 1} AS a
        |RETURN a.p + 1 AS x, count(*) AS cnt
        |  GROUP BY a.p""".stripMargin,
      """WITH {p: 1} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |RETURN `  UNNAMED0` + 1 AS x, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: projection and ORDER BY over an inset complex grouping key both use the hoisted alias") {
    assertRewritten(
      """WITH {p: 1} AS a
        |RETURN a.p + 1 AS x, count(*) AS cnt
        |  GROUP BY a.p
        |  ORDER BY 2 + a.p + 1""".stripMargin,
      """WITH {p: 1} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |RETURN `  UNNAMED0` + 1 AS x, cnt
        |  ORDER BY 2 + `  UNNAMED0` + 1""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: non-aggregating item over an inset complex grouping key uses the hoisted alias") {
    assertRewritten(
      """WITH {p: 1} AS a
        |WITH a.p + 1 AS x, count(*) AS cnt
        |  GROUP BY a.p
        |RETURN x, cnt""".stripMargin,
      """WITH {p: 1} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |WITH `  UNNAMED0` + 1 AS x, cnt
        |RETURN x, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY over a grouping key that references a passthrough grouping key uses the alias") {
    assertRewritten(
      """WITH {p: 1} AS a, 2 AS b
        |RETURN coalesce(a.p, b) AS x, b, count(*) AS cnt
        |  ORDER BY abs(coalesce(a.p, b))""".stripMargin,
      """WITH {p: 1} AS a, 2 AS b
        |RETURN coalesce(a.p, b) AS x, b, count(*) AS cnt
        |  ORDER BY abs(x) ASCENDING""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: ORDER BY over a grouping key that references a passthrough grouping key uses the alias") {
    assertRewritten(
      """WITH {p: 1} AS a, 2 AS b
        |WITH coalesce(a.p, b) AS x, b, count(*) AS cnt
        |  ORDER BY abs(coalesce(a.p, b))
        |RETURN x, b, cnt""".stripMargin,
      """WITH {p: 1} AS a, 2 AS b
        |WITH coalesce(a.p, b) AS x, b, count(*) AS cnt
        |  ORDER BY abs(x) ASCENDING
        |RETURN x, b, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: inset complex grouping key inside a list comprehension is rewritten in source and inner scope") {
    assertRewritten(
      """WITH {p: [1, 2, 3]} AS a
        |RETURN [y IN a.p WHERE y < size(a.p) | y * size(a.p)] AS l, count(*) AS cnt
        |  GROUP BY a.p""".stripMargin,
      """WITH {p: [1, 2, 3]} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |RETURN [y IN `  UNNAMED0` WHERE y < size(`  UNNAMED0`) | y * size(`  UNNAMED0`)] AS l, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: a grouping-key reference shadowed by a list comprehension's variable is not rewritten") {
    assertRewritten(
      """WITH {p: 1} AS a
        |RETURN [a IN [{p: 10}] WHERE a.p > 5 | a.p] AS l, count(*) AS cnt
        |  GROUP BY a.p""".stripMargin,
      """WITH {p: 1} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |RETURN [a IN [{p: 10}] WHERE a.p > 5 | a.p] AS l, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: inset complex grouping key inside an `any` predicate uses the hoisted alias") {
    assertRewritten(
      """WITH {p: [1, 2, 3]} AS a
        |RETURN any(y IN a.p WHERE y < size(a.p)) AS b, count(*) AS cnt
        |  GROUP BY a.p""".stripMargin,
      """WITH {p: [1, 2, 3]} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |RETURN any(y IN `  UNNAMED0` WHERE y < size(`  UNNAMED0`)) AS b, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: inset complex grouping key inside a reduce uses the hoisted alias") {
    assertRewritten(
      """WITH {p: [1, 2, 3]} AS a
        |RETURN reduce(acc = 0, y IN a.p | acc + y + size(a.p)) AS r, count(*) AS cnt
        |  GROUP BY a.p""".stripMargin,
      """WITH {p: [1, 2, 3]} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |RETURN reduce(acc = 0, y IN `  UNNAMED0` | acc + y + size(`  UNNAMED0`)) AS r, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: inset complex grouping key inside a pattern comprehension uses the hoisted alias") {
    assertRewritten(
      """WITH {p: [1, 2, 3]} AS a
        |RETURN [(n)-->(m) WHERE m.v = size(a.p) | m.v] AS l, count(*) AS cnt
        |  GROUP BY a.p""".stripMargin,
      """WITH {p: [1, 2, 3]} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |RETURN [(n)-->(m) WHERE m.v = size(`  UNNAMED0`) | m.v] AS l, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: inset complex grouping key inside a COUNT subquery uses the hoisted alias") {
    assertRewritten(
      """WITH {p: [1, 2, 3]} AS a
        |RETURN COUNT { MATCH (n) WHERE n.v = size(a.p) } AS c, count(*) AS cnt
        |  GROUP BY a.p""".stripMargin,
      """WITH {p: [1, 2, 3]} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |RETURN COUNT { MATCH (n) WHERE n.v = size(`  UNNAMED0`) } AS c, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: inset complex grouping key inside an allreduce uses the hoisted alias") {
    assertRewritten(
      """WITH {p: [1, 2, 3]} AS a
        |RETURN allreduce(acc = 0, y IN a.p | acc + y + size(a.p), acc < size(a.p)) AS r, count(*) AS cnt
        |  GROUP BY a.p""".stripMargin,
      """WITH {p: [1, 2, 3]} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |RETURN allreduce(acc = 0, y IN `  UNNAMED0` | acc + y + size(`  UNNAMED0`), acc < size(`  UNNAMED0`)) AS r, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: a grouping-key reference shadowed by an allreduce's variable is not rewritten") {
    assertRewritten(
      """WITH {p: 1} AS a
        |RETURN allreduce(acc = 0, a IN [{p: 10}] | acc + a.p, a.p > 0) AS r, count(*) AS cnt
        |  GROUP BY a.p""".stripMargin,
      """WITH {p: 1} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |RETURN allreduce(acc = 0, a IN [{p: 10}] | acc + a.p, a.p > 0) AS r, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // 4. GROUP BY () and GROUP BY <key>: project the aggregations, then re-add constant items.

  test("WITH: GROUP BY () projects aggregations first, then re-adds constants") {
    assertRewritten(
      """LET a = 10, const = "const"
        |UNWIND [1, 2, 3] AS x
        |FILTER WHERE false
        |WITH "const2" AS c2, SUM(x / a) * 5 AS s
        |  GROUP BY ()
        |RETURN *;
      """.stripMargin,
      """LET a = 10, const = "const"
        |UNWIND [1, 2, 3] AS x
        |FILTER WHERE false
        |WITH SUM(x / a) * 5 AS s
        |WITH "const2" AS c2, s
        |RETURN *;
      """.stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: GROUP BY () with ORDER BY over aggregation and constant") {
    assertRewritten(
      """LET a = 10, const = "const"
        |UNWIND [1, 2, 3] AS x
        |FILTER WHERE false
        |WITH "const2" AS c2, SUM(x / a) * 5 AS s
        |  GROUP BY ()
        |  ORDER BY SUM(x / a) * 5, c2
        |RETURN *;
      """.stripMargin,
      """LET a = 10, const = "const"
        |UNWIND [1, 2, 3] AS x
        |FILTER WHERE false
        |WITH SUM(x / a) * 5 AS s
        |WITH "const2" AS c2, s
        |  ORDER BY s, c2
        |RETURN *;
      """.stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: GROUP BY x projects key + aggregation, then re-adds constants") {
    assertRewritten(
      """LET a = 10, const = "const"
        |UNWIND [1, 2, 3] AS x
        |WITH "const2" AS c2, SUM(x / a) * 5 AS s
        |  GROUP BY x
        |RETURN *;
      """.stripMargin,
      """LET a = 10, const = "const"
        |UNWIND [1, 2, 3] AS x
        |WITH x, SUM(x / a) * 5 AS s
        |WITH "const2" AS c2, s
        |RETURN *;
      """.stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // 5. Aggregations inside ORDER BY / WHERE: reuse a matching projection alias, else hoist to a generated alias.

  test("RETURN: aggregation in ORDER BY matching a projection item reuses its alias") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |RETURN a, b, count(b) AS cnt
        |  ORDER BY count(b)""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |RETURN a, b, count(b) AS cnt
        |  ORDER BY cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: non-matching aggregation in ORDER BY is hoisted to a generated alias") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |RETURN a, b, count(b) AS cnt
        |  ORDER BY count(b), count(b) + sum(a)""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |WITH a AS a, b AS b, count(b) AS cnt, count(b) + sum(a) AS `  UNNAMED0`
        |RETURN a AS a, b AS b, cnt AS cnt
        |  ORDER BY cnt, `  UNNAMED0` ASCENDING""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: non-matching aggregation in DESC ORDER BY is hoisted") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |RETURN a, b, count(b) AS cnt
        |  ORDER BY count(b) + sum(a) DESC""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |WITH a AS a, b AS b, count(b) AS cnt, count(b) + sum(a) AS `  UNNAMED0`
        |RETURN a AS a, b AS b, cnt AS cnt
        |  ORDER BY `  UNNAMED0` DESCENDING""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: partially-replaceable aggregating ORDER BY expression (one reused, one hoisted)") {
    assertRewritten(
      """UNWIND [1, 2, 3, 4] AS x
        |WITH 1 AS a, 2 AS b, x
        |RETURN x, count(a) AS cnt
        |  ORDER BY count(a) + sum(a), count(a)""".stripMargin,
      """UNWIND [1, 2, 3, 4] AS x
        |WITH 1 AS a, 2 AS b, x AS x
        |WITH x AS x, count(a) AS cnt, count(a) + sum(a) AS `  UNNAMED0`
        |RETURN x AS x, cnt AS cnt
        |  ORDER BY `  UNNAMED0`, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: partially-replaceable aggregating ORDER BY expression") {
    assertRewritten(
      """UNWIND [1, 2, 3, 4] AS x
        |WITH 1 AS a, 2 AS b, x
        |WITH x, count(a) AS cnt
        |  ORDER BY count(a) + sum(a), count(a)
        |RETURN x, cnt""".stripMargin,
      """UNWIND [1, 2, 3, 4] AS x
        |WITH 1 AS a, 2 AS b, x AS x
        |WITH x AS x, count(a) AS cnt, count(a) + sum(a) AS `  UNNAMED0`
        |WITH x AS x, cnt AS cnt
        |  ORDER BY `  UNNAMED0`, cnt
        |RETURN x, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: aggregation in WHERE matching a projection item is substituted with its alias") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |WITH a, b, count(b) AS cnt WHERE count(b) > 0
        |RETURN a, b, cnt""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |WITH a, b, count(b) AS cnt
        |  WHERE cnt > 0
        |RETURN a, b, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: non-matching aggregation in WHERE is hoisted") {
    assertRewritten(
      """UNWIND [1, 2, 3, 4] AS x
        |WITH 1 AS a, 2 AS b, x
        |WITH x, count(a) AS cnt WHERE count(a) + sum(a) > 0
        |RETURN x, cnt""".stripMargin,
      """UNWIND [1, 2, 3, 4] AS x
        |WITH 1 AS a, 2 AS b, x AS x
        |WITH x AS x, count(a) AS cnt, count(a) + sum(a) > 0 AS `  UNNAMED0`
        |WITH x AS x, cnt AS cnt WHERE `  UNNAMED0`
        |RETURN x, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // 6. All subclauses combined, with ambiguous projection items.

  test("RETURN: all subclauses (GROUP BY/ORDER BY/SKIP/LIMIT) with ambiguous items") {
    assertRewritten(
      """UNWIND [1, 2, 3, 4] AS x
        |WITH 1 AS a, 2 AS b, x
        |RETURN b, count(*) AS cnt, a + 2 AS a
        |  GROUP BY a, b, toInteger(x + 1) * b, x
        |  ORDER BY a, b, x
        |  SKIP 1
        |  LIMIT 2""".stripMargin,
      """UNWIND [1, 2, 3, 4] AS x
        |WITH 1 AS a, 2 AS b, x AS x
        |WITH a + 2 AS a, b AS b, toInteger(x + 1) * b AS `  UNNAMED0`, x AS x, count(*) AS cnt
        |RETURN b AS b, cnt AS cnt, a AS a
        |  ORDER BY a ASCENDING, b ASCENDING, x ASCENDING
        |  SKIP 1
        |  LIMIT 2""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: all subclauses (GROUP BY/ORDER BY/SKIP/LIMIT) with ambiguous items") {
    assertRewritten(
      """UNWIND [1, 2, 3, 4] AS x
        |WITH 1 AS a, 2 AS b, x
        |WITH b, count(*) AS cnt, a + 2 AS a
        |  GROUP BY a, b, toInteger(x + 1) * b, x
        |  ORDER BY a, b, x
        |  SKIP 1
        |  LIMIT 2
        |RETURN b, cnt, a""".stripMargin,
      """UNWIND [1, 2, 3, 4] AS x
        |WITH 1 AS a, 2 AS b, x AS x
        |WITH a + 2 AS a, b AS b, toInteger(x + 1) * b AS `  UNNAMED0`, x AS x, count(*) AS cnt
        |WITH b AS b, cnt AS cnt, a AS a
        |  ORDER BY a ASCENDING, b ASCENDING, x ASCENDING
        |  SKIP 1
        |  LIMIT 2
        |RETURN b, cnt, a""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // 7a. Non-aggregating ORDER BY / WHERE expressions that match a projection item reuse its alias
  //     (auto-aliasing the item when needed).

  test("RETURN: ORDER BY expressions matching items reuse their aliases") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |RETURN a.p AS ap, b.p AS bp, count(*) AS count
        |  ORDER BY a.p, b.p""".stripMargin,
      """MATCH (a:A), (b:B)
        |RETURN a.p AS ap, b.p AS bp, count(*) AS count
        |  ORDER BY ap, bp""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY already referencing aliases is not rewritten") {
    assertNotRewritten(
      """MATCH (a:A), (b:B)
        |RETURN a.p AS ap, b.p AS bp, count(*) AS count
        |  ORDER BY ap, bp""".stripMargin
    )
  }

  test("RETURN: GROUP BY ALL dropped; ORDER BY expressions reuse aliases") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |RETURN a.p AS ap, b.p AS bp, count(*) AS count
        |  GROUP BY ALL
        |  ORDER BY a.p, b.p""".stripMargin,
      """MATCH (a:A), (b:B)
        |RETURN a.p AS ap, b.p AS bp, count(*) AS count
        |  ORDER BY ap, bp""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY reuses alias despite a later variable redefinition") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |RETURN a.p AS x, b AS a, count(*) AS count
        |  ORDER BY a.p""".stripMargin,
      """MATCH (a:A), (b:B)
        |RETURN a.p AS x, b AS a, count(*) AS count
        |  ORDER BY x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: GROUP BY ALL - ORDER BY left intact when its variable is redefined") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |RETURN a.p AS x, b AS a, count(*) AS count
        |  GROUP BY ALL
        |  ORDER BY a.p""".stripMargin,
      """MATCH (a:A), (b:B)
        |RETURN a.p AS x, b AS a, count(*) AS count
        |  ORDER BY a.p""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: unaliased ORDER BY expression is auto-aliased and reused (with redefinition)") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |RETURN a.p, b AS a, count(*) AS count
        |  ORDER BY a.p""".stripMargin,
      """MATCH (a:A), (b:B)
        |RETURN a.p AS `a.p`, b AS a, count(*) AS count
        |  ORDER BY `a.p`""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: GROUP BY x,a dropped; ORDER BY left intact when 'a' is redefined") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |RETURN a.p AS x, b AS a, count(*) AS count
        |  GROUP BY x, a
        |  ORDER BY a.p""".stripMargin,
      """MATCH (a:A), (b:B)
        |RETURN a.p AS x, b AS a, count(*) AS count
        |  ORDER BY a.p""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: unaliased projection expression auto-aliased for ORDER BY") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |RETURN a.p, b AS a
        |  ORDER BY a.p""".stripMargin,
      """MATCH (a:A), (b:B)
        |RETURN a.p AS `a.p`, b AS a
        |  ORDER BY `a.p`""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: ORDER BY expression matching an item reuses its alias") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |WITH a.p AS x, b AS a
        |  ORDER BY a.p
        |RETURN x""".stripMargin,
      """MATCH (a:A), (b:B)
        |WITH a.p AS x, b AS a
        |  ORDER BY x
        |RETURN x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY expression matching an item reuses its alias") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |RETURN a.p AS x, b AS a
        |  ORDER BY a.p""".stripMargin,
      """MATCH (a:A), (b:B)
        |RETURN a.p AS x, b AS a
        |  ORDER BY x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY function-call expression reuses alias") {
    assertRewritten(
      """
      MATCH (a:A), (b:B)
      RETURN toInteger(a.p) AS x, b AS a
        ORDER BY toInteger(a.p)
      """.stripMargin,
      """
      MATCH (a:A), (b:B)
      RETURN toInteger(a.p) AS x, b AS a
        ORDER BY x
      """.stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY arithmetic expression reuses alias") {
    assertRewritten(
      """
      MATCH (a:A), (b:B)
      RETURN 1 + toInteger(a.p) AS x, b AS a
        ORDER BY 1 + toInteger(a.p)
      """.stripMargin,
      """
      MATCH (a:A), (b:B)
      RETURN 1 + toInteger(a.p) AS x, b AS a
        ORDER BY x
      """.stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY expression not matching any item is left intact") {
    assertRewritten(
      """
      MATCH (a:A), (b:B)
      RETURN 1 + toInteger(a.p) AS x, b AS a
        ORDER BY 1 + 1 + toInteger(a.p)
      """.stripMargin,
      """
      MATCH (a:A), (b:B)
      RETURN 1 + toInteger(a.p) AS x, b AS a
        ORDER BY 1 + 1 + toInteger(a.p)
      """.stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: ORDER BY expressions matching items reuse their aliases") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |WITH a.p AS ap, b.p AS bp, count(*) AS count
        |  ORDER BY a.p, b.p
        |RETURN collect([ap, bp, count]) AS result""".stripMargin,
      """MATCH (a:A), (b:B)
        |WITH a.p AS ap, b.p AS bp, count(*) AS count
        |  ORDER BY ap, bp
        |RETURN collect([ap, bp, count]) AS result""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: ORDER BY already referencing aliases is not rewritten") {
    assertNotRewritten(
      """MATCH (a:A), (b:B)
        |WITH a.p AS ap, b.p AS bp, count(*) AS count
        |  ORDER BY ap, bp
        |RETURN collect([ap, bp, count]) AS result""".stripMargin
    )
  }

  test("WITH: GROUP BY ALL dropped; ORDER BY expressions reuse aliases") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |WITH a.p AS ap, b.p AS bp, count(*) AS count
        |  GROUP BY ALL
        |  ORDER BY a.p, b.p
        |RETURN collect([ap, bp, count]) AS result""".stripMargin,
      """MATCH (a:A), (b:B)
        |WITH a.p AS ap, b.p AS bp, count(*) AS count
        |  ORDER BY ap, bp
        |RETURN collect([ap, bp, count]) AS result""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: ORDER BY n.prop reuses existing alias prop") {
    assertRewritten(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY n.prop
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY prop
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("RETURN: ORDER BY n.prop reuses existing alias prop") {
    assertRewritten(
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY n.prop
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY prop
      """.stripMargin
    )
  }

  test("WITH: ORDER BY reuses existing alias inside EXISTS subquery") {
    assertRewritten(
      """RETURN EXISTS {
        | MATCH (n)
        | WITH n.prop AS prop
        | ORDER BY n.prop RETURN prop
        | } AS exists
        |""".stripMargin,
      """RETURN EXISTS {
        | MATCH (n)
        | WITH n.prop AS prop
        | ORDER BY prop RETURN prop AS prop
        | } AS exists
        |""".stripMargin
    )
  }

  test("WITH: WHERE expression matching a projection item reuses its alias") {
    assertRewritten(
      """MATCH (n)
        |WITH size(n.prop) > 10 AS result WHERE size(n.prop) > 10
        |RETURN result
      """.stripMargin,
      """MATCH (n)
        |WITH size(n.prop) > 10 AS result WHERE result
        |RETURN result AS result
      """.stripMargin
    )
  }

  test("RETURN: ORDER BY rewrites property expression to alias when variable not redefined") {
    assertRewritten(
      """MATCH (n)
        |RETURN n AS n, n.prop AS `n.prop`
        |ORDER BY n.foo, n.prop * 2 DESC
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS n, n.prop AS `n.prop`
        |ORDER BY n.foo, `n.prop` * 2 DESC
      """.stripMargin
    )
  }

  // 7b. ORDER BY / WHERE expressions that depend on an alias get rewritten to reference it
  //     (and variable renames from the projection follow the same rules).

  test("WITH: ORDER BY expression depending on an alias is rewritten (size(n.prop) -> size(prop))") {
    assertRewritten(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY size(n.prop)
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY size(prop)
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("RETURN: ORDER BY expression depending on an alias is rewritten") {
    assertRewritten(
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY size(n.prop)
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY size(prop)
      """.stripMargin
    )
  }

  test("WITH: WHERE expression depending on an alias is rewritten") {
    assertRewritten(
      """MATCH (n)
        |WITH n.prop AS prop WHERE size(n.prop) > 10
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop WHERE size(prop) > 10
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("WITH: ORDER BY variable renamed to its alias (n -> m)") {
    assertRewritten(
      """MATCH (n)
        |WITH n AS m ORDER BY n
        |RETURN m
      """.stripMargin,
      """MATCH (n)
        |WITH n AS m ORDER BY m
        |RETURN m AS m
      """.stripMargin
    )
  }

  test("RETURN: ORDER BY variable renamed to its alias (n -> m)") {
    assertRewritten(
      """MATCH (n)
        |RETURN n AS m ORDER BY n
      """.stripMargin,
      """MATCH (n)
        |RETURN n AS m ORDER BY m
      """.stripMargin
    )
  }

  test("WITH: WHERE variable renamed to its alias (n -> m)") {
    assertRewritten(
      """UNWIND [true] as n
        |WITH n AS m WHERE n
        |RETURN m
      """.stripMargin,
      """UNWIND [true] as n
        |WITH n AS m WHERE m
        |RETURN m AS m
      """.stripMargin
    )
  }

  test("WITH: nested ORDER BY expression depending on an alias is rewritten") {
    assertRewritten(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY size(n.prop[0])
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY size(prop[0])
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("RETURN: nested ORDER BY expression depending on an alias is rewritten") {
    assertRewritten(
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY size(n.prop[0])
      """.stripMargin,
      """MATCH (n)
        |RETURN n.prop AS prop ORDER BY size(prop[0])
      """.stripMargin
    )
  }

  test("WITH: nested WHERE expression depending on an alias is rewritten") {
    assertRewritten(
      """MATCH (n)
        |WITH n.prop AS prop WHERE size(n.prop[0]) > 10
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH n.prop AS prop WHERE size(prop[0]) > 10
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("WITH: ORDER BY variable not renamed when it also exists on LHS of an AS") {
    assertNotRewritten(
      """MATCH (x), (y)
        |WITH x AS y, y as z
        |ORDER BY y
        |RETURN y AS y, z AS z
        |""".stripMargin
    )
  }

  test("RETURN: ORDER BY variable not renamed when it also exists on LHS of an AS") {
    assertNotRewritten(
      """MATCH (x), (y)
        |RETURN x AS y, y as z
        |ORDER BY y
        |""".stripMargin
    )
  }

  test("WITH: ORDER BY expression's variable not renamed when it exists on LHS of an AS") {
    assertNotRewritten(
      """MATCH (x), (y)
        |WITH x AS y, y as z
        |ORDER BY foo(y)
        |RETURN y AS y, z AS z
        |""".stripMargin
    )
  }

  test("RETURN: ORDER BY expression's variable not renamed when it exists on LHS of an AS") {
    assertNotRewritten(
      """MATCH (x), (y)
        |RETURN x AS y, y as z
        |ORDER BY foo(y)
        |""".stripMargin
    )
  }

  test("WITH: ORDER BY variable renamed from LHS of AS when RHS exists on another AS's LHS") {
    assertRewritten(
      """MATCH (x), (y)
        |WITH x AS y, y as z
        |ORDER BY x
        |RETURN y AS y, z AS z
        |""".stripMargin,
      """MATCH (x), (y)
        |WITH x AS y, y as z
        |ORDER BY y
        |RETURN y AS y, z AS z
        |""".stripMargin
    )
  }

  test("RETURN: ORDER BY variable renamed from LHS of AS when RHS exists on another AS's LHS") {
    assertRewritten(
      """MATCH (x), (y)
        |RETURN x AS y, y as z
        |ORDER BY x
        |""".stripMargin,
      """MATCH (x), (y)
        |RETURN x AS y, y as z
        |ORDER BY y
        |""".stripMargin
    )
  }

  // 7c. The same alias-resolution applies under WITH * / RETURN *.

  test("WITH *: ORDER BY expression depending on an alias is rewritten") {
    assertRewritten(
      """MATCH (n)
        |WITH *, n.prop AS prop ORDER BY n.prop DESC
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH *, n.prop AS prop ORDER BY prop DESC
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("RETURN *: ORDER BY expression depending on an alias is rewritten") {
    assertRewritten(
      """MATCH (n)
        |RETURN *, n.prop AS prop ORDER BY n.prop DESC
      """.stripMargin,
      """MATCH (n)
        |RETURN *, n.prop AS prop ORDER BY prop DESC
      """.stripMargin
    )
  }

  test("WITH *: WHERE expression depending on an alias is rewritten") {
    assertRewritten(
      """MATCH (n)
        |WITH *, n.prop AS prop WHERE n.prop > 10
        |RETURN prop
      """.stripMargin,
      """MATCH (n)
        |WITH *, n.prop AS prop WHERE prop > 10
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  // 7d. Variable shadowing / swapping between projection items.

  test("RETURN: ORDER BY with swapped aliases (a AS b, b AS a) is not rewritten") {
    assertNotRewritten(
      """
      MATCH (a:A), (b:B)
      RETURN a AS b, b AS a
        ORDER BY b, a
      """.stripMargin
    )
  }

  test("RETURN: ORDER BY variable rewritten to alias even with a swap (a -> b)") {
    assertRewritten(
      """
      MATCH (a:A), (b:B)
      RETURN a AS b, b AS x
        ORDER BY b, a
      """.stripMargin,
      """
      MATCH (a:A), (b:B)
      RETURN a AS b, b AS x
        ORDER BY b, b
      """.stripMargin
    )
  }

  test("RETURN: GROUP BY ALL -> DISTINCT; swapped-alias ORDER BY left intact") {
    assertRewritten(
      """
      MATCH (a:A), (b:B)
      RETURN a AS b, b AS a
        GROUP BY ALL
        ORDER BY b, a
      """.stripMargin,
      """
      MATCH (a:A), (b:B)
      RETURN DISTINCT a AS b, b AS a
        ORDER BY b, a
      """.stripMargin
    )
  }

  test("RETURN: ORDER BY expression rewritten to alias 'a' despite a AS b swap") {
    assertRewritten(
      """MATCH (a:A), (b:B)
        |RETURN a AS b, b.p + 1 AS a
        |  ORDER BY b.p + 1""".stripMargin,
      """MATCH (a:A), (b:B)
        |RETURN a AS b, b.p + 1 AS a
        |  ORDER BY a""".stripMargin
    )
  }

  test("RETURN: ORDER BY expression not matching any item not rewritten (with swap)") {
    assertNotRewritten(
      """MATCH (a:A), (b:B)
        |RETURN a AS b, b.p + 1 AS a
        |  ORDER BY b.p + 1 + 1""".stripMargin
    )
  }

  // 8a. Aliases must not be captured inside scoped subexpressions of ORDER BY.

  test("RETURN: alias not captured inside ORDER BY list comprehension scope") {
    assertNotRewritten(
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY 1 IN [var0 IN [1,2] WHERE true]
      """.stripMargin
    )
  }

  test("RETURN: alias not captured inside ORDER BY any() scope") {
    assertNotRewritten(
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY any(var0 IN [1, 2] WHERE true)
      """.stripMargin
    )
  }

  test("RETURN: alias not captured inside ORDER BY none() scope") {
    assertNotRewritten(
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY none(var0 IN [1, 2] WHERE true)
      """.stripMargin
    )
  }

  test("RETURN: alias not captured inside ORDER BY EXISTS scope") {
    assertNotRewritten(
      """MATCH ()
        |RETURN true AS var0
        |ORDER BY EXISTS { RETURN true AS res }
      """.stripMargin
    )
  }

  test("WITH: alias not captured inside ORDER BY list comprehension scope") {
    assertNotRewritten(
      """MATCH ()
        |WITH true AS var0
        |ORDER BY 1 IN [var0 IN [1,2] WHERE true]
        |RETURN var0
      """.stripMargin
    )
  }

  test("WITH: alias not captured inside ORDER BY any() scope") {
    assertNotRewritten(
      """MATCH ()
        |WITH true AS var0
        |ORDER BY any(var0 IN [1, 2] WHERE true)
        |RETURN var0
      """.stripMargin
    )
  }

  test("WITH: alias not captured inside ORDER BY none() scope") {
    assertNotRewritten(
      """MATCH ()
        |WITH true AS var0
        |ORDER BY none(var0 IN [1, 2] WHERE true)
        |RETURN var0
      """.stripMargin
    )
  }

  test("WITH: alias not captured inside ORDER BY EXISTS scope") {
    assertNotRewritten(
      """MATCH ()
        |WITH true AS var0
        |ORDER BY EXISTS { RETURN true AS res }
        |RETURN var0
      """.stripMargin
    )
  }

  // 8b. Do not rewrite when the alias may be redefined within the same projection.

  test("WITH: ORDER BY already referencing the alias is not rewritten") {
    assertNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY prop
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("WITH: WHERE already referencing the alias is not rewritten") {
    assertNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop WHERE prop
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("WITH: WHERE not rewritten when alias may be redefined in same projection") {
    assertNotRewritten(
      """WITH -0.5 as pa0
        |WITH 1 AS pa0, pa0 as pa1
        |WHERE -1 = pa0
        |RETURN pa0 AS pa3
      """.stripMargin
    )
  }

  test("WITH: WHERE not rewritten when redefined alias is negated") {
    assertNotRewritten(
      """WITH -0.5 as pa0
        |WITH 1 AS pa0, -pa0 as pa1
        |WHERE -1 = -pa0
        |RETURN pa0 AS pa3
      """.stripMargin
    )
  }

  test("RETURN: ORDER BY not rewritten when redefined alias is negated") {
    assertNotRewritten(
      """WITH -0.5 as pa0
        |RETURN 1 AS pa0, -pa0 as pa1
        |ORDER BY -1 = -pa0
      """.stripMargin
    )
  }

  test("RETURN: ORDER BY rewritten on exact match of redefined expression (-pa0 -> pa1)") {
    assertRewritten(
      """WITH -0.5 as pa0
        |RETURN 1 AS pa0, -pa0 as pa1
        |  ORDER BY -pa0
      """.stripMargin,
      """WITH -0.5 as pa0
        |RETURN 1 AS pa0, -pa0 as pa1
        |  ORDER BY pa1
      """.stripMargin
    )
  }

  test("RETURN: GROUP BY ALL -> DISTINCT; ORDER BY -pa0 left intact (redefined)") {
    assertRewritten(
      """WITH -0.5 as pa0
        |RETURN 1 AS pa0, -pa0 as pa1
        |  GROUP BY pa0, pa1
        |  ORDER BY -pa0
      """.stripMargin,
      """WITH -0.5 as pa0
        |RETURN DISTINCT 1 AS pa0, -pa0 as pa1
        |  ORDER BY -pa0
      """.stripMargin
    )
  }

  test("RETURN: ORDER BY not rewritten when redefined alias is a negated property") {
    assertNotRewritten(
      """WITH {p: -0.5} as pa0
        |RETURN {p: 1} AS pa0, -pa0.p as pa1
        |ORDER BY -1 = -pa0.p
      """.stripMargin
    )
  }

  test("WITH: ORDER BY/WHERE not rewritten when redefined alias is a property") {
    assertNotRewritten(
      """WITH {p: -0.5} as pa0
        |WITH {p: 1} AS pa0, pa0.p as pa1
        |ORDER BY -1 = pa0.p
        |WHERE -1 = pa0.p
        |RETURN pa0 AS pa0
      """.stripMargin
    )
  }

  test("RETURN: ORDER BY not rewritten when redefined alias is a property") {
    assertNotRewritten(
      """WITH {p: -0.5} as pa0
        |RETURN {p: 1} AS pa0, pa0.p as pa1
        |ORDER BY -1 = pa0.p
      """.stripMargin
    )
  }

  test("RETURN: ORDER BY not rewritten when redefined alias is a variable") {
    assertNotRewritten(
      """WITH 0.5 as pa0
        |RETURN 1 AS pa0, pa0 as pa1
        |ORDER BY -1 = -pa0
      """.stripMargin
    )
  }

  test("WITH: WHERE not rewritten when redefined alias is negated (aggregating)") {
    assertNotRewritten(
      """WITH -0.5 as pa0
        |WITH 1 AS pa0, -pa0 as pa1, count(*) AS cnt
        |  WHERE -1 = -pa0
        |RETURN pa0 AS pa3
      """.stripMargin
    )
  }

  test("WITH: GROUP BY ALL dropped; WHERE -pa0 left intact (redefined, aggregating)") {
    assertRewritten(
      """WITH -0.5 as pa0
        |WITH 1 AS pa0, -pa0 as pa1, count(*) AS cnt
        |  GROUP BY ALL
        |  WHERE -1 = -pa0
        |RETURN pa0 AS pa3
      """.stripMargin,
      """WITH -0.5 as pa0
        |WITH 1 AS pa0, -pa0 as pa1, count(*) AS cnt
        |  WHERE -1 = -pa0
        |RETURN pa0 AS pa3
      """.stripMargin
    )
  }

  test("WITH: GROUP BY ALL dropped; WHERE -pa1 left intact (no replacement, aggregating)") {
    assertRewritten(
      """WITH -0.5 as pa0
        |WITH 1 AS pa0, -pa0 as pa1, count(*) AS cnt
        |  GROUP BY ALL
        |  WHERE -1 = -pa1
        |RETURN pa0 AS pa3
      """.stripMargin,
      """WITH -0.5 as pa0
        |WITH 1 AS pa0, -pa0 as pa1, count(*) AS cnt
        |  WHERE -1 = -pa1
        |RETURN pa0 AS pa3
      """.stripMargin
    )
  }

  test("WITH DISTINCT: WHERE not rewritten when redefined alias is negated") {
    assertNotRewritten(
      """WITH -0.5 as pa0
        |WITH DISTINCT 1 AS pa0, -pa0 as pa1
        |WHERE -1 = -pa0
        |RETURN pa0 AS pa3
      """.stripMargin
    )
  }

  test("RETURN DISTINCT: ORDER BY rewrites only the non-redefined alias (x -> y)") {
    assertRewritten(
      """WITH {p: -0.5} as pa0, 1 AS x
        |RETURN DISTINCT {p: 1} AS pa0, pa0.p as pa1, x AS y
        |ORDER BY -1 = pa0.p + x
      """.stripMargin,
      """WITH {p: -0.5} as pa0, 1 AS x
        |RETURN DISTINCT {p: 1} AS pa0, pa0.p as pa1, x AS y
        |ORDER BY -1 = pa0.p + y
      """.stripMargin
    )
  }

  test("RETURN DISTINCT + GROUP BY ALL: ORDER BY rewrites only the non-redefined alias (x -> y)") {
    assertRewritten(
      """WITH {p: -0.5} as pa0, 1 AS x
        |RETURN DISTINCT {p: 1} AS pa0, pa0.p as pa1, x AS y
        |  GROUP BY ALL
        |  ORDER BY -1 = pa0.p + x
      """.stripMargin,
      """WITH {p: -0.5} as pa0, 1 AS x
        |RETURN DISTINCT {p: 1} AS pa0, pa0.p as pa1, x AS y
        |ORDER BY -1 = pa0.p + y
      """.stripMargin
    )
  }

  // 8c. Do not rewrite when the subclause expression depends on a non-aliased variable.

  test("WITH: ORDER BY depending on a non-aliased variable is not rewritten") {
    assertNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop ORDER BY n.foo DESC
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("WITH: WHERE depending on a non-aliased variable is not rewritten") {
    assertNotRewritten(
      """MATCH (n)
        |WITH n.prop AS prop WHERE n.foo > 10
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("WITH *: ORDER BY depending on a non-aliased variable is not rewritten") {
    assertNotRewritten(
      """MATCH (n)
        |WITH *, n.prop AS prop ORDER BY n.foo DESC
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  test("WITH *: WHERE depending on a non-aliased variable is not rewritten") {
    assertNotRewritten(
      """MATCH (n)
        |WITH *, n.prop AS prop WHERE n.foo > 10
        |RETURN prop AS prop
      """.stripMargin
    )
  }

  // 9. GROUP BY lowering: additional RETURN shapes and nested scopes.

  test("RETURN: multiple aggregations with all GROUP BY keys projected drops GROUP BY") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |RETURN a, count(*) AS cnt, sum(b) AS s
        |  GROUP BY a""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |RETURN a, count(*) AS cnt, sum(b) AS s""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: GROUP BY () with only an aggregation drops GROUP BY") {
    assertRewritten(
      """UNWIND [1, 2] AS x
        |RETURN count(x) AS c
        |  GROUP BY ()""".stripMargin,
      """UNWIND [1, 2] AS x
        |RETURN count(x) AS c""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: GROUP BY inside a CALL subquery is dropped when keys are projected") {
    assertRewritten(
      """UNWIND [1, 2, 3] AS x
        |CALL (x) {
        |  RETURN x AS gx, count(*) AS c
        |    GROUP BY x
        |}
        |RETURN gx, c""".stripMargin,
      """UNWIND [1, 2, 3] AS x
        |CALL (x) {
        |  RETURN x AS gx, count(*) AS c
        |}
        |RETURN gx, c""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // 10. GROUP BY with a WHERE subclause: reuse the aggregation alias, else hoist the aggregating expression.

  test("WITH: GROUP BY with WHERE filtering on the aggregation alias") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |WITH a, count(b) AS cnt
        |  GROUP BY a
        |  WHERE cnt > 0
        |RETURN a, cnt""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |WITH a, count(b) AS cnt
        |  WHERE cnt > 0
        |RETURN a, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("WITH: GROUP BY with an aggregating WHERE expression is hoisted") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |WITH a, count(b) AS cnt
        |  GROUP BY a
        |  WHERE sum(b) > 0
        |RETURN a, cnt""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |WITH a AS a, count(b) AS cnt, sum(b) > 0 AS `  UNNAMED0`
        |WITH a AS a, cnt AS cnt
        |  WHERE `  UNNAMED0`
        |RETURN a, cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // 11. GROUP BY combined with RETURN * / WITH *. No withUpdate() here: it would
  //     normalize away the AdditiveProjection (`*`) and hide the actual lowering.

  test("RETURN *: explicit GROUP BY is dropped when all keys are projected") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |RETURN *, count(*) AS cnt
        |  GROUP BY a, b""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |RETURN *, count(*) AS cnt""".stripMargin
    )
  }

  test("RETURN *: GROUP BY ALL is dropped") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |RETURN *, count(*) AS cnt
        |  GROUP BY ALL""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |RETURN *, count(*) AS cnt""".stripMargin
    )
  }

  test("WITH *: explicit GROUP BY is dropped when all keys are projected") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |WITH *, count(*) AS cnt
        |  GROUP BY a, b
        |RETURN cnt""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |WITH *, count(*) AS cnt
        |RETURN cnt""".stripMargin
    )
  }

  test("RETURN *: GROUP BY without aggregation becomes RETURN DISTINCT") {
    assertRewritten(
      """WITH 1 AS a, 2 AS b
        |RETURN *
        |  GROUP BY a, b""".stripMargin,
      """WITH 1 AS a, 2 AS b
        |RETURN DISTINCT *""".stripMargin
    )
  }

  test("RETURN *: shadowing explicit item with GROUP BY ALL is dropped") {
    assertRewritten(
      """WITH 1 AS x, 2 AS y
        |RETURN *, 10 AS x, count(*) AS cnt
        |  GROUP BY ALL""".stripMargin,
      """WITH 1 AS x, 2 AS y
        |RETURN *, 10 AS x, count(*) AS cnt""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN *: shadowing explicit item, GROUP BY dropped and ORDER BY on the shadowing alias intact") {
    assertRewritten(
      """WITH 1 AS x, 2 AS y
        |RETURN *, 10 AS x, count(*) AS cnt
        |  GROUP BY x, y
        |  ORDER BY x""".stripMargin,
      """WITH 1 AS x, 2 AS y
        |RETURN *, 10 AS x, count(*) AS cnt
        |  ORDER BY x""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // 12. GROUP BY: an ORDER BY referencing a grouping key reuses the key's column alias,
  //     using the scope survey so an unambiguous key is rewritten while a shadowed one is left alone.

  test("RETURN: ORDER BY an inset (non-projected) grouping key reuses the generated grouping alias") {
    assertRewritten(
      """WITH {p: 1} AS a
        |RETURN count(*) AS cnt
        |  GROUP BY a.p
        |  ORDER BY a.p""".stripMargin,
      """WITH {p: 1} AS a
        |WITH a.p AS `  UNNAMED0`, count(*) AS cnt
        |RETURN cnt
        |  ORDER BY `  UNNAMED0`""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY a projected grouping key reuses its alias") {
    assertRewritten(
      """WITH {p: 1} AS a
        |RETURN a.p AS p, count(*) AS cnt
        |  GROUP BY a.p
        |  ORDER BY a.p""".stripMargin,
      """WITH {p: 1} AS a
        |RETURN a.p AS p, count(*) AS cnt
        |  ORDER BY p""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY a complex grouping key reuses its alias (AST-equal)") {
    assertRewritten(
      """WITH 1 AS b
        |RETURN b + 1 AS b1, count(*) AS cnt
        |  GROUP BY b + 1
        |  ORDER BY b + 1""".stripMargin,
      """WITH 1 AS b
        |RETURN b + 1 AS b1, count(*) AS cnt
        |  ORDER BY b1""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  // Counter-case: a shadowing alias `a` makes `a.p` in ORDER BY ambiguous,
  // so the full expression must NOT be substituted to the grouping-key alias `p`.
  test("RETURN: ORDER BY an ambiguous grouping-key reference is not substituted") {
    assertRewritten(
      """WITH {p: 1} AS a
        |RETURN a.p AS p, {p: -1} AS a, count(*) AS cnt
        |  GROUP BY a.p
        |  ORDER BY a.p""".stripMargin,
      """WITH {p: 1} AS a
        |RETURN a.p AS p, {p: -1} AS a, count(*) AS cnt
        |  ORDER BY a.p""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY a list-comprehension grouping key reuses its alias") {
    assertRewritten(
      """WITH [1, 2, 3] AS a
        |RETURN [x IN a | x] AS l, count(*) AS cnt
        |  GROUP BY [x IN a | x]
        |  ORDER BY [x IN a | x]""".stripMargin,
      """WITH [1, 2, 3] AS a
        |RETURN [x IN a | x] AS l, count(*) AS cnt
        |  ORDER BY l""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY a shadowed list-comprehension grouping key is not substituted") {
    assertRewritten(
      """WITH [1, 2, 3] AS a
        |RETURN [x IN a | x] AS l, [-1] AS a, count(*) AS cnt
        |  GROUP BY [x IN a | x]
        |  ORDER BY [x IN a | x]""".stripMargin,
      """WITH [1, 2, 3] AS a
        |RETURN [x IN a | x] AS l, [-1] AS a, count(*) AS cnt
        |  ORDER BY [x IN a | x]""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY a COUNT-subquery grouping key reuses its alias") {
    assertRewritten(
      """WITH 1 AS a
        |RETURN COUNT { RETURN a } AS c, count(*) AS cnt
        |  GROUP BY COUNT { RETURN a }
        |  ORDER BY COUNT { RETURN a }""".stripMargin,
      """WITH 1 AS a
        |RETURN COUNT { RETURN a } AS c, count(*) AS cnt
        |  ORDER BY c""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }

  test("RETURN: ORDER BY a shadowed COUNT-subquery grouping key is not substituted") {
    assertRewritten(
      """WITH 1 AS a
        |RETURN COUNT { RETURN a } AS c, 2 AS a, count(*) AS cnt
        |  GROUP BY COUNT { RETURN a }
        |  ORDER BY COUNT { RETURN a }""".stripMargin,
      """WITH 1 AS a
        |RETURN COUNT { RETURN a } AS c, 2 AS a, count(*) AS cnt
        |  ORDER BY COUNT { RETURN a }""".stripMargin,
      additionalExpectedAstUpdates = withUpdate(),
      additionalActualAstCleanup = withUpdate()
    )
  }
}
