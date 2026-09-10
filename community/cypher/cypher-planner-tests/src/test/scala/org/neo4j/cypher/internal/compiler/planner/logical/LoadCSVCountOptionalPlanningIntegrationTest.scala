/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.UnwindCollection

import scala.reflect.ClassTag

/**
 * Regression coverage for Neo4j issue #13924:
 * nested COUNT + LOAD CSV inside OPTIONAL MATCH WHERE.
 *
 * Invariant under test: an outer-only read-only COUNT subquery in an OPTIONAL
 * WHERE is evaluated once per outer row, above the OPTIONAL RHS fan-out, via an
 * `Apply` on the outer LHS. The rewritten predicate (`countVar >= 0`) is still
 * applied by a `Selection` inside the OPTIONAL RHS, preserving null-extension
 * semantics. Before the fix the COUNT Apply sat on one Cartesian side inside the
 * OPTIONAL RHS, re-evaluating the nested plan once per RHS candidate row.
 */
class LoadCSVCountOptionalPlanningIntegrationTest extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport {

  private def planWith(
    q: String,
    model: ExecutionModel,
    labelA: Int = 100,
    labelB: Int = 100
  ) = plannerBuilder()
    .setAllNodesCardinality(1000)
    .setLabelCardinality("A", labelA)
    .setLabelCardinality("B", labelB)
    .setRelationshipCardinality("(:A)-[]->(:B)", 10)
    .setRelationshipCardinality("()-[]->()", 10)
    .setExecutionModel(model)
    .build()
    .plan(q)

  test("#13924: minimal COUNT+LOAD CSV in OPTIONAL WHERE uses Apply+Limit, Volcano and Batched agree") {
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row } >= 0
        |RETURN k""".stripMargin
    val volcano = planWith(q, ExecutionModel.Volcano)
    val batched = planWith(q, ExecutionModel.Batched.default)

    Seq(volcano, batched).foreach { plan =>
      plan.folder.findAllByClass[LoadCSV] should have size 1
      plan.folder.findAllByClass[Aggregation] should have size 1
      // COUNT >= 0 is rewritten to Limit 1 over the inner rows.
      plan.folder.findAllByClass[Limit] should have size 1
      plan.folder.findAllByClass[Apply] should not be empty
      plan.folder.findAllByClass[CartesianProduct] should have size 1
      plan.folder.findAllByClass[Optional] should have size 1
    }
    // Planning shape must not diverge between execution models for this shape;
    // the slotted/pipelined delta is runtime per-open cost, not a different join order here.
    volcano.toString shouldEqual batched.toString
  }

  test("#13924: minimal COUNT+UNWIND in OPTIONAL WHERE uses Apply, Volcano and Batched agree") {
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { UNWIND [['a','b'],['c','d']] AS row RETURN row } >= 0
        |RETURN k""".stripMargin
    val volcano = planWith(q, ExecutionModel.Volcano)
    val batched = planWith(q, ExecutionModel.Batched.default)

    Seq(volcano, batched).foreach { plan =>
      plan.folder.findAllByClass[LoadCSV] shouldBe empty
      plan.folder.findAllByClass[UnwindCollection].size should be >= 2
      plan.folder.findAllByClass[Aggregation] should have size 1
      plan.folder.findAllByClass[Apply] should not be empty
    }
    volcano.toString shouldEqual batched.toString
  }

  test("#13924 scaling: hoisted COUNT stays above the fan-out as populations sweep") {
    // The nested evaluation must scale with outer rows only, regardless of RHS
    // candidate population: exactly one LoadCSV/Aggregation above every Optional.
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row } >= 0
        |RETURN k""".stripMargin
    Seq((1, 1), (4, 4), (16, 16), (2, 32), (32, 2)).foreach { case (a, b) =>
      val plan = planWith(q, ExecutionModel.Volcano, a, b)
      withClue(s"A=$a B=$b: ") {
        plan.folder.findAllByClass[LoadCSV] should have size 1
        plan.folder.findAllByClass[Aggregation] should have size 1
        plan.folder.findAllByClass[Limit] should have size 1
        plan.folder.findAllByClass[CartesianProduct] should have size 1
        optionalSubtrees(plan).foreach { optional =>
          optional.folder.findAllByClass[LoadCSV] shouldBe empty
        }
      }
    }
  }

  test("#13924: multiple outer rows keep single nested LOAD CSV (scales with N, not N x M)") {
    Seq(1, 2, 8).foreach { n =>
      val list = (1 to n).mkString("[", ", ", "]")
      val q =
        s"""UNWIND $list AS k
           |OPTIONAL MATCH (a:A), (b:B)
           |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row } >= 0
           |RETURN k""".stripMargin
      val plan = planWith(q, ExecutionModel.Volcano)
      withClue(s"N=$n: ") {
        plan.folder.findAllByClass[LoadCSV] should have size 1
        plan.folder.findAllByClass[Aggregation] should have size 1
      }
    }
  }

  test("#13924: correlated nested LOAD CSV references outer row") {
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row WITH row WHERE row[0] = k RETURN row } >= 0
        |RETURN k""".stripMargin
    Seq(ExecutionModel.Volcano, ExecutionModel.Batched.default).foreach { model =>
      val plan = planWith(q, model)
      plan.folder.findAllByClass[LoadCSV] should have size 1
      plan.folder.findAllByClass[Aggregation] should have size 1
      plan.folder.findAllByClass[Apply] should not be empty
    }
  }

  test("#13924: two nested LOAD CSV operators plan independently") {
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS r1 RETURN r1 } >= 0
        |  AND COUNT { LOAD CSV FROM 'file:///other.csv' AS r2 RETURN r2 } >= 0
        |RETURN k""".stripMargin
    val plan = planWith(q, ExecutionModel.Volcano)
    plan.folder.findAllByClass[LoadCSV] should have size 2
    plan.folder.findAllByClass[Aggregation].size should be >= 2
    // Both outer-only COUNTs hoist above the fan-out (chained on the outer LHS
    // spine under a single root); no CSV or aggregation work remains inside.
    optionalSubtrees(plan).foreach { optional =>
      optional.folder.findAllByClass[LoadCSV] shouldBe empty
      optional.folder.findAllByClass[Aggregation] shouldBe empty
    }
    hoistRoots(plan) should have size 1
  }

  test("#13924: headers vs no-headers nested LOAD CSV") {
    Seq(
      "LOAD CSV FROM 'file:///fuzz.csv' AS row",
      "LOAD CSV WITH HEADERS FROM 'file:///fuzz.csv' AS row"
    ).foreach { load =>
      val q =
        s"""UNWIND [1, 2] AS k
           |OPTIONAL MATCH (a:A), (b:B)
           |WHERE COUNT { $load RETURN row } >= 0
           |RETURN k""".stripMargin
      val plan = planWith(q, ExecutionModel.Volcano)
      withClue(s"$load: ") {
        plan.folder.findAllByClass[LoadCSV] should have size 1
        plan.folder.findAllByClass[Aggregation] should have size 1
      }
    }
  }

  test("#13924: early termination keeps Limit over nested rows") {
    // COUNT > 10 (non-trivial bound) still plans a bounded nested evaluation;
    // COUNT >= 0 always short-circuits to Limit 1.
    Seq(">= 0", "> 10").foreach { cmp =>
      val q =
        s"""UNWIND [1, 2] AS k
           |OPTIONAL MATCH (a:A), (b:B)
           |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row RETURN row } $cmp
           |RETURN k""".stripMargin
      val plan = planWith(q, ExecutionModel.Volcano)
      withClue(s"COUNT $cmp: ") {
        plan.folder.findAllByClass[LoadCSV] should have size 1
        plan.folder.findAllByClass[Aggregation] should have size 1
      }
    }
  }

  test("#13924: normal top-level LOAD CSV unaffected") {
    val q =
      """LOAD CSV FROM 'file:///fuzz.csv' AS row
        |OPTIONAL MATCH (a:A), (b:B)
        |RETURN row""".stripMargin
    val plan = planWith(q, ExecutionModel.Volcano)
    plan.folder.findAllByClass[LoadCSV] should have size 1
    plan.folder.findAllByClass[Aggregation] shouldBe empty
  }

  test("#13924: nested COUNT without CSV (UNWIND-only) stays equivalent across models") {
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { UNWIND [1, 2, 3] AS x RETURN x } >= 0
        |RETURN k""".stripMargin
    val volcano = planWith(q, ExecutionModel.Volcano)
    val batched = planWith(q, ExecutionModel.Batched.default)
    volcano.folder.findAllByClass[LoadCSV] shouldBe empty
    batched.folder.findAllByClass[LoadCSV] shouldBe empty
    volcano.toString shouldEqual batched.toString
  }

  private def optionalSubtrees(plan: LogicalPlan) =
    plan.folder.findAllByClass[Optional]

  private def insideOptional(plan: LogicalPlan, apply: Apply): Boolean =
    optionalSubtrees(plan).exists(_.folder.findAllByClass[Apply].exists(_ eq apply))

  /**
   * Plan nodes of type T under root, following only logical-plan children
   * (lhs/rhs) and never descending into expressions. In particular, nested
   * subplans held by per-row NestedPlanExpression pipes are excluded: only
   * Apply-evaluated (once-per-row) work counts here. (The generic folder
   * traversal sees through nested subplans, which would conflate legacy
   * per-row nested evaluation with hoisted Apply evaluation.)
   */
  private def barePlans[T <: LogicalPlan: ClassTag](root: LogicalPlan): Seq[T] = {
    val found = Seq.newBuilder[T]
    def visit(plan: LogicalPlan): Unit = {
      if (implicitly[ClassTag[T]].runtimeClass.isInstance(plan)) found += plan.asInstanceOf[T]
      plan.lhs.foreach(visit)
      plan.rhs.foreach(visit)
    }
    visit(root)
    found.result()
  }

  /**
   * Roots of hoisted COUNT evaluations: an Apply outside every Optional subtree
   * whose direct right-hand side hosts the Aggregation over the nested rows.
   * Chained hoists (one Apply per hoisted predicate on the outer LHS spine)
   * share a single root; deeper Applies inside a COUNT subquery itself only see
   * Argument/LoadCSV on their right-hand side and are excluded, as is the outer
   * ApplyOptional Apply whose right-hand side is the LoadCSV-free Optional.
   */
  private def hoistRoots(plan: LogicalPlan) = {
    val candidates = barePlans[Apply](plan).filter { apply =>
      barePlans[LoadCSV](apply).nonEmpty &&
      apply.rhs.exists(rhs => barePlans[Aggregation](rhs).nonEmpty) &&
      !insideOptional(plan, apply)
    }
    candidates.filterNot(c => candidates.exists(o => (o ne c) && o.rhs.exists(_.folder.findAllByClass[Apply].exists(_ eq c))))
  }

  test("#13924 hoist: uncorrelated COUNT is evaluated above the OPTIONAL fan-out") {
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row } >= 0
        |RETURN k""".stripMargin
    Seq(ExecutionModel.Volcano, ExecutionModel.Batched.default).foreach { model =>
      val plan = planWith(q, model)
      // The nested CSV work must not sit under the OPTIONAL expansion:
      // no LoadCSV/Aggregation may remain inside any Optional subtree.
      optionalSubtrees(plan) should not be empty
      optionalSubtrees(plan).foreach { optional =>
        optional.folder.findAllByClass[LoadCSV] shouldBe empty
        optional.folder.findAllByClass[Aggregation] shouldBe empty
      }
      // ...while exactly one nested COUNT evaluation still exists above it.
      plan.folder.findAllByClass[LoadCSV] should have size 1
      plan.folder.findAllByClass[Aggregation] should have size 1
      // And the rewritten predicate is still applied inside the OPTIONAL RHS,
      // which is what preserves null-extension semantics: a false (or null)
      // outcome eliminates that outer row's RHS candidates, and the OPTIONAL
      // still emits exactly one null-extended row instead of dropping the row.
      optionalSubtrees(plan).flatMap(_.folder.findAllByClass[Selection]) should not be empty
      // And the hoisted evaluation is a single Apply outside every Optional subtree.
      hoistRoots(plan) should have size 1
    }
  }

  test("#13924 hoist: outer-correlated COUNT is evaluated above the OPTIONAL fan-out") {
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row WITH row WHERE row[0] = k RETURN row } >= 0
        |RETURN k""".stripMargin
    val plan = planWith(q, ExecutionModel.Volcano)
    optionalSubtrees(plan) should not be empty
    optionalSubtrees(plan).foreach { optional =>
      optional.folder.findAllByClass[LoadCSV] shouldBe empty
    }
    plan.folder.findAllByClass[LoadCSV] should have size 1
  }

  test("#13924 hoist: RHS-correlated COUNT stays inside the OPTIONAL RHS") {
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row WITH row WHERE row[1] = a.name RETURN row } >= 0
        |RETURN k""".stripMargin
    val plan = planWith(q, ExecutionModel.Volcano)
    // The COUNT depends on `a`, introduced by the OPTIONAL pattern, so it must
    // keep its existing in-RHS evaluation: some Optional subtree still hosts it.
    optionalSubtrees(plan).map(_.folder.findAllByClass[LoadCSV].size).sum should be >= 1
    plan.folder.findAllByClass[LoadCSV] should have size 1
  }

  test("#13924 hoist: mixed predicates split outer COUNT from RHS predicate") {
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row } >= 0 AND a.prop > 10
        |RETURN k""".stripMargin
    val plan = planWith(q, ExecutionModel.Volcano)
    // Outer-only COUNT moves above the fan-out ...
    optionalSubtrees(plan).foreach { optional =>
      optional.folder.findAllByClass[LoadCSV] shouldBe empty
    }
    plan.folder.findAllByClass[LoadCSV] should have size 1
    // ... while the RHS-dependent remainder still constrains the OPTIONAL match.
    plan.folder.findAllByClass[CartesianProduct] should have size 1
  }

  test("#13924 hoist: COUNT under CASE keeps legacy per-row nested evaluation") {
    // A COUNT nested inside CASE cannot be unnested to an Apply by the ordinary
    // subquery machinery (it becomes a per-row nested pipe), so the hoist must
    // leave it alone: the CSV work stays inside the OPTIONAL RHS in its legacy
    // form (here still as an unresolved COUNT expression inside the Filter).
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE CASE WHEN k > 0 THEN COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row } >= 0 ELSE false END
        |RETURN k""".stripMargin
    val plan = planWith(q, ExecutionModel.Volcano)
    // The legacy in-RHS evaluation site is preserved ...
    optionalSubtrees(plan).map(_.folder.findAllByClass[LoadCSV].size).sum should be >= 1
    // ... and no Apply-based hoist is manufactured for the blocked COUNT.
    hoistRoots(plan) shouldBe empty
  }

  test("#13924 hoist: EXISTS conjunct stays while outer COUNT hoists") {
    val q =
      """UNWIND [1, 2] AS k
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row } >= 0 AND EXISTS { (a)-->(b) }
        |RETURN k""".stripMargin
    val plan = planWith(q, ExecutionModel.Volcano)
    // The COUNT part still hoists above the fan-out ...
    optionalSubtrees(plan).foreach { optional =>
      optional.folder.findAllByClass[LoadCSV] shouldBe empty
    }
    plan.folder.findAllByClass[LoadCSV] should have size 1
    // ... while the OPTIONAL shape (and its EXISTS handling) is preserved:
    // the EXISTS pattern still expands inside the RHS, not above it.
    plan.folder.findAllByClass[Expand].size should be >= 1
    optionalSubtrees(plan) should not be empty
  }

  test("#13924 hoist: outer writes do not block the hoist and keep their order") {
    val q =
      """UNWIND [1, 2] AS k
        |CREATE (n:N {p: k})
        |WITH k, n
        |OPTIONAL MATCH (a:A), (b:B)
        |WHERE COUNT { LOAD CSV FROM 'file:///fuzz.csv' AS row } >= 0
        |RETURN k""".stripMargin
    val plan = planWith(q, ExecutionModel.Volcano)
    // The hoisted COUNT Apply chains after the outer writes; evaluation stays
    // once per outer row and never crosses the updating boundary.
    optionalSubtrees(plan).foreach { optional =>
      optional.folder.findAllByClass[LoadCSV] shouldBe empty
    }
    plan.folder.findAllByClass[LoadCSV] should have size 1
    plan.folder.findAllByClass[Aggregation] should have size 1
  }
}
