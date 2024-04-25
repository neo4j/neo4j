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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.EagerWhereNeededRewriter.ChildrenIds
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.LogicalBinaryPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlans
import org.neo4j.cypher.internal.logical.plans.LogicalUnaryPlan
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.Extractors.SetExtractor

class ConflictFinderTest extends CypherFunSuite with AstConstructionTestSupport {

  // -------------------------
  // readMightNotBeInitialized
  // -------------------------

  test("readMightNotBeInitialized for unary plan is always false") {
    val wholePlan = new LogicalPlanBuilder()
      .produceResults("x")
      .distinct("x AS x")
      .nodeByLabelScan("x", "X")
      .build()

    val childrenIds: ChildrenIds = childrenIdsForPlan(wholePlan)

    (wholePlan #:: LazyList.unfold(wholePlan) {
      case p: LogicalUnaryPlan => Some((p.source, p.source))
      case _                   => None
    }).foreach { p =>
      withClue(p) {
        childrenIds.readMightNotBeInitialized(p) should be(false)
      }
    }
  }

  test("readMightNotBeInitialized going left in a plan is always false") {
    val wholePlan = new LogicalPlanBuilder()
      .produceResults("x")
      .apply()
      .|.argument()
      .nodeHashJoin("x")
      .|.nodeByLabelScan("x", "X")
      .nodeByLabelScan("x", "X")
      .build()

    val childrenIds: ChildrenIds = childrenIdsForPlan(wholePlan)

    (wholePlan #:: LazyList.unfold(wholePlan) {
      case p: LogicalUnaryPlan  => Some((p.source, p.source))
      case p: LogicalBinaryPlan => Some((p.left, p.left))
      case _                    => None
    }).foreach { p =>
      withClue(p) {
        childrenIds.readMightNotBeInitialized(p) should be(false)
      }
    }
  }

  test("readMightNotBeInitialized is true right of some binary plans") {
    val wholePlan = new LogicalPlanBuilder()
      .produceResults("x")
      .apply()
      .|.nodeHashJoin("here") // Expect `true` here
      .|.|.argument("here") // Expect `true` here
      .|.argument("here") // Expect `true` here
      .cartesianProduct()
      .|.argument("here") // Expect `true` here
      .nodeHashJoin("x")
      .|.union()
      .|.|.argument("here") // Expect `true` here
      .|.orderedUnion()
      .|.|.argument("not here")
      .|.argument("not here")
      .argument("not here")
      .build()

    val childrenIds: ChildrenIds = childrenIdsForPlan(wholePlan)

    wholePlan.folder.treeFold(()) {
      case p @ Argument(SetExtractor(Variable("here"))) =>
        withClue(p) {
          childrenIds.readMightNotBeInitialized(p) should be(true)
        }
        acc => SkipChildren(acc)

      case p @ NodeHashJoin(SetExtractor(Variable("here")), _, _) =>
        withClue(p) {
          childrenIds.readMightNotBeInitialized(p) should be(true)
        }
        acc => SkipChildren(acc)

      case p: LogicalPlan =>
        withClue(p) {
          childrenIds.readMightNotBeInitialized(p) should be(false)
        }
        acc => TraverseChildren(acc)

      case _ => acc => SkipChildren(acc)
    }
  }

  // ------------------
  // mostDownstreamPlan
  // ------------------

  test("mostDownstreamPlan in a linear plan") {
    val wholePlan = new LogicalPlanBuilder()
      .produceResults("x")
      .distinct("x AS x")
      .nodeByLabelScan("x", "X")
      .build()

    val childrenIds: ChildrenIds = childrenIdsForPlan(wholePlan)

    val ds = wholePlan.lhs.get
    val nbls = ds.lhs.get

    val inOrder = Seq(nbls, ds, wholePlan)

    inOrder.toSet.subsets().foreach {
      case SetExtractor() => // Do nothing
      case plans =>
        val expected = plans.toSeq.maxBy(inOrder.indexOf)
        childrenIds.mostDownstreamPlan(plans.toSeq: _*) should be(expected)
    }
  }

  test("mostDownstreamPlan in a binary plan") {
    val wholePlan = new LogicalPlanBuilder()
      .produceResults("x")
      .apply() // 2
      .|.nodeHashJoin("x")
      .|.|.argument("4")
      .|.argument("3")
      .apply() // 1
      .|.argument("2")
      .argument("1")
      .build()

    val childrenIds: ChildrenIds = childrenIdsForPlan(wholePlan)

    val a2 = wholePlan.lhs.get
    val nhj = a2.rhs.get
    val arg4 = nhj.rhs.get
    val arg3 = nhj.lhs.get
    val a1 = a2.lhs.get
    val arg2 = a1.rhs.get
    val arg1 = a1.lhs.get

    val inOrder = Seq(arg1, arg2, a1, arg3, arg4, nhj, a2, wholePlan)

    inOrder.toSet.subsets().foreach {
      case SetExtractor() => // Do nothing
      case plans =>
        val expected = plans.toSeq.maxBy(inOrder.indexOf)
        childrenIds.mostDownstreamPlan(plans.toSeq: _*) should be(expected)
    }
  }

  // ----------------------------
  // containsNestedPlanExpression
  // ----------------------------

  private def makeNestedPlanExpression: NestedPlanExpression = {
    val nestedPlan = new LogicalPlanBuilder(wholePlan = false)
      .allNodeScan("x")
      .build()

    NestedPlanExistsExpression(nestedPlan, "exists { (x) }")(pos)
  }

  test("containsNestedPlanExpression for Create without a nested plan") {
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .create(createNodeWithProperties("n", Seq.empty, "{prop: a.name}"))
      .allNodeScan("a")
      .build()

    ConflictFinder.containsNestedPlanExpression(plan) shouldBe false
  }

  test("containsNestedPlanExpression for Create with a nested plan") {
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .create(createNodeWithProperties("n", Seq.empty, mapOf("prop" -> makeNestedPlanExpression)))
      .allNodeScan("a")
      .build()

    ConflictFinder.containsNestedPlanExpression(plan) shouldBe true
  }

  test("containsNestedPlanExpression for Selection without a nested plan") {
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .filter("a.prop = 123")
      .allNodeScan("a")
      .build()

    ConflictFinder.containsNestedPlanExpression(plan) shouldBe false
  }

  test("containsNestedPlanExpression for Selection with a nested plan") {
    val plan = new LogicalPlanBuilder(wholePlan = false)
      .filterExpression(makeNestedPlanExpression)
      .allNodeScan("a")
      .build()

    ConflictFinder.containsNestedPlanExpression(plan) shouldBe true
  }

  private def childrenIdsForPlan(lp: LogicalPlan): ChildrenIds = {
    val childrenIds = new ChildrenIds
    LogicalPlans.simpleFoldPlan(())(lp, (_, p) => childrenIds.recordChildren(p))
    childrenIds
  }
}
