/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.frontend.v3_4.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.util.v3_4.Cardinality
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.Equals
import org.neo4j.cypher.internal.v3_4.logical.plans._

class CartesianProductsOrValueJoinsTest
  extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {
  val planA = allNodesScan("a")
  val planB = allNodesScan("b")
  val planC = allNodesScan("c")

  private def allNodesScan(n: String): LogicalPlan = {
    val solved = CardinalityEstimation.lift(RegularPlannerQuery(queryGraph = QueryGraph(patternNodes = Set(IdName(n)))), Cardinality(0))
    AllNodesScan(n, Set.empty)(solved)
  }

  test("should plan cartesian product between 2 pattern nodes") {
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b")),
      input = Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), planA),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), planB)),
      expectedPlan = CartesianProduct(
        planA,
        planB
      )(solved)
    )
  }

  test("should plan cartesian product between 3 pattern nodes") {
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b", "c")),
      input = Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), planA),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), planB),
        PlannedComponent(QueryGraph(patternNodes = Set("c")), planC)),
      expectedPlan = CartesianProduct(
        planC,
        CartesianProduct(
          planB,
          planA
        )(solved)
      )(solved))
  }

  test("should plan cartesian product between lots of pattern nodes") {
    val components = ('a' to 'z') map { x =>
      PlannedComponent(QueryGraph(patternNodes = Set(x.toString)), allNodesScan(x.toString))
    }

    val includedPlans = components.map(_.plan).toSet

    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b", "c")),
      input = components.toSet,
      assertion = (x: LogicalPlan) => {
        val leaves = x.leaves
        leaves.toSet should equal(includedPlans)
        leaves.size should equal(components.size)
      }
    )
  }

  test("should plan hash join between 2 pattern nodes") {
    val equality = Equals(prop("a", "id"), prop("b", "id"))(pos)

    testThis(
      graph = QueryGraph(
        patternNodes = Set("a", "b"),
        selections = Selections.from(equality)),
      input = Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), planA),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), planB)),
      expectedPlan = ValueHashJoin(planA, planB, equality)(solved))
  }

  test("should plan hash joins between 3 pattern nodes") {
    val eq1 = Equals(prop("b", "id"), prop("a", "id"))(pos)
    val eq2 = Equals(prop("b", "id"), prop("c", "id"))(pos)
    val eq3 = Equals(prop("a", "id"), prop("c", "id"))(pos)

    testThis(
      graph = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(Seq(eq1, eq2, eq3))),
      input = Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), planA),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), planB),
        PlannedComponent(QueryGraph(patternNodes = Set("c")), planC)),
      expectedPlan =
        Selection(Seq(eq3),
          ValueHashJoin(planA,
            ValueHashJoin(planB, planC, eq2)(solved), eq1.switchSides)(solved))(solved))
  }

  test("should recognize value joins") {
    // given WHERE x.id = z.id
    val lhs = prop("x", "id")
    val rhs = prop("z", "id")
    val equalityComparison = Equals(lhs, rhs)(pos)
    val selections = Selections.from(equalityComparison)

    // when
    val result = cartesianProductsOrValueJoins.predicatesForPotentialValueJoins(selections)

    // then
    result should equal(Set(equalityComparison))
  }

  test("if one side is a literal, it's not a value join") {
    // given WHERE x.id = 42
    val selections = Selections.from(propEquality("x","id", 42))

    // when
    val result = cartesianProductsOrValueJoins.predicatesForPotentialValueJoins(selections)

    // then
    result should be(empty)
  }

  test("if both lhs and rhs come from the same variable, it's not a value join") {
    // given WHERE x.id1 = x.id2
    val lhs = prop("x", "id1")
    val rhs = prop("x", "id2")
    val equalityComparison = Equals(lhs, rhs)(pos)
    val selections = Selections.from(equalityComparison)

    // when
    val result = cartesianProductsOrValueJoins.predicatesForPotentialValueJoins(selections)

    // then
    result should be(empty)
  }

  test("combination of predicates is not a problem") {
    // given WHERE x.id1 = z.id AND x.id1 = x.id2 AND x.id2 = 42
    val x_id1 = prop("x", "id1")
    val x_id2 = prop("x", "id2")
    val z_id = prop("z", "id")
    val lit = literalInt(42)

    val pred1 = Equals(x_id1, x_id2)(pos)
    val pred2 = Equals(x_id1, z_id)(pos)
    val pred3 = Equals(x_id2, lit)(pos)

    val selections = Selections.from(Seq(pred1, pred2, pred3))

    // when
    val result = cartesianProductsOrValueJoins.predicatesForPotentialValueJoins(selections)

    // then
    result should be(Set(pred2))
  }

  private def testThis(graph: QueryGraph, input: Set[PlannedComponent], assertion: LogicalPlan => Unit): Unit = {
    new given {
      qg = graph
      cardinality = mapCardinality {
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set(IdName("a")) => 1000.0
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set(IdName("b")) => 2000.0
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set(IdName("c")) => 3000.0
        case _ => 100.0
      }
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx
      implicit val kit = ctx.config.toKit()

      var plans: Set[PlannedComponent] = input
      while (plans.size > 1) {
        plans = cartesianProductsOrValueJoins(plans, cfg.qg)(ctx, kit, SingleComponentPlanner(mock[IDPQueryGraphSolverMonitor]))
      }

      val result = plans.head.plan

      assertion(result)
    }
  }

  private def testThis(graph: QueryGraph, input: Set[PlannedComponent], expectedPlan: LogicalPlan): Unit =
    testThis(graph, input, (result: LogicalPlan) => result should equal(expectedPlan))
}
