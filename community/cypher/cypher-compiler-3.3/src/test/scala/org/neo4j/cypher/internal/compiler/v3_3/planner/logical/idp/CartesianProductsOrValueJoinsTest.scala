/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_3.planner._
import org.neo4j.cypher.internal.frontend.v3_3.ast.{AstConstructionTestSupport, Equals}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3._
import org.neo4j.cypher.internal.v3_3.logical.plans._

class CartesianProductsOrValueJoinsTest
  extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {
  val planA = allNodesScan("a")
  val planB = allNodesScan("b")
  val planC = allNodesScan("c")

  private def allNodesScan(n: String): LogicalPlan = {
    // The cardinality is tied to the first letter in the node variables. n.head is the first char of the variable name.
    // 'a' has number ASCII number 97, so that would produce cardinality 1000
    val cardinality = (n.head.toInt - 96) * 1000
    val solved = CardinalityEstimation.lift(RegularPlannerQuery(queryGraph = QueryGraph(patternNodes = Set(n))), Cardinality(cardinality))
    AllNodesScan(n, Set.empty)(solved)
  }

  test("should plan cartesian product between 2 pattern nodes") {
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b")),
      input = Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), planA),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), planB)),
      expectedPlans =
        List(planA, planB).permutations.map { l =>
          val (a, b) = (l.head, l(1))
          CartesianProduct(
            planA,
            planB
          )(solved)
        }.toSeq: _*)
  }

  test("should plan cartesian product between 3 pattern nodes") {
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b", "c")),
      input = Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), planA),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), planB),
        PlannedComponent(QueryGraph(patternNodes = Set("c")), planC)),
      expectedPlans =
        List(planA, planB, planC).permutations.map { l =>
          val (x, y, z) = (l.head, l(1), l(2))
          CartesianProduct(
            CartesianProduct(
              y,
              z
            )(solved),
            x
          )(solved)
        }.toSeq : _*)
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
      expectedPlans = ValueHashJoin(planA, planB, equality)(solved))
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
      expectedPlans =
        Selection(Seq(Equals(prop("a", "id"), prop("c", "id"))(pos)),
          ValueHashJoin(
            ValueHashJoin(
              planA,
              planB, Equals(prop("a", "id"), prop("b", "id"))(pos))(solved),
            planC, Equals(prop("b", "id"), prop("c", "id"))(pos))(solved))(solved))
  }

  private def testThis(graph: QueryGraph, input: Set[PlannedComponent], assertion: LogicalPlan => Unit): Unit = {
    new given {
      qg = graph
      cardinality = mapCardinality {
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set("a") => 1000.0
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set("c") => 3000.0
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

  private def testThis(graph: QueryGraph, input: Set[PlannedComponent], expectedPlans: LogicalPlan*): Unit =
    testThis(graph, input, (result: LogicalPlan) => expectedPlans should contain(result))
}
