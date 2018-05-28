/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_2.planner._
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_2.ast.{AstConstructionTestSupport, Equals}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_2._

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
          val (a, b, c) = (l.head, l(1), l(2))
          CartesianProduct(
            a,
            CartesianProduct(
              b,
              c
            )(solved)
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
        List((planA, "a"), (planB, "b"), (planC, "c")).permutations.map { l =>
          val ((a, aName), (b, bName), (c, cName)) = (l.head, l(1), l(2))
        Selection(Seq(Equals(prop(aName, "id"), prop(cName, "id"))(pos)),
          ValueHashJoin(a,
            ValueHashJoin(b, c, Equals(prop(bName, "id"), prop(cName, "id"))(pos))(solved), Equals(prop(aName, "id"), prop(bName, "id"))(pos))(solved))(solved)
        }.toSeq : _*)
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

  private def testThis(graph: QueryGraph, input: Set[PlannedComponent], expectedPlans: LogicalPlan*): Unit =
    testThis(graph, input, (result: LogicalPlan) => expectedPlans should contain(result))
}
