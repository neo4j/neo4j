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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_4.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.Cardinality
import org.neo4j.cypher.internal.v3_4.expressions.Equals

class CartesianProductsOrValueJoinsTest
  extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {
  val planA = allNodesScan("a")
  val planB = allNodesScan("b")
  val planC = allNodesScan("c")

  private def allNodesScan(n: String, solveds: Solveds = new Solveds, cardinalities: Cardinalities = new Cardinalities): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph(patternNodes = Set((n))))
    val cardinality = Cardinality(0)
    val res = AllNodesScan(n, Set.empty)
    solveds.set(res.id, solved)
    cardinalities.set(res.id, cardinality)
    res
  }

  test("should plan cartesian product between 2 pattern nodes") {
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b")),
      input = (solveds: Solveds, cardinalities: Cardinalities) => Set(
          PlannedComponent(QueryGraph(patternNodes = Set("a")), allNodesScan("a", solveds, cardinalities)),
          PlannedComponent(QueryGraph(patternNodes = Set("b")), allNodesScan("b", solveds, cardinalities))),
      expectedPlan = CartesianProduct(
        planA,
        planB
      )
    )
  }

  test("should plan cartesian product between 3 pattern nodes") {
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b", "c")),
      input = (solveds: Solveds, cardinalities: Cardinalities) =>  Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), allNodesScan("a", solveds, cardinalities)),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), allNodesScan("b", solveds, cardinalities)),
        PlannedComponent(QueryGraph(patternNodes = Set("c")), allNodesScan("c", solveds, cardinalities))),
      expectedPlan = CartesianProduct(
        planB,
        CartesianProduct(
          planA,
          planC
        )
      ))
  }

  test("should plan cartesian product between lots of pattern nodes") {
    val chars = 'a' to 'z'
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b", "c")),
      input = (solveds, cardinalities) => (chars map { x =>
        PlannedComponent(QueryGraph(patternNodes = Set(x.toString)), allNodesScan(x.toString, solveds, cardinalities))
      }).toSet,
      assertion = (x: LogicalPlan) => {
        val leaves = x.leaves
        leaves.toSet should equal((chars map { x =>
          PlannedComponent(QueryGraph(patternNodes = Set(x.toString)), allNodesScan(x.toString))
        }).map(_.plan).toSet)
        leaves.size should equal(chars.size)
      }
    )
  }

  test("should plan hash join between 2 pattern nodes") {
    val equality = Equals(prop("a", "id"), prop("b", "id"))(pos)

    testThis(
      graph = QueryGraph(
        patternNodes = Set("a", "b"),
        selections = Selections.from(equality)),
      input = (solveds: Solveds, cardinalities: Cardinalities) =>  Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), allNodesScan("a", solveds, cardinalities)),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), allNodesScan("b", solveds, cardinalities))),
      expectedPlan = ValueHashJoin(planA, planB, equality))
  }

  test("should plan hash joins between 3 pattern nodes") {
    val eq1 = Equals(prop("b", "id"), prop("a", "id"))(pos)
    val eq2 = Equals(prop("b", "id"), prop("c", "id"))(pos)
    val eq3 = Equals(prop("a", "id"), prop("c", "id"))(pos)

    testThis(
      graph = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(Seq(eq1, eq2, eq3))),
      input = (solveds: Solveds, cardinalities: Cardinalities) =>  Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), allNodesScan("a", solveds, cardinalities)),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), allNodesScan("b", solveds, cardinalities)),
        PlannedComponent(QueryGraph(patternNodes = Set("c")), allNodesScan("c", solveds, cardinalities))),
      expectedPlan =
        Selection(Seq(eq3),
          ValueHashJoin(planA,
            ValueHashJoin(planB, planC, eq2), eq1.switchSides)))
  }

  private def testThis(graph: QueryGraph, input: (Solveds, Cardinalities) => Set[PlannedComponent], assertion: LogicalPlan => Unit): Unit = {
    new given {
      qg = graph
      cardinality = mapCardinality {
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set("a") => 1000.0
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set("c") => 3000.0
        case _ => 100.0
      }
    }.withLogicalPlanningContext { (cfg, ctx, solveds, cardinalities) =>
      val kit = ctx.config.toKit(ctx, solveds, cardinalities)

      var plans: Set[PlannedComponent] = input(solveds, cardinalities)
      while (plans.size > 1) {
        plans = cartesianProductsOrValueJoins(plans, cfg.qg, ctx, solveds, cardinalities, kit, SingleComponentPlanner(mock[IDPQueryGraphSolverMonitor]))
      }

      val result = plans.head.plan

      assertion(result)
    }
  }

  private def testThis(graph: QueryGraph, input: (Solveds, Cardinalities) => Set[PlannedComponent], expectedPlan: LogicalPlan): Unit =
    testThis(graph, input, (result: LogicalPlan) => result should equal(expectedPlan))
}
