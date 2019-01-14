/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
      expectedPlans =
        List(planA, planB).permutations.map { l =>
          val (a, b) = (l.head, l(1))
          CartesianProduct(
            planA,
            planB
          )
        }.toSeq: _*)
  }

  test("should plan cartesian product between 3 pattern nodes") {
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b", "c")),
      input = (solveds: Solveds, cardinalities: Cardinalities) =>  Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), allNodesScan("a", solveds, cardinalities)),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), allNodesScan("b", solveds, cardinalities)),
        PlannedComponent(QueryGraph(patternNodes = Set("c")), allNodesScan("c", solveds, cardinalities))),
      expectedPlans =
        List(planA, planB, planC).permutations.map { l =>
          val (a, b, c) = (l.head, l(1), l(2))
          CartesianProduct(
            b,
            CartesianProduct(
              a,
              c
            )
          )
        }.toSeq : _*)
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
    testThis(
      graph = QueryGraph(
        patternNodes = Set("a", "b"),
        selections = Selections.from(Equals(prop("a", "id"), prop("b", "id"))(pos))),
      input = (solveds: Solveds, cardinalities: Cardinalities) =>  Set(
        PlannedComponent(QueryGraph(patternNodes = Set("a")), allNodesScan("a", solveds, cardinalities)),
        PlannedComponent(QueryGraph(patternNodes = Set("b")), allNodesScan("b", solveds, cardinalities))),
      expectedPlans =
        List((planA, "a"), (planB, "b")).permutations.map { l =>
          val ((a, aName), (b, bName)) = (l.head, l(1))
            ValueHashJoin(a, b, Equals(prop(aName, "id"), prop(bName, "id"))(pos))
        }.toSeq : _*)
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
      expectedPlans =
        List((planA, "a"), (planB, "b"), (planC, "c")).permutations.flatMap { l =>
          val ((a, aName), (b, bName), (c, cName)) = (l.head, l(1), l(2))
          // permutate equals order
          List(prop(bName, "id"), prop(cName, "id")).permutations.map { l2 =>
            val (prop1, prop2) = (l2.head, l2(1))
            Selection(Seq(Equals(prop(aName, "id"), prop2)(pos)),
              ValueHashJoin(a,
                ValueHashJoin(b, c, Equals(prop(bName, "id"), prop(cName, "id"))(pos)), Equals(prop(aName, "id"), prop1)(pos)))
          }
        }.toSeq : _*)
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

  private def testThis(graph: QueryGraph, input: (Solveds, Cardinalities) => Set[PlannedComponent], expectedPlans: LogicalPlan*): Unit =
    testThis(graph, input, (result: LogicalPlan) => expectedPlans should contain(result))
}
