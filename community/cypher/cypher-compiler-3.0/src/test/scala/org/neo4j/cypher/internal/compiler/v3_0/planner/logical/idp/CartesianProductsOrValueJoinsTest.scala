/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_0.planner.{LogicalPlanningTestSupport2, QueryGraph, Selections}
import org.neo4j.cypher.internal.frontend.v3_0.ast.{AstConstructionTestSupport, Equals}
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class CartesianProductsOrValueJoinsTest
  extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {
  val planA = AllNodesScan("a", Set.empty)(solved)
  val planB = AllNodesScan("b", Set.empty)(solved)
  val planC = AllNodesScan("c", Set.empty)(solved)

  test("should plan cartesian product between 2 pattern nodes") {
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b")),
      input = Set(planA, planB),
      expectedPlan = CartesianProduct(
        planA,
        planB
      )(solved)
    )
  }

  test("should plan cartesian product between 3 pattern nodes") {
    testThis(
      graph = QueryGraph(patternNodes = Set("a", "b", "c")),
      input = Set(planA, planB, planC),
      expectedPlan = CartesianProduct(
        planA,
        CartesianProduct(
          planB,
          planC
        )(solved)
      )(solved))
  }

  test("should plan hash join between 2 pattern nodes") {
    val equality = Equals(prop("a", "id"), prop("b", "id"))(pos)

    testThis(
      graph = QueryGraph(
        patternNodes = Set("a", "b"),
        selections = Selections.from(equality)),
      input = Set(planA, planB),
      expectedPlan = ValueHashJoin(planA, planB, equality)(solved))
  }

  test("should plan hash joins between 3 pattern nodes") {
    val eq1 = Equals(prop("b", "id"), prop("a", "id"))(pos)
    val eq2 = Equals(prop("b", "id"), prop("c", "id"))(pos)
    val eq3 = Equals(prop("a", "id"), prop("c", "id"))(pos)

    val AxC = ValueHashJoin(planA, planC, eq3)(solved)
    val Bx_AxC = ValueHashJoin(planB, AxC, eq1)(solved)
    val expectedPlan = Selection(Seq(eq2), Bx_AxC)(solved)

    testThis(
      graph = QueryGraph(
        patternNodes = Set("a", "b", "c"),
        selections = Selections.from(eq1, eq2, eq3)),
      input = Set(planA, planB, planC),
      expectedPlan = expectedPlan)
  }

  private def testThis(graph: QueryGraph, input: Set[LogicalPlan], expectedPlan: LogicalPlan) = {
    new given {
      qg = graph
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val x = ctx
      implicit val kit = ctx.config.toKit()

      var plans = input
      while (plans.size > 1) {
        plans = cartesianProductsOrValueJoins(plans, cfg.qg)
      }

      plans should equal(Set(expectedPlan))
    }
  }
}
