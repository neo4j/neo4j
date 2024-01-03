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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CartesianProductComponentConnectorTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private def register[X](registry: IdRegistry[X], elements: X*): Goal = Goal(registry.registerAll(elements))

  test("produces all cartesian product combinations of two components") {
    val table = IDPTable.empty[LogicalPlan]
    val registry: DefaultIdRegistry[QueryGraph] = IdRegistry[QueryGraph]

    new givenConfig().withLogicalPlanningContext { (cfg, ctx) =>
      val order = InterestingOrderConfig.empty
      val kit = ctx.plannerState.config.toKit(order, ctx)
      val nQg = QueryGraph(patternNodes = Set(v"n"))
      val mQg = QueryGraph(patternNodes = Set(v"m"))
      val fullQg = nQg ++ mQg

      val nPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "n")
      val mPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "m")
      table.put(register(registry, nQg), sorted = false, nPlan)
      table.put(register(registry, mQg), sorted = false, mPlan)
      val goal = register(registry, nQg, mQg)

      val step =
        CartesianProductComponentConnector.solverStep(GoalBitAllocation(2, 0, Seq.empty), fullQg, order, kit, ctx)
      val plans = step(registry, goal, table, ctx).toSeq
      plans should contain theSameElementsAs Seq(CartesianProduct(nPlan, mPlan), CartesianProduct(mPlan, nPlan))
    }
  }

  test("produces only cartesian product combinations with sort on LHS") {
    val table = IDPTable.empty[LogicalPlan]
    val registry: DefaultIdRegistry[QueryGraph] = IdRegistry[QueryGraph]

    new givenConfig().withLogicalPlanningContext { (cfg, ctx) =>
      val order = InterestingOrderConfig.empty
      val kit = ctx.plannerState.config.toKit(order, ctx)
      val nQg = QueryGraph(patternNodes = Set(v"n"))
      val mQg = QueryGraph(patternNodes = Set(v"m"))
      val fullQg = nQg ++ mQg

      // extra-symbol is used to make `nPlan != nPlanSort`
      val nPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "n")
      val nPlanSort = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "n", "extra-symbol")
      val mPlan = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "m")
      val mPlanSort = fakeLogicalPlanFor(ctx.staticComponents.planningAttributes, "m", "extra-symbol")

      table.put(register(registry, nQg), sorted = false, nPlan)
      table.put(register(registry, nQg), sorted = true, nPlanSort)
      table.put(register(registry, mQg), sorted = false, mPlan)
      table.put(register(registry, mQg), sorted = true, mPlanSort)
      val goal = register(registry, nQg, mQg)

      val step =
        CartesianProductComponentConnector.solverStep(GoalBitAllocation(2, 0, Seq.empty), fullQg, order, kit, ctx)
      val plans = step(registry, goal, table, ctx).toSeq
      plans should contain theSameElementsAs Seq(
        CartesianProduct(nPlan, mPlan),
        CartesianProduct(nPlanSort, mPlan),
        CartesianProduct(mPlan, nPlan),
        CartesianProduct(mPlanSort, nPlan)
      )
    }
  }

}
