/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CartesianProductComponentConnectorTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  private def register[X](registry: IdRegistry[X], elements: X*): Goal = Goal(registry.registerAll(elements))

  test("produces all cartesian product combinations of two components") {
    val table = IDPTable.empty[LogicalPlan]
    val registry: DefaultIdRegistry[QueryGraph] = IdRegistry[QueryGraph]

    new given().withLogicalPlanningContext { (cfg, ctx) =>
      val order = InterestingOrder.empty
      val kit = ctx.config.toKit(order, ctx)
      val nQg = QueryGraph(patternNodes = Set("n"))
      val mQg = QueryGraph(patternNodes = Set("m"))
      val fullQg = nQg ++ mQg

      val nPlan = fakeLogicalPlanFor(ctx.planningAttributes, "n")
      val mPlan = fakeLogicalPlanFor(ctx.planningAttributes, "m")
      table.put(register(registry, nQg), sorted = false, nPlan)
      table.put(register(registry, mQg), sorted = false, mPlan)
      val goal = register(registry, nQg, mQg)

      val step = CartesianProductComponentConnector.solverStep(fullQg, order, kit)
      val plans = step(registry, goal, table, ctx).toSeq
      plans should contain theSameElementsAs (Seq(CartesianProduct(nPlan, mPlan), CartesianProduct(mPlan, nPlan)))

    }
  }

}
