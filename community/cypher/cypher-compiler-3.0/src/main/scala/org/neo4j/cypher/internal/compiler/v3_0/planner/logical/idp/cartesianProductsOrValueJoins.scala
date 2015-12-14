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

import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningContext, QueryPlannerKit}

/*
This class is responsible for connecting two disconnected logical plans, which can be
done with hash joins when an useful predicate connects the two plans, or with cartesian
product lacking that.

The input is a set of disconnected patterns and this class will greedily find the
cheapest connection that can be done replace the two input plans with the connected
one. This process can then be repeated until a single plan remains.
 */
case object cartesianProductsOrValueJoins {
  def apply(plans: Set[LogicalPlan], qg: QueryGraph)(implicit context: LogicalPlanningContext, kit: QueryPlannerKit): Set[LogicalPlan] = {

    assert(plans.size > 1, "Can't build cartesian product with less than two input plans")

    val connectedPlans = {
      val hashJoins = produceHashJoins(plans, qg)

      if (hashJoins.nonEmpty)
        hashJoins
      else
        produceCartesianProducts(plans, qg)
    }

    val bestPlan = kit.pickBest(connectedPlans.keySet).get
    val (p1, p2) = connectedPlans(bestPlan)

    plans - p1 - p2 + bestPlan
  }

  private def produceCartesianProducts(plans: Set[LogicalPlan], qg: QueryGraph)
                                      (implicit context: LogicalPlanningContext, kit: QueryPlannerKit)
  : Map[LogicalPlan, (LogicalPlan, LogicalPlan)] = {
    (for (p1 <- plans; p2 <- plans if p1 != p2) yield {
      val crossProduct = kit.select(context.logicalPlanProducer.planCartesianProduct(p1, p2), qg)
      (crossProduct, (p1, p2))
    }).toMap
  }

  private def produceHashJoins(plans: Set[LogicalPlan], qg: QueryGraph)
                              (implicit context: LogicalPlanningContext, kit: QueryPlannerKit)
  : Map[LogicalPlan, (LogicalPlan, LogicalPlan)] = {
    (for {
      join <- qg.selections.valueJoins
      planA <- plans if planA.satisfiesExpressionDependencies(join.lhs)
      planB <- plans if planB.satisfiesExpressionDependencies(join.rhs) && planA != planB
      plans = planA -> planB
    } yield {
      val lpp = context.logicalPlanProducer
      // We produce both sides of the join and allow the cost model to pick the best direction to join on
      val AxB = kit.select(lpp.planValueHashJoin(planA, planB, join, join), qg) -> plans
      val BxA = kit.select(lpp.planValueHashJoin(planB, planA, join.switchSides, join), qg) -> plans
      Set(AxB, BxA)
    }).flatten.toMap
  }
}
