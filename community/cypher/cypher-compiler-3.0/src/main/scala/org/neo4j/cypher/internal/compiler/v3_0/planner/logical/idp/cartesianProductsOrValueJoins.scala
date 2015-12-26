/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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


trait JoinDisconnectedQueryGraphComponents {
  def apply(componentPlans: Set[(QueryGraph, LogicalPlan)], fullQG: QueryGraph)
           (implicit context: LogicalPlanningContext, kit: QueryPlannerKit,
            singleComponentPlanner: SingleComponentPlannerTrait):Set[(QueryGraph, LogicalPlan)]
}


/*
This class is responsible for connecting two disconnected logical plans, which can be
done with hash joins when an useful predicate connects the two plans, or with cartesian
product lacking that.

The input is a set of disconnected patterns and this class will greedily find the
cheapest connection that can be done replace the two input plans with the connected
one. This process can then be repeated until a single plan remains.
 */
case object cartesianProductsOrValueJoins extends JoinDisconnectedQueryGraphComponents {
  type T = (QueryGraph, LogicalPlan)

  def apply(plans: Set[T], qg: QueryGraph)(implicit context: LogicalPlanningContext, kit: QueryPlannerKit,
                                           singleComponentPlanner: SingleComponentPlannerTrait): Set[T] = {

    assert(plans.size > 1, "Can't build cartesian product with less than two input plans")

    val connectedPlans: Map[(QueryGraph, LogicalPlan), ((QueryGraph, LogicalPlan), (QueryGraph, LogicalPlan))] = {
      val joins = produceJoinVariations(plans, qg)

      if (joins.nonEmpty)
        joins
      else
        produceCartesianProducts(plans, qg)
    }

    val bestPlan = kit.pickBest(connectedPlans.map(_._1._2)).get
    val bestQG = connectedPlans.collectFirst {
      case ((fqg,pl) ,_) if bestPlan == pl=> fqg
    }.get
    val (p1, p2) = connectedPlans(bestQG -> bestPlan)

    plans - p1 - p2 + (bestQG -> bestPlan)
  }

  private def produceCartesianProducts(plans: Set[T], qg: QueryGraph)
                                      (implicit context: LogicalPlanningContext, kit: QueryPlannerKit): Map[T, (T, T)] = {
    (for (t1@(qg1, p1) <- plans; t2@(qg2, p2) <- plans if p1 != p2) yield {
      val crossProduct = kit.select(context.logicalPlanProducer.planCartesianProduct(p1, p2), qg)
      ((qg1 ++ qg2) -> crossProduct, (t1, t2))
    }).toMap
  }

  private def produceJoinVariations(plans: Set[T], qg: QueryGraph)
                                   (implicit context: LogicalPlanningContext, kit: QueryPlannerKit,
                                    singleComponentPlanner: SingleComponentPlannerTrait): Map[T, (T, T)] = {
    (for {
      join <- qg.selections.valueJoins
      t1@(qgA, planA) <- plans if planA.satisfiesExpressionDependencies(join.lhs)
      t2@(qgB, planB) <- plans if planB.satisfiesExpressionDependencies(join.rhs) && planA != planB
      plans = planA -> planB
    } yield {
      val AxB = kit.select(context.logicalPlanProducer.planValueHashJoin(planA, planB, join, join), qg)
      val BxA = kit.select(context.logicalPlanProducer.planValueHashJoin(planB, planA, join.switchSides, join), qg)

      Set(
        (AxB.solved.lastQueryGraph -> AxB, t1 -> t2),
        (BxA.solved.lastQueryGraph -> BxA, t1 -> t2)
      )
    }).flatten.toMap
  }
}
