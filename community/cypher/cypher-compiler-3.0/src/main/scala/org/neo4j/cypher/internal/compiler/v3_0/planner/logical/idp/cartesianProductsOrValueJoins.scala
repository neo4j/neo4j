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
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{LogicalPlan, IndexLeafPlan}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningContext, QueryPlannerKit}
import org.neo4j.cypher.internal.frontend.v3_0.ast.Expression


trait JoinDisconnectedQueryGraphComponents {
  def apply(componentPlans: Set[PlannedComponent], fullQG: QueryGraph)
           (implicit context: LogicalPlanningContext, kit: QueryPlannerKit,
            singleComponentPlanner: SingleComponentPlannerTrait): Set[PlannedComponent]
}

case class PlannedComponent(queryGraph: QueryGraph, plan: LogicalPlan)

/*
This class is responsible for connecting two disconnected logical plans, which can be
done with hash joins when an useful predicate connects the two plans, or with cartesian
product lacking that.

The input is a set of disconnected patterns and this class will greedily find the
cheapest connection that can be done replace the two input plans with the connected
one. This process can then be repeated until a single plan remains.
 */
case object cartesianProductsOrValueJoins extends JoinDisconnectedQueryGraphComponents {

  def apply(plans: Set[PlannedComponent], qg: QueryGraph)(implicit context: LogicalPlanningContext, kit: QueryPlannerKit,
                                                          singleComponentPlanner: SingleComponentPlannerTrait): Set[PlannedComponent] = {

    assert(plans.size > 1, "Can't build cartesian product with less than two input plans")

    // Map of newly found plans to originating LHS and RHS plans to join
    val connectedPlans: Map[PlannedComponent, (PlannedComponent, PlannedComponent)] = {
      val joins = produceJoinVariations(plans, qg)

      if (joins.nonEmpty)
        joins
      else
        produceCartesianProducts(plans, qg)
    }

    val bestPlan = kit.pickBest(connectedPlans.map(_._1.plan)).get
    val bestQG: QueryGraph = connectedPlans.collectFirst {
      case (PlannedComponent(fqg, pl), _) if bestPlan == pl => fqg
    }.get
    val (p1, p2) = connectedPlans(PlannedComponent(bestQG, bestPlan))

    plans - p1 - p2 + PlannedComponent(bestQG, bestPlan)
  }

  private def produceCartesianProducts(plans: Set[PlannedComponent], qg: QueryGraph)
                                      (implicit context: LogicalPlanningContext, kit: QueryPlannerKit):
                                      Map[PlannedComponent, (PlannedComponent, PlannedComponent)] = {
    (for (t1@PlannedComponent(qg1, p1) <- plans; t2@PlannedComponent(qg2, p2) <- plans if p1 != p2) yield {
      val crossProduct = kit.select(context.logicalPlanProducer.planCartesianProduct(p1, p2), qg)
      (PlannedComponent(qg1 ++ qg2, crossProduct), (t1, t2))
    }).toMap
  }

  private def produceJoinVariations(plans: Set[PlannedComponent], qg: QueryGraph)
                                   (implicit context: LogicalPlanningContext, kit: QueryPlannerKit,
                                    singleComponentPlanner: SingleComponentPlannerTrait):
                                   Map[PlannedComponent, (PlannedComponent, PlannedComponent)] = {
    (for {
      join <- qg.selections.valueJoins
      t1@PlannedComponent(qgA, planA) <- plans if planA.satisfiesExpressionDependencies(join.lhs)
      t2@PlannedComponent(qgB, planB) <- plans if planB.satisfiesExpressionDependencies(join.rhs) && planA != planB
      plans = planA -> planB
    } yield {
      val hashJoinAB = kit.select(context.logicalPlanProducer.planValueHashJoin(planA, planB, join, join), qg)
      val hashJoinBA = kit.select(context.logicalPlanProducer.planValueHashJoin(planB, planA, join.switchSides, join), qg)
      val nestedIndexJoinAB = planNIJ(planA, planB, qgA, qgB, qg, join)
      val nestedIndexJoinBA = planNIJ(planB, planA, qgB, qgA, qg, join)

      Set(
        (PlannedComponent(hashJoinAB.solved.lastQueryGraph, hashJoinAB), t1 -> t2),
        (PlannedComponent(hashJoinBA.solved.lastQueryGraph, hashJoinBA), t1 -> t2)
      ) ++
        nestedIndexJoinAB.map(x => (x, t1 -> t2)) ++
        nestedIndexJoinBA.map(x => (x, t1 -> t2))

    }).flatten.toMap
  }

  /*
  Index Nested Loop Joins -- if there is a value join connection between the LHS and RHS, and a useful index exists for
  one of the sides, it can be used if the query is planned as an apply with the index seek on the RHS.

      Apply
    LHS  Index Seek
   */
  private def planNIJ(lhsPlan: LogicalPlan, rhsInputPlan: LogicalPlan,
                      lhsQG: QueryGraph, rhsQG: QueryGraph,
                      fullQG: QueryGraph, predicate: Expression)
                     (implicit context: LogicalPlanningContext,
                      kit: QueryPlannerKit,
                      singleComponentPlanner: SingleComponentPlannerTrait) = {

    val builtOnTopOfOtherPlans = rhsQG.argumentIds.nonEmpty
    val notSingleComponent = rhsQG.connectedComponents.size > 1
    val containsOptionals = rhsInputPlan.solved.lastQueryGraph.optionalMatches.nonEmpty

    if (builtOnTopOfOtherPlans || notSingleComponent || containsOptionals) None
    else {
      // Replan the RHS with the LHS arguments available. If good indexes exist, they can now be used
      val ids = rhsInputPlan.solved.lastQueryGraph.addArgumentIds(lhsQG.coveredIds.toSeq).addPredicates(predicate)
      val rhsPlan = singleComponentPlanner.planComponent(ids)
      val result = kit.select(context.logicalPlanProducer.planApply(lhsPlan, rhsPlan), fullQG)

      // If none of the leaf-plans leverages the data from the RHS to use an index, let's not use this plan at all
      // The reason is that when this happens, we are producing a cartesian product disguising as an Apply, and
      // this confuses the cost model
      val lhsDependencies = result.leaves.collect {
        case x: IndexLeafPlan => x.valueExpr.expression.dependencies
      }.flatten

      if (lhsDependencies.nonEmpty)
        Some(PlannedComponent(result.solved.lastQueryGraph, result))
      else
        None
    }
  }
}
