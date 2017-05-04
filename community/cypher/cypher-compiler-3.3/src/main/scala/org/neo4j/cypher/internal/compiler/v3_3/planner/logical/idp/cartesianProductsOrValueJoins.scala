/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{IndexLeafPlan, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{LogicalPlanningContext, QueryPlannerKit}
import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression
import org.neo4j.cypher.internal.ir.v3_2.QueryGraph

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

    /*
    To connect disconnected query parts, we have a couple of different ways. First we check if there are any joins that
    we could do. Joins are equal or better than cartesian products, so we always go for the joins when possible.

    Next we perform an exhaustive search for how to combine the remaining query parts together. In-between each step we
    check if any joins have been made available and if any predicates can be applied. This exhaustive search makes for
    better plans, but is exponentially expensive.

    So, when we have too many plans to combine, we fall back to the naive way of just building a left deep tree with
    all query parts cross joined together.
     */
    val joins = produceJoinVariations(plans, qg)

    if (joins.nonEmpty) {
      pickTheBest(plans, kit, joins)
    } else if (plans.size < 8) {
      val cartesianProducts = produceCartesianProducts(plans, qg)
      pickTheBest(plans, kit, cartesianProducts)
    }
    else {
      planLotsOfCartesianProducts(plans, qg)
    }
  }

  private def pickTheBest(plans: Set[PlannedComponent], kit: QueryPlannerKit, joins: Map[PlannedComponent, (PlannedComponent, PlannedComponent)]): Set[PlannedComponent] = {
    val bestPlan = kit.pickBest(joins.map(_._1.plan)).get
    val bestQG: QueryGraph = joins.collectFirst {
      case (PlannedComponent(fqg, pl), _) if bestPlan == pl => fqg
    }.get
    val (p1, p2) = joins(PlannedComponent(bestQG, bestPlan))

    plans - p1 - p2 + PlannedComponent(bestQG, bestPlan)
  }

  /**
    * Plans a large amount of query parts together. Produces a left deep tree sorted by the cost of the query parts.
    */
  private def planLotsOfCartesianProducts(plans: Set[PlannedComponent], qg: QueryGraph)
                                         (implicit context: LogicalPlanningContext, kit: QueryPlannerKit): Set[PlannedComponent] = {
    val allPlans = plans.toList.sortBy(c => context.cost.apply(c.plan, context.input))
    val onePlanToRuleThemAll = allPlans.tail.foldLeft(allPlans.head) {
      case (l, r) =>
        val crossProduct = kit.select(context.logicalPlanProducer.planCartesianProduct(l.plan, r.plan), qg)
        PlannedComponent(l.queryGraph ++ r.queryGraph, crossProduct)
    }
    Set(onePlanToRuleThemAll)
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

    val notSingleComponent = rhsQG.connectedComponents.size > 1
    val containsOptionals = rhsInputPlan.solved.lastQueryGraph.optionalMatches.nonEmpty

    if (notSingleComponent || containsOptionals) None
    else {
      // Replan the RHS with the LHS arguments available. If good indexes exist, they can now be used
      val ids = rhsInputPlan.solved.lastQueryGraph.addArgumentIds(lhsQG.coveredIds.toIndexedSeq).addPredicates(predicate)
      val rhsPlan = singleComponentPlanner.planComponent(ids)
      val result = kit.select(context.logicalPlanProducer.planApply(lhsPlan, rhsPlan), fullQG)

      // If none of the leaf-plans leverages the data from the RHS to use an index, let's not use this plan at all
      // The reason is that when this happens, we are producing a cartesian product disguising as an Apply, and
      // this confuses the cost model
      val lhsDependencies = result.leaves.collect {
        case x: IndexLeafPlan => x.valueExpr.expressions.flatMap(_.dependencies)
      }.flatten

      if (lhsDependencies.nonEmpty)
        Some(PlannedComponent(result.solved.lastQueryGraph, result))
      else
        None
    }
  }
}
