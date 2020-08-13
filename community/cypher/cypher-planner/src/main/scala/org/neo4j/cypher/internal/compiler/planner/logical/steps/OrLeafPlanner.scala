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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanFromExpressions
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlansForVariable
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.PartialPredicate.PartialPredicateWrapper
import org.neo4j.cypher.internal.frontend.helpers.SeqCombiner.combine
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds

case class OrLeafPlanner(inner: Seq[LeafPlanFromExpressions]) extends LeafPlanner {

  override def apply(qg: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Seq[LogicalPlan] = {
    qg.selections.flatPredicates.flatMap {
      case orPredicate@Ors(exprs) =>

        val plansPerExpression: Array[Array[LeafPlansForVariable]] = producePlansForExpressions(exprs, qg, context, interestingOrder)
        val wasUnableToFindPlanForAtLeastOnePredicate = plansPerExpression.exists(_.isEmpty)

        if (wasUnableToFindPlanForAtLeastOnePredicate || hasPlanSolvingOtherVariable(plansPerExpression)) {
          Seq.empty
        } else {
          val combination: Array[Array[LeafPlansForVariable]] = combine(plansPerExpression)
          val step2: Array[Array[LogicalPlan]] = combination.map(_.flatMap(_.plans))

          val producer = context.logicalPlanProducer
          step2.flatMap {
            case plans if plans.isEmpty =>
              None
            case plans =>
              // We need to collect the predicates to be able to update solved correctly. After finishing to build the
              // OR plan, we will report solving the OR predicate, but also other predicates which are covered by ALL
              // underlying plans are solved.
              val predicates = collection.mutable.HashSet[Expression]()
              predicates ++= coveringPredicates(plans.head, context.planningAttributes.solveds)

              val singlePlan = plans.reduce[LogicalPlan] {
                case (p1, p2) =>
                  predicates --= (predicates diff coveringPredicates(p2, context.planningAttributes.solveds).toSet)
                  producer.planUnionForOrLeaves(p1, p2, context)
              }
              val orPlan = context.logicalPlanProducer.planDistinctForOrLeaves(singlePlan, context)

              Some(context.logicalPlanProducer.updateSolvedForOr(orPlan, orPredicate, predicates.toSet, context))
          }
        }

      case _ => Seq.empty
    }
  }

  private[steps] def producePlansForExpressions(exprs: Set[Expression],
                                         qg: QueryGraph,
                                         context: LogicalPlanningContext,
                                         interestingOrder: InterestingOrder): Array[Array[LeafPlansForVariable]] = {

    def filterPlans(plans: Seq[LeafPlansForVariable], findFunc: LogicalPlan => Boolean, filterFunc: LogicalPlan => Boolean): Seq[LeafPlansForVariable] =
      if (plans.exists(leafPlans => leafPlans.plans.exists(findFunc))) plans.filter(x => !x.plans.exists(filterFunc)) else plans

    // This is a Seq of possible solutions per expression
    // We really only want the best option IndexSeek > IndexScan > LabelScan as combine() explodes to p^n
    // (number of plans ^ number of predicates) so we really want p to be 1
    exprs.map {
      e: Expression =>
        val plansForVariables: Seq[LeafPlansForVariable] = inner.flatMap(_.producePlanFor(Set(e), qg, interestingOrder, context))
        val qgForExpression = qg.copy(selections = Selections.from(e))
        val withoutLabelScans = filterPlans(plansForVariables, p => nodeIndexSeek(p) || nodeIndexScan(p), nodeByLabelScan)
        val withoutIndexScans = filterPlans(withoutLabelScans, nodeIndexSeek, nodeIndexScan)
        withoutIndexScans.map(p =>
          p.copy(plans = p.plans.map(context.config.applySelections(_, qgForExpression, interestingOrder, context)))).toArray
    }.toArray
  }

  private def hasPlanSolvingOtherVariable(plansPerExpression: Array[Array[LeafPlansForVariable]]) = {
    val id: String = plansPerExpression.head.head.id

    plansPerExpression.exists(leafs => leafs.exists(_.id != id))
  }

  private def coveringPredicates(plan: LogicalPlan, solveds: Solveds): Seq[Expression] = {
    solveds.get(plan.id).asSinglePlannerQuery.tailOrSelf.queryGraph.selections.flatPredicates.map {
      case PartialPredicateWrapper(coveredPredicate, coveringPredicate) => coveringPredicate
      case predicate => predicate
    }
  }

  private def nodeIndexSeek(logicalPlan: LogicalPlan): Boolean =
    logicalPlan match {
      case _: NodeIndexSeek |
           _: NodeUniqueIndexSeek |
           _: NodeIndexEndsWithScan |
           _: NodeIndexContainsScan => true
      case _ => false
    }

  private def nodeIndexScan(logicalPlan: LogicalPlan): Boolean =
    logicalPlan.isInstanceOf[NodeIndexScan]

  private def nodeByLabelScan(logicalPlan: LogicalPlan): Boolean =
    logicalPlan.isInstanceOf[NodeByLabelScan]
}
