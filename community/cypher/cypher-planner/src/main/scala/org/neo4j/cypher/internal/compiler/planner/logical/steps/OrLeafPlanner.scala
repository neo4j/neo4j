/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.helpers.SeqCombiner.combine
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ordering
import org.neo4j.cypher.internal.logical
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds

case class OrLeafPlanner(inner: Seq[LeafPlanFromExpressions]) extends LeafPlanner {

  override def apply(qg: QueryGraph, interestingOrderConfig: InterestingOrderConfig, context: LogicalPlanningContext): Seq[LogicalPlan] = {
    qg.selections.flatPredicates.flatMap {
      case orPredicate@Ors(exprs) =>

        val plansPerExpression: Array[Array[LeafPlansForVariable]] = producePlansForExpressions(exprs, qg, context, interestingOrderConfig)
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
              predicates ++= solvedPredicates(plans.head, context.planningAttributes.solveds)

              // Determines if we can plan OrderedUnion
              val maybeSortColumn = Option(context.planningAttributes.providedOrders(plans.head.id))
                // We only support a sorted union if the plans are sorted by a single column.
                .filter(_.columns.size == 1)
                // All plans must be ordered by the same thing.
                .filter(head => plans.tail.map(p => context.planningAttributes.providedOrders(p.id)).forall(_ == head))
                .flatMap(_.columns.headOption)
                // The only sort column must be by a variable. Convert to a logical plan ColumnOrder.
                .collect {
                  case ordering.ColumnOrder.Asc(v@Variable(varName), _) => (v, logical.plans.Ascending(varName))
                  case ordering.ColumnOrder.Desc(v@Variable(varName), _) => (v, logical.plans.Descending(varName))
                }

              val singlePlan = plans.reduce[LogicalPlan] {
                case (p1, p2) =>
                  predicates --= (predicates diff solvedPredicates(p2, context.planningAttributes.solveds).toSet)
                  maybeSortColumn match {
                    case Some((_, sortColumn)) => producer.planOrderedUnion(p1, p2, List(), Seq(sortColumn), context)
                    case None => producer.planUnion(p1, p2, List(), context)
                  }
              }

              val orPlan = maybeSortColumn match {
                case Some((sortVariable, _)) => context.logicalPlanProducer.planOrderedDistinctForUnion(singlePlan, Seq(sortVariable), context)
                case None => context.logicalPlanProducer.planDistinctForUnion(singlePlan, context)
              }

              Some(context.logicalPlanProducer.updateSolvedForOr(orPlan, orPredicate, predicates.toSet, context))
          }
        }

      case _ => Seq.empty
    }
  }

  private def hasPlanSolvingOtherVariable(plansPerExpression: Array[Array[LeafPlansForVariable]]) = {
    val id: String = plansPerExpression.head.head.id

    plansPerExpression.exists(leafs => leafs.exists(_.id != id))
  }

  private def solvedPredicates(plan: LogicalPlan, solveds: Solveds): Seq[Expression] = {
    solveds.get(plan.id).asSinglePlannerQuery.tailOrSelf.queryGraph.selections.flatPredicates
  }

  private[steps] def producePlansForExpressions(exprs: Seq[Expression],
                                                qg: QueryGraph,
                                                context: LogicalPlanningContext,
                                                interestingOrderConfig: InterestingOrderConfig): Array[Array[LeafPlansForVariable]] = {

    // This is a Seq of possible solutions per expression
    // We really only want the best option IndexSeek > IndexScan > LabelScan as combine() explodes to p^n
    // (number of plans ^ number of predicates) so we really want p to be 1
    exprs.map {
      e: Expression =>
        val plansForVariables: Seq[LeafPlansForVariable] = inner.flatMap(_.producePlanFor(Set(e), qg, interestingOrderConfig, context))
        val qgForExpression = qg.copy(selections = Selections.from(e))
        val filteredPlansForVariables = bestPlansByHeuristic(plansForVariables)
        filteredPlansForVariables.map(p =>
          p.copy(plans = p.plans.map(context.config.applySelections(_, qgForExpression, interestingOrderConfig, context)))).toArray
    }.toArray
  }

  private def bestPlansByHeuristic(plans: Seq[LeafPlansForVariable]): Seq[LeafPlansForVariable] = {
    plans.groupBy(_.id).toSeq.flatMap { case (id, plansForVar) =>
      val allPlans = plansForVar.flatMap(_.plans)
      val bestPlans = allPlans.groupBy(planOrderingHeuristic).minBy(_._1)._2
      bestPlans.map(p => LeafPlansForVariable(id, Set(p)))
    }
  }

  private def planOrderingHeuristic(logicalPlan: LogicalPlan): Int = logicalPlan match {
    case _: NodeIndexSeek |
         _: NodeUniqueIndexSeek |
         _: NodeIndexEndsWithScan |
         _: NodeIndexContainsScan |
         _: DirectedRelationshipIndexEndsWithScan |
         _: UndirectedRelationshipIndexEndsWithScan |
         _: DirectedRelationshipIndexContainsScan |
         _: UndirectedRelationshipIndexContainsScan |
         _: DirectedRelationshipIndexSeek |
         _: UndirectedRelationshipIndexSeek |
         _: DirectedRelationshipIndexEndsWithScan |
         _: UndirectedRelationshipIndexEndsWithScan |
         _: DirectedRelationshipIndexContainsScan |
         _: UndirectedRelationshipIndexContainsScan   => 0
    case _: NodeIndexScan |
         _: UndirectedRelationshipIndexScan |
         _: DirectedRelationshipIndexScan             => 1
    case _: NodeByLabelScan |
         _: DirectedRelationshipTypeScan |
         _: UndirectedRelationshipTypeScan            => 2
    case _                                            => 3
  }

}
