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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_4.planner.logical._
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.SeqCombiner.combine
import org.neo4j.cypher.internal.ir.v3_4.{QueryGraph, Selections}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_4.expressions.PartialPredicate.PartialPredicateWrapper
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, Ors}

case class OrLeafPlanner(inner: Seq[LeafPlanFromExpressions]) extends LeafPlanner {

  override def apply(qg: QueryGraph, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): Seq[LogicalPlan] = {
    qg.selections.flatPredicates.flatMap {
      case orPredicate@Ors(exprs) =>

        // This is a Seq of possible solutions per expression
        val plansPerExpression: Seq[Seq[LeafPlansForVariable]] = exprs.toSeq.map {
          (e: Expression) =>
            val plansForVariables: Seq[LeafPlansForVariable] = inner.flatMap(_.producePlanFor(Set(e), qg, context))
            val qgForExpression = qg.copy(selections = Selections.from(e))
            val canDoIndexSeek = plansForVariables.exists(leafPlans => leafPlans.plans.exists(nodeIndexSeek))
            val withoutIndexScans = if (canDoIndexSeek) plansForVariables.filter(x => !x.plans.exists(nodeIndexScan)) else plansForVariables
            withoutIndexScans.map(p =>
              p.copy(plans = p.plans.map(context.config.applySelections(_, qgForExpression, context, solveds, cardinalities))))
        }

        val wasUnableToFindPlanForAtLeastOnePredicate = plansPerExpression.exists(_.isEmpty)

        if (wasUnableToFindPlanForAtLeastOnePredicate || hasPlanSolvingOtherVariable(plansPerExpression)) {
          Seq.empty
        } else {
          val combination: Seq[Seq[LeafPlansForVariable]] = combine(plansPerExpression)
          val step2: Seq[Seq[LogicalPlan]] = combination.map(_.flatMap(_.plans))

          val producer = context.logicalPlanProducer
          step2.flatMap {
            case plans if plans.isEmpty =>
              None
            case plans =>
              // We need to collect the predicates to be able to update solved correctly. After finishing to build the
              // OR plan, we will report solving the OR predicate, but also other predicates which are covered by ALL
              // underlying plans are solved.
              val predicates = collection.mutable.HashSet[Expression]()
              predicates ++= coveringPredicates(plans.head, solveds)

              val singlePlan = plans.reduce[LogicalPlan] {
                case (p1, p2) =>
                  predicates --= (predicates diff coveringPredicates(p2, solveds).toSet)
                  producer.planUnion(p1, p2, context)
              }
              val orPlan = context.logicalPlanProducer.planDistinctStar(singlePlan, context)

              Some(context.logicalPlanProducer.updateSolvedForOr(orPlan, orPredicate, predicates.toSet, context))
          }
        }

      case _ => Seq.empty
    }
  }

  private def hasPlanSolvingOtherVariable(plansPerExpression: Seq[Seq[LeafPlansForVariable]]) = {
    val id: String = plansPerExpression.head.head.id

    plansPerExpression.exists(leafs => leafs.exists(_.id != id))
  }

  private def coveringPredicates(plan: LogicalPlan, solveds: Solveds): Seq[Expression] = {
    solveds.get(plan.id).tailOrSelf.queryGraph.selections.flatPredicates.map {
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
}
