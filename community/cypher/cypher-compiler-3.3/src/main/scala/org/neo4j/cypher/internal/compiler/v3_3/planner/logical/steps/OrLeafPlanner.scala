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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v3_3.ast.PartialPredicate.PartialPredicateWrapper
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, Ors}
import org.neo4j.cypher.internal.frontend.v3_3.helpers.SeqCombiner.combine
import org.neo4j.cypher.internal.ir.v3_3.{IdName, QueryGraph, Selections}

case class OrLeafPlanner(inner: Seq[LeafPlanFromExpressions]) extends LeafPlanner {

  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    qg.selections.flatPredicates.flatMap {
      case orPredicate@Ors(exprs) =>

        // This is a Seq of possible solutions per expression
        val plansPerExpression: Seq[Seq[LeafPlansForVariable]] = exprs.toSeq.map {
          (e: Expression) =>
            val plansForVariables: Seq[LeafPlansForVariable] = inner.flatMap(_.producePlanFor(Set(e), qg))
            val qgForExpression = qg.copy(selections = Selections.from(e))
            plansForVariables.map(p =>
              p.copy(plans = p.plans.map(context.config.applySelections(_, qgForExpression))))
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
              // OR plan, we will report solving the OR predicate, but also other predicates solved.
              val predicates = collection.mutable.HashSet[Expression]()
              val singlePlan = plans.reduce[LogicalPlan] {
                case (p1, p2) =>
                  predicates ++= coveringPredicates(p1)
                  predicates ++= coveringPredicates(p2)
                  producer.planUnion(p1, p2)
              }
              val orPlan = context.logicalPlanProducer.planDistinct(singlePlan)

              Some(context.logicalPlanProducer.updateSolvedForOr(orPlan, orPredicate, predicates.toSet))
          }
        }

      case _ => Seq.empty
    }
  }

  private def hasPlanSolvingOtherVariable(plansPerExpression: Seq[Seq[LeafPlansForVariable]]) = {
    val id: IdName = plansPerExpression.head.head.id

    plansPerExpression.exists(leafs => leafs.exists(_.id != id))
  }

  def coveringPredicates(plan: LogicalPlan): Seq[Expression] = {
    plan.solved.tailOrSelf.queryGraph.selections.flatPredicates.map {
      case PartialPredicateWrapper(coveredPredicate, coveringPredicate) => coveringPredicate
      case predicate => predicate
    }
  }
}
