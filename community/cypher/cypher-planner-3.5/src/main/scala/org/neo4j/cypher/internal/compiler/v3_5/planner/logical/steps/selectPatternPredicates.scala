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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical._
import org.neo4j.cypher.internal.ir.v3_5.{InterestingOrder, QueryGraph}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.expressions.functions.Exists
import org.neo4j.cypher.internal.v3_5.util.FreshIdNameGenerator

case object selectPatternPredicates extends CandidateGenerator[LogicalPlan] {

  def apply(lhs: LogicalPlan, queryGraph: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Seq[LogicalPlan] = {
    for (
      pattern <- queryGraph.selections.patternPredicatesGiven(lhs.availableSymbols)
      if applicable(lhs, queryGraph, pattern, context.planningAttributes.solveds))
      yield {
        pattern match {
          case p@Exists(patternExpression: PatternExpression) =>
            val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
            context.logicalPlanProducer.planSemiApply(lhs, rhs, p, context)
          case p@Not(Exists(patternExpression: PatternExpression)) =>
            val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
            context.logicalPlanProducer.planAntiSemiApply(lhs, rhs, p, context)
          case p@Ors(exprs) =>
            val (patternExpressions, expressions) = exprs.partition {
              case Exists(_: PatternExpression) => true
              case Not(Exists(_: PatternExpression)) => true
              case _ => false
            }
            val (plan, solvedPredicates) = planPredicates(lhs, patternExpressions, expressions, None, interestingOrder, context)
            context.logicalPlanProducer.solvePredicate(plan, onePredicate(solvedPredicates), context)
        }
      }
  }

  private def planPredicates(lhs: LogicalPlan,
                             patternExpressions: Set[Expression],
                             expressions: Set[Expression],
                             letExpression: Option[Expression],
                             interestingOrder: InterestingOrder,
                             context: LogicalPlanningContext): (LogicalPlan, Set[Expression]) = {
    patternExpressions.toList match {
      case (p@Exists(patternExpression: PatternExpression)) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        val plan = context.logicalPlanProducer.planSelectOrSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet), context)
        (plan, expressions + p)

      case (p@Not(Exists(patternExpression: PatternExpression))) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        val plan = context.logicalPlanProducer.planSelectOrAntiSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet), context)
        (plan, expressions + p)

      case (p@Exists(patternExpression: PatternExpression)) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        val (newLhs, newLetExpr) = createLetSemiApply(lhs, rhs, patternExpression, expressions, letExpression, context)
        val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr), interestingOrder, context)
        (plan, solvedPredicates ++ Set(p) ++ expressions)

      case (p@Not(Exists(patternExpression: PatternExpression))) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        val (newLhs, newLetExpr) = createLetAntiSemiApply(lhs, rhs, patternExpression, p, expressions, letExpression, context)
        val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr), interestingOrder, context)
        (plan, solvedPredicates ++ Set(p) ++ expressions)

      case _ =>
        throw new IllegalArgumentException("There should be at least one pattern expression")
    }
  }

  private def createLetSemiApply(lhs: LogicalPlan,
                                 rhs: LogicalPlan,
                                 patternExpression: PatternExpression,
                                 expressions: Set[Expression],
                                 letExpression: Option[Expression],
                                 context: LogicalPlanningContext) = {
    val (idName, ident) = freshId(patternExpression)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.logicalPlanProducer.planLetSemiApply(lhs, rhs, idName, context), ident)
    else
      (context.logicalPlanProducer.planLetSelectOrSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet), context), ident)
  }

  private def createLetAntiSemiApply(lhs: LogicalPlan,
                                     rhs: LogicalPlan,
                                     patternExpression: PatternExpression,
                                     predicate: Expression,
                                     expressions: Set[Expression],
                                     letExpression: Option[Expression],
                                     context: LogicalPlanningContext) = {
    val (idName, ident) = freshId(patternExpression)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.logicalPlanProducer.planLetAntiSemiApply(lhs, rhs, idName, context), ident)
    else
      (context.logicalPlanProducer.planLetSelectOrAntiSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet), context), ident)
  }

  private def rhsPlan(lhs: LogicalPlan, pattern: PatternExpression, interestingOrder: InterestingOrder, ctx: LogicalPlanningContext) = {
    val context = ctx.withUpdatedCardinalityInformation(lhs)
    val (plan, _) = context.strategy.planPatternExpression(lhs.availableSymbols, pattern, interestingOrder, context)
    plan
  }

  private def onePredicate(expressions: Set[Expression]): Expression = if (expressions.size == 1)
    expressions.head
  else
    Ors(expressions)(expressions.head.position)

  private def applicable(outerPlan: LogicalPlan, qg: QueryGraph, expression: Expression, solveds: Solveds) = {
    val symbolsAvailable = qg.argumentIds.subsetOf(outerPlan.availableSymbols)
    val isSolved = solveds.get(outerPlan.id).exists(_.queryGraph.selections.contains(expression))
    symbolsAvailable && !isSolved
  }


  private def freshId(patternExpression: PatternExpression) = {
    val name = FreshIdNameGenerator.name(patternExpression.position)
    (name, Variable(name)(patternExpression.position))
  }
}
