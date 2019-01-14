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
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.ir.v3_4.QueryGraph
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.FreshIdNameGenerator
import org.neo4j.cypher.internal.v3_4.expressions._

case object selectPatternPredicates extends CandidateGenerator[LogicalPlan] {

  def apply(lhs: LogicalPlan, queryGraph: QueryGraph, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): Seq[LogicalPlan] = {
    for (
      pattern <- queryGraph.selections.patternPredicatesGiven(lhs.availableSymbols)
      if applicable(lhs, queryGraph, pattern, solveds))
      yield {
        pattern match {
          case patternExpression: PatternExpression =>
            val rhs = rhsPlan(lhs, patternExpression, context, solveds, cardinalities)
            context.logicalPlanProducer.planSemiApply(lhs, rhs, patternExpression, context)
          case p@Not(patternExpression: PatternExpression) =>
            val rhs = rhsPlan(lhs, patternExpression, context, solveds, cardinalities)
            context.logicalPlanProducer.planAntiSemiApply(lhs, rhs, patternExpression, p, context)
          case p@Ors(exprs) =>
            val (patternExpressions, expressions) = exprs.partition {
              case _: PatternExpression => true
              case Not(_: PatternExpression) => true
              case _ => false
            }
            val (plan, solvedPredicates) = planPredicates(lhs, patternExpressions, expressions, None, context, solveds, cardinalities)
            context.logicalPlanProducer.solvePredicate(plan, onePredicate(solvedPredicates), context)
        }
      }
  }

  private def planPredicates(lhs: LogicalPlan,
                             patternExpressions: Set[Expression],
                             expressions: Set[Expression],
                             letExpression: Option[Expression],
                             context: LogicalPlanningContext,
                             solveds: Solveds,
                             cardinalities: Cardinalities): (LogicalPlan, Set[Expression]) = {
    patternExpressions.toList match {
      case (patternExpression: PatternExpression) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression, context, solveds, cardinalities)
        val plan = context.logicalPlanProducer.planSelectOrSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet), context)
        (plan, expressions + patternExpression)

      case (p@Not(patternExpression: PatternExpression)) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression, context, solveds, cardinalities)
        val plan = context.logicalPlanProducer.planSelectOrAntiSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet), context)
        (plan, expressions + p)

      case (patternExpression: PatternExpression) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression, context, solveds, cardinalities)
        val (newLhs, newLetExpr) = createLetSemiApply(lhs, rhs, patternExpression, expressions, letExpression, context)
        val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr), context, solveds, cardinalities)
        (plan, solvedPredicates ++ Set(patternExpression) ++ expressions)

      case (p@Not(patternExpression: PatternExpression)) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression, context, solveds, cardinalities)
        val (newLhs, newLetExpr) = createLetAntiSemiApply(lhs, rhs, patternExpression, p, expressions, letExpression, context)
        val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr), context, solveds, cardinalities)
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

  private def rhsPlan(lhs: LogicalPlan, pattern: PatternExpression, ctx: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities) = {
    val context = ctx.withUpdatedCardinalityInformation(lhs, solveds, cardinalities)
    val (plan, _) = context.strategy.planPatternExpression(lhs.availableSymbols, pattern, context, solveds, cardinalities)
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
