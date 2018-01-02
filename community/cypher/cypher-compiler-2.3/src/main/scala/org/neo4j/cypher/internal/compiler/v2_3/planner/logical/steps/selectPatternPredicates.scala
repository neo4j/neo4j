/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.FreshIdNameGenerator
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.helpers.ThisShouldNotHappenError

case object selectPatternPredicates extends CandidateGenerator[LogicalPlan] {

  def apply(lhs: LogicalPlan, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    for (
      pattern <- queryGraph.selections.patternPredicatesGiven(lhs.availableSymbols)
      if applicable(lhs, queryGraph, pattern))
      yield {
        pattern match {
          case patternExpression: PatternExpression =>
            val rhs = rhsPlan(lhs, patternExpression)
            context.logicalPlanProducer.planSemiApply(lhs, rhs, patternExpression)
          case p@Not(patternExpression: PatternExpression) =>
            val rhs = rhsPlan(lhs, patternExpression)
            context.logicalPlanProducer.planAntiSemiApply(lhs, rhs, patternExpression, p)
          case p@Ors(exprs) =>
            val (patternExpressions, expressions) = exprs.partition {
              case _: PatternExpression => true
              case Not(_: PatternExpression) => true
              case _ => false
            }
            val (plan, solvedPredicates) = planPredicates(lhs, patternExpressions, expressions, None)
            context.logicalPlanProducer.solvePredicate(plan, onePredicate(solvedPredicates))
        }
      }
  }

  private def planPredicates(lhs: LogicalPlan, patternExpressions: Set[Expression], expressions: Set[Expression], letExpression: Option[Expression])
                            (implicit context: LogicalPlanningContext): (LogicalPlan, Set[Expression]) = {
    patternExpressions.toList match {
      case (patternExpression: PatternExpression) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression)
        val plan = context.logicalPlanProducer.planSelectOrSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet))
        (plan, expressions + patternExpression)

      case (p@Not(patternExpression: PatternExpression)) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression)
        val plan = context.logicalPlanProducer.planSelectOrAntiSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet))
        (plan, expressions + p)

      case (patternExpression: PatternExpression) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression)
        val (newLhs, newLetExpr) = createLetSemiApply(lhs, rhs, patternExpression, expressions, letExpression)
        val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr))
        (plan, solvedPredicates ++ Set(patternExpression) ++ expressions)

      case (p@Not(patternExpression: PatternExpression)) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression)
        val (newLhs, newLetExpr) = createLetAntiSemiApply(lhs, rhs, patternExpression, p, expressions, letExpression)
        val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr))
        (plan, solvedPredicates ++ Set(p) ++ expressions)

      case _ =>
        throw new ThisShouldNotHappenError("Davide", "There should be at least one pattern expression")
    }
  }

  private def createLetSemiApply(lhs: LogicalPlan, rhs: LogicalPlan, patternExpression: PatternExpression, expressions: Set[Expression], letExpression: Option[Expression])
                                (implicit context: LogicalPlanningContext) = {
    val (idName, ident) = freshId(patternExpression)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.logicalPlanProducer.planLetSemiApply(lhs, rhs, idName), ident)
    else
      (context.logicalPlanProducer.planLetSelectOrSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet)), ident)
  }

  private def createLetAntiSemiApply(lhs: LogicalPlan, rhs: LogicalPlan, patternExpression: PatternExpression, predicate: Expression, expressions: Set[Expression], letExpression: Option[Expression])
                                    (implicit context: LogicalPlanningContext) = {
    val (idName, ident) = freshId(patternExpression)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.logicalPlanProducer.planLetAntiSemiApply(lhs, rhs, idName), ident)
    else
      (context.logicalPlanProducer.planLetSelectOrAntiSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet)), ident)
  }

  private def rhsPlan(lhs: LogicalPlan, pattern: PatternExpression)(implicit ctx: LogicalPlanningContext) = {
    val context = ctx.recurse(lhs)
    val (plan, _) = context.strategy.planPatternExpression(lhs.availableSymbols, pattern)(context)
    plan
  }

  private def onePredicate(expressions: Set[Expression]): Expression = if (expressions.size == 1)
    expressions.head
  else
    Ors(expressions)(expressions.head.position)

  private def applicable(outerPlan: LogicalPlan, qg: QueryGraph, expression: Expression) = {
    val symbolsAvailable = qg.argumentIds.subsetOf(outerPlan.availableSymbols)
    val isSolved = outerPlan.solved.exists(_.graph.selections.contains(expression))
    symbolsAvailable && !isSolved
  }


  private def freshId(patternExpression: PatternExpression) = {
    val name = FreshIdNameGenerator.name(patternExpression.position)
    (IdName(name), Identifier(name)(patternExpression.position))
  }
}
