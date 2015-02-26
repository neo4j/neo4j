/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.helpers.FreshIdNameGenerator
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged
import org.neo4j.helpers.ThisShouldNotHappenError

case class selectPatternPredicates(simpleSelection: PlanTransformer[QueryGraph]) extends PlanTransformer[QueryGraph] {
  private object candidatesProducer extends CandidateGenerator[PlanTable] {
    def apply(planTable: PlanTable, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
      for (
        lhs <- planTable.plans;
        pattern <- queryGraph.selections.patternPredicatesGiven(lhs.availableSymbols)
        if applicable(lhs, queryGraph, pattern))
      yield {
        pattern match {
          case patternExpression: PatternExpression =>
            val rhs = rhsPlan(lhs.availableSymbols, patternExpression)
            planSemiApply(lhs, rhs, patternExpression)
          case p@Not(patternExpression: PatternExpression) =>
            val rhs = rhsPlan(lhs.availableSymbols, patternExpression)
            planAntiSemiApply(lhs, rhs, patternExpression, p)
          case p@Ors(exprs) =>
            val (patternExpressions, expressions) = exprs.partition {
              case _: PatternExpression => true
              case Not(_: PatternExpression) => true
              case _ => false
            }
            val (outerMostPlan, solvedPredicates) = planPredicates(lhs, patternExpressions, expressions, None)
            solvePredicate(outerMostPlan, onePredicate(solvedPredicates))
        }
      }
    }

    private def planPredicates(lhs: LogicalPlan, patternExpressions: Set[Expression], expressions: Set[Expression], letExpression: Option[Expression])
                              (implicit context: LogicalPlanningContext): (LogicalPlan, Set[Expression]) = {
      patternExpressions.toList match {
        case (patternExpression: PatternExpression) :: Nil =>
          val rhs = rhsPlan(lhs.availableSymbols, patternExpression)
          val plan = planSelectOrSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet))
          (plan, expressions + patternExpression)

        case (p@Not(patternExpression: PatternExpression)) :: Nil =>
          val rhs = rhsPlan(lhs.availableSymbols, patternExpression)
          val plan = planSelectOrAntiSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet))
          (plan, expressions + p)

        case (patternExpression: PatternExpression) :: tail =>
          val rhs = rhsPlan(lhs.availableSymbols, patternExpression)
          val (newLhs, newLetExpr) = createLetSemiApply(lhs, rhs, patternExpression, expressions, letExpression)
          val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr))
          (plan, solvedPredicates ++ Set(patternExpression) ++ expressions)

        case (p@Not(patternExpression: PatternExpression)) :: tail =>
          val rhs = rhsPlan(lhs.availableSymbols, patternExpression)
          val (newLhs, newLetExpr) = createLetAntiSemiApply(lhs, rhs, patternExpression, p, expressions, letExpression)
          val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr))
          (plan, solvedPredicates ++ Set(p) ++ expressions)

        case _ =>
          throw new ThisShouldNotHappenError("Davide", "There should be at least one pattern expression")
      }
    }

    private def createLetSemiApply(lhs: LogicalPlan, rhs: LogicalPlan, patternExpression: PatternExpression, expressions: Set[Expression], letExpression: Option[Expression]) = {
      val (idName, ident) = freshIdForLet(patternExpression)
      if (expressions.isEmpty && letExpression.isEmpty)
        (planLetSemiApply(lhs, rhs, idName), ident)
      else
        (planLetSelectOrSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet)), ident)
    }

    private def createLetAntiSemiApply(lhs: LogicalPlan, rhs: LogicalPlan, patternExpression: PatternExpression, predicate: Expression, expressions: Set[Expression], letExpression: Option[Expression]) = {
      val (idName, ident) = freshIdForLet(patternExpression)
      if (expressions.isEmpty && letExpression.isEmpty)
        (planLetAntiSemiApply(lhs, rhs, idName), ident)
      else
        (planLetSelectOrAntiSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet)), ident)
    }

    private def rhsPlan(planArguments: Set[IdName], pattern: PatternExpression)(implicit context: LogicalPlanningContext): LogicalPlan = {
      val (plan, _) = context.strategy.planPatternExpression(planArguments, pattern)
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
  }

  def freshIdForLet(patternExpression: PatternExpression) = {
    val name = FreshIdNameGenerator.name(patternExpression.position)
    (IdName(name), Identifier(name)(patternExpression.position))
  }

  def apply(input: LogicalPlan, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val plan = simpleSelection(input, queryGraph)

    def findBestPlanForPatternPredicates(plan: LogicalPlan): LogicalPlan = {
      val secretPlanTable = context.strategy.emptyPlanTable + plan
      val result = candidatesProducer(secretPlanTable, queryGraph)
      pickBestPlan(result.iterator).getOrElse(plan)
    }

    iterateUntilConverged(findBestPlanForPatternPredicates)(plan)
  }
}
