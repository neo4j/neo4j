/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_1.helpers.FreshIdNameGenerator

case class selectPatternPredicates(simpleSelection: PlanTransformer) extends PlanTransformer {
  private object candidateListProducer extends CandidateGenerator[PlanTable] {
    def apply(planTable: PlanTable)(implicit context: QueryGraphSolvingContext): CandidateList = {
      val queryGraph = context.queryGraph
      val applyCandidates =
        for (
          lhs <- planTable.plans;
          pattern <- queryGraph.selections.patternPredicatesGiven(lhs.availableSymbols)
          if applicable(lhs, queryGraph, pattern))
        yield {
          pattern match {
            case patternExpression: PatternExpression =>
              val rhs = rhsPlan(context, patternExpression)
              planSemiApply(lhs, rhs, patternExpression)
            case p@Not(patternExpression: PatternExpression) =>
              val rhs = rhsPlan(context, patternExpression)
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

      CandidateList(applyCandidates)
    }

    private def planPredicates(lhs: QueryPlan, patternExpressions: Set[Expression], expressions: Set[Expression], letExpression: Option[Expression])
                              (implicit context: QueryGraphSolvingContext): (QueryPlan, Set[Expression]) = {
      patternExpressions.toList match {
        case (patternExpression: PatternExpression) :: Nil =>
          val rhs = rhsPlan(context, patternExpression)
          val plan = planSelectOrSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet))
          (plan, expressions + patternExpression)

        case (p@Not(patternExpression: PatternExpression)) :: Nil =>
          val rhs = rhsPlan(context, patternExpression)
          val plan = planSelectOrAntiSemiApply(lhs, rhs, onePredicate(expressions ++ letExpression.toSet))
          (plan, expressions + p)

        case (patternExpression: PatternExpression) :: tail =>
          val rhs = rhsPlan(context, patternExpression)
          val (newLhs, newLetExpr) = createLetSemiApply(lhs, rhs, patternExpression, expressions, letExpression)
          val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr))
          (plan, solvedPredicates ++ Set(patternExpression) ++ expressions)

        case (p@Not(patternExpression: PatternExpression)) :: tail =>
          val rhs = rhsPlan(context, patternExpression)
          val (newLhs, newLetExpr) = createLetAntiSemiApply(lhs, rhs, patternExpression, p, expressions, letExpression)
          val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr))
          (plan, solvedPredicates ++ Set(p) ++ expressions)

        case _ =>
          throw new ThisShouldNotHappenError("Davide", "There should be at least one pattern expression")
      }
    }

    private def createLetSemiApply(lhs: QueryPlan, rhs: QueryPlan, patternExpression: PatternExpression, expressions: Set[Expression], letExpression: Option[Expression]) = {
      val (idName, ident) = freshIdForLet(patternExpression)
      if (expressions.isEmpty && letExpression.isEmpty)
        (planLetSemiApply(lhs, rhs, idName), ident)
      else
        (planLetSelectOrSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet)), ident)
    }

    private def createLetAntiSemiApply(lhs: QueryPlan, rhs: QueryPlan, patternExpression: PatternExpression, predicate: Expression, expressions: Set[Expression], letExpression: Option[Expression]) = {
      val (idName, ident) = freshIdForLet(patternExpression)
      if (expressions.isEmpty && letExpression.isEmpty)
        (planLetAntiSemiApply(lhs, rhs, idName), ident)
      else
        (planLetSelectOrAntiSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet)), ident)
    }

    private def rhsPlan(context: QueryGraphSolvingContext, pattern: PatternExpression) = {
      val qg = context.subQueriesLookupTable.getOrElse(pattern,
        throw new ThisShouldNotHappenError("Davide/Stefan", s"Did not find QueryGraph for pattern expression $pattern")
      )
      context.strategy.plan(context.copy(queryGraph = qg))
    }

    private def onePredicate(expressions: Set[Expression]): Expression = if (expressions.size == 1)
      expressions.head
    else
      Ors(expressions)(expressions.head.position)

    private def applicable(outerPlan: QueryPlan, qg: QueryGraph, expression: Expression) = {
      val symbolsAvailable = qg.argumentIds.subsetOf(outerPlan.availableSymbols)
      val isSolved = outerPlan.solved.graph.selections.contains(expression)
      symbolsAvailable && !isSolved
    }
  }

  def freshIdForLet(patternExpression: PatternExpression) = {
    val name = FreshIdNameGenerator.name(patternExpression.position)
    (IdName(name), Identifier(name)(patternExpression.position))
  }

  def apply(input: QueryPlan)(implicit context: QueryGraphSolvingContext): QueryPlan = {
    val plan = simpleSelection(input)

    def findBestPlanForPatternPredicates(plan: QueryPlan): QueryPlan = {
      val secretPlanTable = PlanTable(Map(plan.availableSymbols -> plan))
      val result: CandidateList = candidateListProducer(secretPlanTable)
      result.bestPlan(context.cost).getOrElse(plan)
    }

    iterateUntilConverged(findBestPlanForPatternPredicates)(plan)
  }
}
