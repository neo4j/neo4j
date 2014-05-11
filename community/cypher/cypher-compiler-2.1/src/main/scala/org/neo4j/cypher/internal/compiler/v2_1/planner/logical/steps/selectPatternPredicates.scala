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

case class selectPatternPredicates(simpleSelection: PlanTransformer) extends PlanTransformer {
  private object candidateListProducer extends CandidateGenerator[PlanTable] {
    def apply(planTable: PlanTable)(implicit context: LogicalPlanContext): CandidateList = {
      val queryGraph = context.query.graph
      val applyCandidates =
        for (
          lhs <- planTable.plans;
          pattern <- queryGraph.selections.patternPredicatesGiven(lhs.availableSymbols)
          if applicable(lhs, queryGraph, pattern))
        yield {
          pattern match {
            case p@Not(patternExpression: PatternExpression) =>
              val rhs = rhsPlan(context, patternExpression)
              planAntiSemiApply(lhs, rhs, patternExpression, p)
            case p@Ors(Not(patternExpression: PatternExpression) :: tail) if doesNotContainPatterns(tail) =>
              val rhs = rhsPlan(context, patternExpression)
              planSelectOrAntiSemiApply(lhs, rhs, onePredicate(tail), pattern)
            case p@Ors((patternExpression: PatternExpression) :: tail) if doesNotContainPatterns(tail) =>
              val rhs = rhsPlan(context, patternExpression)
              planSelectOrSemiApply(lhs, rhs, onePredicate(tail), pattern)
            case patternExpression: PatternExpression =>
              val rhs = rhsPlan(context, patternExpression)
              planSemiApply(lhs, rhs, patternExpression)
          }
        }

      CandidateList(applyCandidates)
    }

    private def rhsPlan(context: LogicalPlanContext, pattern: PatternExpression) = {
      val qg = context.subQueriesLookupTable.getOrElse(pattern,
        throw new ThisShouldNotHappenError("Davide/Stefan", s"Did not find QueryGraph for pattern expression $pattern")
      )
      context.strategy.plan(context.copy(query = PlannerQuery(graph = qg)))
    }

    private def doesNotContainPatterns(e: Seq[Expression]) = !e.exists(_.exists { case e: PatternExpression => true })

    private def onePredicate(expressions: Seq[Expression]): Expression = expressions.toList match {
      case e :: Nil => e
      case predicates => Ors(predicates)(predicates.head.position)
    }

    private def applicable(outerPlan: QueryPlan, qg: QueryGraph, expression: Expression) = {
      val symbolsAvailable = qg.argumentIds.subsetOf(outerPlan.availableSymbols)
      val isSolved = outerPlan.solved.graph.selections.contains(expression)
      symbolsAvailable && !isSolved
    }
  }

  def apply(input: QueryPlan)(implicit context: LogicalPlanContext): QueryPlan = {
    val plan = simpleSelection(input)

    def findBestPlanForPatternPredicates(plan: QueryPlan): QueryPlan = {
      val secretPlanTable = PlanTable(Map(plan.availableSymbols -> plan))
      val result: CandidateList = candidateListProducer(secretPlanTable)
      result.bestPlan(context.cost).getOrElse(plan)
    }

    iterateUntilConverged(findBestPlanForPatternPredicates)(plan)
  }
}
