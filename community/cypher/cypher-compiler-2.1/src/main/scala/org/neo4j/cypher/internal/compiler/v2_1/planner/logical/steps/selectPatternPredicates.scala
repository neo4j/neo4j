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

case class selectPatternPredicates(simpleSelection: PlanTransformer) extends PlanTransformer {
  private object candidateListProducer extends CandidateGenerator[PlanTable] {
    def apply(planTable: PlanTable)(implicit context: LogicalPlanContext): CandidateList = {
      val applyCandidates =
        for (pattern <- context.queryGraph.patternPredicates;
             lhs <- planTable.plans if applicable(lhs.plan, pattern))
        yield {
          val rhs = context.strategy.plan(context.copy(queryGraph = pattern.queryGraph))
          val exp: Expression = pattern.predicate.exp
          exp match {
            case _: Not =>
              AntiSemiApply(lhs.plan, rhs.plan)(pattern)
            case Ors((_: Not) :: tail) if doesNotContainPatterns(tail) =>
              SelectOrAntiSemiApply(lhs.plan, rhs.plan, onePredicate(tail))(pattern)
            case Ors(_ :: tail) if doesNotContainPatterns(tail) =>
              SelectOrSemiApply(lhs.plan, rhs.plan, onePredicate(tail))(pattern)
            case _ =>
              SemiApply(lhs.plan, rhs.plan)(pattern)
          }
        }

      CandidateList(applyCandidates.map(QueryPlan))
    }

    private def doesNotContainPatterns(e: Seq[Expression]) = !e.exists(_.exists {
      case e: PatternExpression => true
    })

    private def onePredicate(expressions: Seq[Expression]): Expression = expressions.toList match {
      case e :: Nil => e
      case predicates => Ors(predicates)(predicates.head.position)
    }

    private def applicable(outerPlan: LogicalPlan, inner: SubQuery) = {
      inner match {
        case e: Exists => {
          val providedIds = outerPlan.coveredIds
          val hasDependencies = inner.queryGraph.argumentIds.forall(providedIds.contains)
          val isSolved = outerPlan.solved.selections.contains(e.predicate.exp)
          hasDependencies && !isSolved
        }
      }
    }
  }

  def apply(input: QueryPlan)(implicit context: LogicalPlanContext): QueryPlan = {
    val plan = simpleSelection(input)

    def findBestPlanForPatternPredicates(plan: QueryPlan): QueryPlan = {
      val secretPlanTable = PlanTable(Map(plan.coveredIds -> plan))
      val result: CandidateList = candidateListProducer(secretPlanTable)
      result.bestPlan(context.cost).getOrElse(plan)
    }

    iterateUntilConverged(findBestPlanForPatternPredicates)(plan)
  }
}
