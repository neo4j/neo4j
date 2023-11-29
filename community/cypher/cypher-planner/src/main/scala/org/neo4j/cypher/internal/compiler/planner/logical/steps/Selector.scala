/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.PlanSelector
import org.neo4j.cypher.internal.compiler.planner.logical.SelectorHeuristic
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds

case class Selector(
  pickBestFactory: CandidateSelectorFactory,
  candidateGenerators: SelectionCandidateGenerator*
) extends PlanSelector {

  /**
   * Given a plan, using SelectionCandidateGenerators, plan all selections currently possible in the order cheapest first and return the resulting plan.
   */
  override def apply(
    input: LogicalPlan,
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val pickBest = pickBestFactory(context)

    val unsolvedPredicates =
      unsolvedPreds(context.staticComponents.planningAttributes.solveds, queryGraph.selections, input)

    def selectIt(plan: LogicalPlan, stillUnsolvedPredicates: Set[Expression]): LogicalPlan = {
      val candidates = candidateGenerators.flatMap(generator =>
        generator(plan, stillUnsolvedPredicates, queryGraph, interestingOrderConfig, context)
      )

      def stringifiedSolvedPredicates(e: Set[Expression]) = {
        val es = ExpressionStringifier(e => e.asCanonicalStringVal)
        e.map(es(_)).mkString(", ")
      }

      candidates match {
        case Seq()                              => plan
        case Seq(candidate)                     => candidate.plan
        case candidates if candidates.size <= 3 =>
          // If we have only 2 or 3 candidates we can cost compare all alternative orders of applying the selections.
          val candidatesWithAllSelectionsApplied = candidates.map {
            case SelectionCandidate(plan, solvedPredicates) =>
              selectIt(plan, stillUnsolvedPredicates -- solvedPredicates)
          }
          pickBest(
            candidatesWithAllSelectionsApplied,
            s"best selection candidate for ${stringifiedSolvedPredicates(stillUnsolvedPredicates)}"
          ).get
        case _ =>
          // If we have more than 2 candidates we pick the cheapest first greedily and then recurse.
          pickBest.applyWithResolvedPerPlan[SelectionCandidate](
            _.plan,
            candidates,
            s"greedily cheapest selection candidate for ${stringifiedSolvedPredicates(stillUnsolvedPredicates)}",
            plan =>
              "Solved predicates: " + stringifiedSolvedPredicates(candidates.collectFirst {
                case SelectionCandidate(`plan`, solvedPredicates) => solvedPredicates
              }.get),
            SelectorHeuristic.constant
          ) match {
            case Some(SelectionCandidate(plan, solvedPredicates)) =>
              selectIt(plan, stillUnsolvedPredicates -- solvedPredicates)
            case None => plan
          }
      }
    }

    selectIt(input, unsolvedPredicates)
  }

  /**
   * All unsolved predicates. Includes scalar and pattern predicates.
   */
  private def unsolvedPreds(solveds: Solveds, s: Selections, l: LogicalPlan): Set[Expression] =
    s.predicatesGiven(l.availableSymbols)
      .filterNot(predicate =>
        solveds.get(l.id).asSinglePlannerQuery.exists(_.queryGraph.selections.contains(predicate))
      )
      .toSet

}

trait SelectionCandidateGenerator extends {

  /**
   * Generate candidates which solve a predicate.
   * @param input the current plan
   * @param unsolvedPredicates all predicates which are left to be solved
   * @param queryGraph the query graph to solve
   * @return candidates, where each candidate is a plan building on top of input that solves some predicates.
   */
  def apply(
    input: LogicalPlan,
    unsolvedPredicates: Set[Expression],
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Iterator[SelectionCandidate]
}

case class SelectionCandidate(plan: LogicalPlan, solvedPredicates: Set[Expression])
