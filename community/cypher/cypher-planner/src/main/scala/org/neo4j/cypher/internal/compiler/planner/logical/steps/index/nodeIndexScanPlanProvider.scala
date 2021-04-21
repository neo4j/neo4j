/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.plans.Scannable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexMatch
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object nodeIndexScanPlanProvider extends AbstractNodeIndexScanPlanProvider {

  /**
   * Represents that we can solve solvedPredicates, solvedHint and providedOrder by creating a plan from indexScanParameters.
   * We put values in this container so that we can avoid creating duplicate plans.
   */
  case class Solution(
    indexScanParameters: NodeIndexScanParameters,
    solvedPredicates: Seq[Expression],
    solvedHint: Option[UsingIndexHint],
    providedOrder: ProvidedOrder,
  )

  /**
   * Container for all values that define a NodeIndexScan plan
   */
  case class NodeIndexScanParameters(
    idName: String,
    label: LabelToken,
    properties: Seq[IndexedProperty],
    argumentIds: Set[String],
    indexOrder: IndexOrder,
  )

  override def createPlans(indexMatches: Set[IndexMatch], hints: Set[Hint], argumentIds: Set[String], restrictions: LeafPlanRestrictions, context: LogicalPlanningContext): Set[LogicalPlan] = {
    val solutions = for {
      indexMatch <- indexMatches
      if isAllowedByRestrictions(indexMatch, restrictions)
    } yield createSolution(indexMatch, hints, argumentIds, context)

    val distinctSolutions = mergeSolutions(solutions)

    distinctSolutions.map(solution =>
      context.logicalPlanProducer.planNodeIndexScan(
        idName = solution.indexScanParameters.idName,
        label = solution.indexScanParameters.label,
        properties = solution.indexScanParameters.properties,
        solvedPredicates = solution.solvedPredicates,
        solvedHint = solution.solvedHint,
        argumentIds = solution.indexScanParameters.argumentIds,
        providedOrder = solution.providedOrder,
        indexOrder = solution.indexScanParameters.indexOrder,
        context = context,
      )
    )
  }

  private def createSolution(indexMatch: IndexMatch, hints: Set[Hint], argumentIds: Set[String], context: LogicalPlanningContext): Solution = {
    val predicateSet = indexMatch.predicateSet(predicatesForIndexScan(indexMatch.propertyPredicates), exactPredicatesCanGetValue = false)

    val hint = predicateSet.matchingHints(hints).find(_.spec.fulfilledByScan)

    Solution(
      NodeIndexScanParameters(
        idName = indexMatch.variableName,
        label = indexMatch.labelToken,
        properties = predicateSet.indexedProperties(context),
        argumentIds = argumentIds,
        indexOrder = indexMatch.indexOrder,
      ),
      solvedPredicates = predicateSet.allSolvedPredicates,
      solvedHint = hint,
      providedOrder = indexMatch.providedOrder,
    )
  }

  private def predicatesForIndexScan(propertyPredicates: Seq[IndexCompatiblePredicate]): Seq[IndexCompatiblePredicate] =
    propertyPredicates.map(_.convertToScannable)

  private def mergeSolutions(solutions: Set[Solution]): Set[Solution] =
    solutions
      .groupBy(_.indexScanParameters)
      .map { case (parameters, solutions) => Solution(
        indexScanParameters = parameters,
        solvedPredicates = mergeSolvedPredicates(solutions).toSeq,
        solvedHint = solutions.flatMap(_.solvedHint).headOption,
        providedOrder = solutions.map(_.providedOrder).head,
      )}
      .toSet

  private def mergeSolvedPredicates(solutions: Set[Solution]): Set[Expression] = {
    val allSolved = solutions.flatMap(_.solvedPredicates)

    def equivalentAlreadySolved(predicate: Expression) =
      allSolved.exists(Scannable.isEquivalentScannable(predicate, _))

    allSolved.filter {
      // Discard partial predicates if we already solve it directly
      case pp: PartialPredicate[_] if equivalentAlreadySolved(pp.coveredPredicate) => false
      case _                                                                       => true
    }
  }
}
