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

import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.plans.Scannable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder

object EntityIndexScanPlanProvider {

  /**
   * Represents that we can solve solvedPredicates, solvedHint and providedOrder by creating a plan from indexScanParameters.
   * We put values in this container so that we can avoid creating duplicate plans.
   */
  case class Solution[PARAMETERS](
                                   indexScanParameters: PARAMETERS,
                                   solvedPredicates: Seq[Expression],
                                   solvedHint: Option[UsingIndexHint],
                                   providedOrder: ProvidedOrder,
                                 )

  private[index] def predicatesForIndexScan(propertyPredicates: Seq[IndexCompatiblePredicate]): Seq[IndexCompatiblePredicate] =
    propertyPredicates.map(_.convertToScannable)

  private[index] def mergeSolutions[PARAMETERS](solutions: Set[Solution[PARAMETERS]]): Set[Solution[PARAMETERS]] =
    solutions
      .groupBy(_.indexScanParameters)
      .map { case (parameters, solutions) => Solution(
        indexScanParameters = parameters,
        solvedPredicates = mergeSolvedPredicates(solutions).toSeq,
        solvedHint = solutions.flatMap(_.solvedHint).headOption,
        providedOrder = solutions.map(_.providedOrder).head,
      )}
      .toSet

  private def mergeSolvedPredicates[PARAMETERS](solutions: Set[Solution[PARAMETERS]]): Set[Expression] = {
    val allSolved = solutions.flatMap(_.solvedPredicates)

    def equivalentAlreadySolved(predicate: Expression) =
      allSolved.exists(Scannable.isEquivalentScannable(predicate, _))

    allSolved.filter {
      // Discard partial predicates if we already solve it directly
      case pp: PartialPredicate[_] if equivalentAlreadySolved(pp.coveredPredicate) => false
      case _                                                                       => true
    }
  }

  def isAllowedByRestrictions(variableName: String, restrictions: LeafPlanRestrictions): Boolean =
    !restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners.contains(variableName)

}
