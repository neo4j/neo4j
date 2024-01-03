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
package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexLeafPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.QueryExpression

object EntityIndexSeekPlanProvider {

  // Index seeks can solve
  //   1. equality predicates for an arbitrary prefix of its properties
  //   2. one seekable predicate
  //   3. existence predicates for any other of its properties
  // see https://neo4j.com/docs/cypher-manual/current/administration/indexes-for-search-performance/#administration-indexes-single-vs-composite-index
  def predicatesForIndexSeek(propertyPredicates: Seq[IndexCompatiblePredicate]): Seq[IndexCompatiblePredicate] = {
    val (exactPrefix, rest) = propertyPredicates.span(_.predicateExactness.isExact)

    val (seekablePrefix, nonSeekableSuffix) = rest match {
      case Seq(next, tail @ _*) if next.predicateExactness.isSeekable => (exactPrefix :+ next, tail)
      case _                                                          => (exactPrefix, rest)
    }

    seekablePrefix ++ nonSeekableSuffix.map(_.convertToRangeScannable)
  }

  // Test if solving using this match is valid given the leaf plan restrictions
  def isAllowedByRestrictions(
    propertyPredicates: Seq[IndexCompatiblePredicate],
    restrictions: LeafPlanRestrictions
  ): Boolean = {
    def isAllowed(predicate: IndexCompatiblePredicate) = restrictions match {
      case LeafPlanRestrictions.NoRestrictions => true

      case LeafPlanRestrictions.OnlyIndexSeekPlansFor(variable, dependencyRestrictions) =>
        val isRestrictedVariable = predicate.variable == variable
        if (isRestrictedVariable) {
          val dependencies = predicate.dependencies
          dependencies.nonEmpty && dependencies.subsetOf(dependencyRestrictions)
        } else {
          true
        }
    }
    propertyPredicates.exists(isAllowed)
  }

  def mergeQueryExpressionsToSingleOne(predicates: Seq[IndexCompatiblePredicate]): QueryExpression[Expression] =
    if (predicates.length == 1) {
      predicates.head.queryExpression
    } else {
      CompositeQueryExpression(predicates.map(_.queryExpression))
    }
}
