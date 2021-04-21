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
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexCompatiblePredicate
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexMatch
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.QueryExpression

abstract class AbstractNodeIndexSeekPlanProvider extends NodeIndexPlanProvider {

  // Test if solving using this match is valid given the leaf plan restrictions
  def isAllowedByRestrictions(indexMatch: IndexMatch, restrictions: LeafPlanRestrictions): Boolean = {
    def isAllowed(predicate: IndexCompatiblePredicate) = restrictions match {
      case LeafPlanRestrictions.NoRestrictions => true

      case LeafPlanRestrictions.OnlyIndexSeekPlansFor(variable, dependencyRestrictions) =>
        val isRestrictedVariable = predicate.variable.name == variable
        if (isRestrictedVariable) predicate.dependencies.map(_.name) == dependencyRestrictions
        else true
    }
    indexMatch.propertyPredicates.exists(isAllowed)
  }

  protected def constructPlan(
    idName: String,
    label: LabelToken,
    properties: Seq[IndexedProperty],
    isUnique: Boolean,
    valueExpr: QueryExpression[Expression],
    hint: Option[UsingIndexHint],
    argumentIds: Set[String],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    context: LogicalPlanningContext,
    solvedPredicates: Seq[Expression],
    predicatesForCardinalityEstimation: Seq[Expression],
  ): LogicalPlan

  def doCreatePlans(indexMatch: IndexMatch, hints: Set[Hint], argumentIds: Set[String], context: LogicalPlanningContext): Set[LogicalPlan] = {

    val predicateSet = indexMatch.predicateSet(predicatesForIndexSeek(indexMatch.propertyPredicates), exactPredicatesCanGetValue = true)

    if (predicateSet.propertyPredicates.forall(_.isExists)) {
      Set.empty
    } else {

      val queryExpression: QueryExpression[Expression] = mergeQueryExpressionsToSingleOne(predicateSet.propertyPredicates)

      val properties = predicateSet.indexedProperties(context)

      val hint = predicateSet.matchingHints(hints).headOption

      val originalPredicates = indexMatch.propertyPredicates

      // TODO: This seems very unfair if there is a tail of non-seekable predicates
      val predicatesForCardinalityEstimation = originalPredicates.map(p => p.predicate) :+ indexMatch.labelPredicate

      Set(constructPlan(
        indexMatch.variableName,
        indexMatch.labelToken,
        properties,
        indexMatch.indexDescriptor.isUnique,
        queryExpression,
        hint,
        argumentIds,
        indexMatch.providedOrder,
        indexMatch.indexOrder,
        context,
        predicateSet.allSolvedPredicates,
        predicatesForCardinalityEstimation,
      ))
    }
  }

  // Index seeks can solve
  //   1. equality predicates for an arbitrary prefix of its properties
  //   2. one seekable predicate
  //   3. existence predicates for any other of its properties
  // see https://neo4j.com/docs/cypher-manual/current/administration/indexes-for-search-performance/#administration-indexes-single-vs-composite-index
  private def predicatesForIndexSeek(propertyPredicates: Seq[IndexCompatiblePredicate]): Seq[IndexCompatiblePredicate] = {
    val (exactPrefix, rest) = propertyPredicates.span(_.predicateExactness.isExact)

    val (seekablePrefix, nonSeekableSuffix) = rest match {
      case Seq()              => (exactPrefix, rest)
      case Seq(next, tail@_*) => (exactPrefix :+ next, tail)
    }

    seekablePrefix ++ nonSeekableSuffix.map(_.convertToScannable)
  }

  private def mergeQueryExpressionsToSingleOne(predicates: Seq[IndexCompatiblePredicate]): QueryExpression[Expression] =
    if (predicates.length == 1)
      predicates.head.queryExpression
    else {
      CompositeQueryExpression(predicates.map(_.queryExpression))
    }
}
