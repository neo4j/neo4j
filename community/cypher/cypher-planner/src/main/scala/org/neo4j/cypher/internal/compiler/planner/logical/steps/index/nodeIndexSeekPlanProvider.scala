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
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexMatch
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.QueryExpression

object nodeIndexSeekPlanProvider extends AbstractNodeIndexSeekPlanProvider {

  override def createPlans(indexMatches: Set[IndexMatch], hints: Set[Hint], argumentIds: Set[String], restrictions: LeafPlanRestrictions, context: LogicalPlanningContext): Set[LogicalPlan] = for {
    indexMatch <- indexMatches
    if isAllowedByRestrictions(indexMatch, restrictions)
    plan <- doCreatePlans(indexMatch, hints, argumentIds, context)
  } yield plan

  override protected def constructPlan(
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
    predicatesForCardinalityEstimation: Seq[Expression]
  ): Option[LogicalPlan] =
    if (isUnique) {
      Some(context.logicalPlanProducer.planNodeUniqueIndexSeek(idName, label, properties, valueExpr, solvedPredicates, predicatesForCardinalityEstimation, hint, argumentIds, providedOrder, indexOrder, context))
    } else {
      Some(context.logicalPlanProducer.planNodeIndexSeek(idName, label, properties, valueExpr, solvedPredicates, predicatesForCardinalityEstimation, hint, argumentIds, providedOrder, indexOrder, context))
    }
}

