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

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexSeekPlanProvider.mergeQueryExpressionsToSingleOne
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.EntityIndexSeekPlanProvider.predicatesForIndexSeek
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexLeafPlanner.NodeIndexMatch
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.IndexType
import org.neo4j.internal.kernel.api.PropertyIndexQuery.allEntries

abstract class AbstractNodeIndexSeekPlanProvider extends NodeIndexPlanProvider {

  case class Solution(
    variable: LogicalVariable,
    label: LabelToken,
    properties: Seq[IndexedProperty],
    isUnique: Boolean,
    valueExpr: QueryExpression[Expression],
    hint: Option[UsingIndexHint],
    argumentIds: Set[LogicalVariable],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    solvedPredicates: Seq[Expression],
    indexType: IndexType,
    supportPartitionedScan: Boolean
  )

  protected def constructPlan(solution: Solution, context: LogicalPlanningContext): LogicalPlan

  def createSolution(
    indexMatch: NodeIndexMatch,
    hints: Set[Hint],
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): Option[Solution] = {

    val predicateSet =
      indexMatch.predicateSet(predicatesForIndexSeek(indexMatch.propertyPredicates), exactPredicatesCanGetValue = true)

    if (predicateSet.propertyPredicates.forall(_.isExists)) {
      None
    } else {

      val queryExpression: QueryExpression[Expression] =
        mergeQueryExpressionsToSingleOne(predicateSet.propertyPredicates)

      val properties = predicateSet.indexedProperties(context)

      val hint = predicateSet
        .fulfilledHints(hints, indexMatch.indexDescriptor.indexType, planIsScan = false)
        .headOption

      val supportsPartitionedScans = queryExpression match {
        case _: SingleQueryExpression[_] | _: RangeQueryExpression[_] =>
          // NOTE: we still need to check at runtime if we can use a partitioned scan since it is dependent on
          // values etc
          indexMatch.indexDescriptor.maybeKernelIndexCapability.exists(_.supportPartitionedScan(allEntries()))
        case _ => false
      }

      Some(Solution(
        indexMatch.variable,
        indexMatch.labelToken,
        properties,
        indexMatch.indexDescriptor.isUnique,
        queryExpression,
        hint,
        argumentIds,
        indexMatch.providedOrder,
        indexMatch.indexOrder,
        predicateSet.allSolvedPredicates,
        indexMatch.indexDescriptor.indexType,
        supportsPartitionedScans
      ))
    }
  }
}
