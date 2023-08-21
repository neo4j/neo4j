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

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext.PlannerState
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.plannerQueryPlanner
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.Ref

trait ExistsSubqueryPlanner {

  def planInnerOfExistsSubquery(
    subquery: ExistsIRExpression,
    labelInfo: LabelInfo,
    context: LogicalPlanningContext
  ): LogicalPlan
}

case object ExistsSubqueryPlanner extends ExistsSubqueryPlanner {

  override def planInnerOfExistsSubquery(
    subquery: ExistsIRExpression,
    labelInfo: LabelInfo,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val subqueryContext = context.withModifiedPlannerState(_.withFusedLabelInfo(labelInfo))
    plannerQueryPlanner.planSubquery(subquery, subqueryContext)
  }
}

final case class ExistsSubqueryPlannerWithCaching() extends ExistsSubqueryPlanner {

  private val cachedPlanInnerOfExistsSubquery = CachedFunction(doPlan _)

  override def planInnerOfExistsSubquery(
    subquery: ExistsIRExpression,
    labelInfo: LabelInfo,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    cachedPlanInnerOfExistsSubquery(Ref(subquery), labelInfo, computeContextCacheKey(context))
  }

  private def doPlan(
    subqueryRef: Ref[ExistsIRExpression],
    labelInfo: LabelInfo,
    context: CachedFunction.CacheKey[Unit, LogicalPlanningContext]
  ): LogicalPlan = {
    ExistsSubqueryPlanner.planInnerOfExistsSubquery(subqueryRef.value, labelInfo, context.value)
  }

  private def computeContextCacheKey(context: LogicalPlanningContext)
    : CachedFunction.CacheKey[Unit, LogicalPlanningContext] = {
    CachedFunction.CacheKey.computeFrom(context) {
      // The same EXISTS subquery can be planned with different instances of LogicalPlanningContext, but it is assumed
      // that the resulting plan will be the same regardless of any particular field.
      // To avoid generating cache-misses and re-computing the same plan multiple times, we're excluding
      // LogicalPlanningContext from the cache key.
      // When adding a new field to LogicalPlanningContext, consider if this assumption still holds. If not, add relevant
      // fields to the cache key below.
      case LogicalPlanningContext(
          _,
          _,
          PlannerState(_, _, _, _, _, _)
        ) =>
        ()
    }
  }
}
