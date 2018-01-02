/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_3.planner.AggregatingQueryProjection
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan

object aggregation {
  def apply(plan: LogicalPlan, aggregation: AggregatingQueryProjection)(implicit context: LogicalPlanningContext): LogicalPlan = {

    val groupingExpressions: Map[String, Expression] = aggregation.groupingKeys

    val identifiersToKeep: Map[String, Expression] = aggregation.aggregationExpressions.flatMap {
      case (_, exp) => exp.dependencies
    }.toList.distinct.map {
      case id => id.name -> id
    }.toMap

    //  TODO: we need to project here since the pipe does not do that,
    //  when moving to the new runtime the aggregation pipe MUST do the projection itself
    val projectedPlan = projection(plan, groupingExpressions ++ identifiersToKeep)
    context.logicalPlanProducer.planAggregation(projectedPlan, groupingExpressions, aggregation.aggregationExpressions)
  }
}
