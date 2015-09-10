/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Identifier
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningContext, PlanTransformer}
import org.neo4j.cypher.internal.compiler.v3_0.planner.{AggregatingQueryProjection, PlannerQuery}
import org.neo4j.cypher.internal.frontend.v3_0.ast.{FunctionName, FunctionInvocation}

object considerAggregationByCountStore extends PlanTransformer[PlannerQuery] {

  def apply(plan: LogicalPlan, query: PlannerQuery)(implicit context: LogicalPlanningContext) = query.horizon match {
    case AggregatingQueryProjection(groupingKeys, aggregatingExpressions, shuffle)
      if (groupingKeys.size == 0 && aggregatingExpressions.size == 1) => aggregatingExpressions.head._2 match {
      case FunctionInvocation(FunctionName("count"), false, Vector(Identifier(countName))) => plan match {
        case a: Aggregation => a.left match {
          case AllNodesScan(nodeId, argumentIds) if (countName == nodeId.name) =>
            context.logicalPlanProducer.planCountStoreNodeAggregation(plan, nodeId, None, argumentIds)
          // MATCH ()-[r]->(), MATCH ()-[r:X]->(), MATCH ()-[r:X|Y]->()
          case Expand(AllNodesScan(nodeId, argumentIds), from, direction, types, to, relId, ExpandAll) if (countName == relId.name) =>
            context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, relId, None, types, None, argumentIds)
          case NodeByLabelScan(nodeId, label, argumentIds) if (countName == nodeId.name) =>
            context.logicalPlanProducer.planCountStoreNodeAggregation(plan, nodeId, Some(label.name), argumentIds)
          case _ => plan
        }
        case _ => plan
      }
      case _ => plan
    }
    case _ => plan
  }
}
