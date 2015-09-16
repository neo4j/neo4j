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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_0.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v3_0.planner.{AggregatingQueryProjection, PlannerQuery}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{LogicalPlanningContext, PlanTransformer}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.frontend.v3_0.ast._

object considerAggregationByCountStore extends PlanTransformer[PlannerQuery] {

  def apply(plan: LogicalPlan, query: PlannerQuery)(implicit context: LogicalPlanningContext) = query.horizon match {
    case AggregatingQueryProjection(groupingKeys, aggregatingExpressions, shuffle)
      if (groupingKeys.isEmpty && aggregatingExpressions.size == 1) => aggregatingExpressions.head match {
      case (aggregationIdent, FunctionInvocation(FunctionName("count"), false, Vector(Identifier(countName)))) => plan match {
        case a: Aggregation => a.left match {
          // MATCH (n) RETURN count(n)
          case AllNodesScan(nodeId, argumentIds) if (countName == nodeId.name) =>
            context.logicalPlanProducer.planCountStoreNodeAggregation(plan, IdName(aggregationIdent), None, argumentIds)
          // MATCH ()-[r]->(), MATCH ()-[r:X]->(), MATCH ()-[r:X|Y]->()
          case Expand(AllNodesScan(nodeId, argumentIds), from, direction, types, to, relId, ExpandAll) if (countName == relId.name) =>
            context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, IdName(aggregationIdent), None, types, None, argumentIds)
          // MATCH (:A)-[r]->(), MATCH (:A)<-[r]-()
          case Expand(NodeByLabelScan(nodeId, label, argumentIds), from, direction, types, to, relId, ExpandAll) if (countName == relId.name) => direction match {
            // MATCH (:A)-[r]->()
            case OUTGOING =>
              context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, IdName(aggregationIdent), Some(label), types, None, argumentIds)
            // MATCH (:A)<-[r]-()
              // TODO: Consider BOTH!!!
            case _ =>
              context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, IdName(aggregationIdent), None, types, Some(label), argumentIds)
          }
          // MATCH (:A)-[r]->(), MATCH (:A)<-[r]-()
          case Selection(predicates, Expand(AllNodesScan(nodeId, argumentIds), from, direction, types, to, relId, ExpandAll))
            // TODO: consider predicates with more than just HasLabel, by maintaining a Selection() around the plan
            if (countName == relId.name && predicates.length == 1) => predicates.head match {
              case HasLabels(Identifier(nodeIdentifier), labels) if(labels.length == 1) => direction match {
                // MATCH (:A)<-[r]-()
                case OUTGOING =>
                  context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, IdName(aggregationIdent), None, types, Some(LazyLabel(labels.head)(context.semanticTable)), argumentIds)
                // MATCH (:A)-[r]->()
                // TODO: Consider BOTH!!!
                case _ =>
                  context.logicalPlanProducer.planCountStoreRelationshipAggregation(plan, IdName(aggregationIdent), Some(LazyLabel(labels.head)(context.semanticTable)), types, None, argumentIds)
              }
              case _ => plan
          }
          // MATCH (n:A) RETURN count(n)
            // TODO: This query could be planned with Selection(HasLabels,AllNodeScan), so we should consider this too!
          case NodeByLabelScan(nodeId, label, argumentIds) if (countName == nodeId.name) =>
            context.logicalPlanProducer.planCountStoreNodeAggregation(plan, IdName(aggregationIdent), Some(label), argumentIds)
          case _ => plan
        }
        case _ => plan
      }
      case _ => plan
    }
    case _ => plan
  }
}
