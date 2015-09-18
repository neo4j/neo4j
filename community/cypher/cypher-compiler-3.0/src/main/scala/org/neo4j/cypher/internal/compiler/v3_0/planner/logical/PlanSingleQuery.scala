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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{SimplePatternLength, IdName, PatternRelationship, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.steps.verifyBestPlan
import org.neo4j.cypher.internal.frontend.v3_0.{SemanticTable, Rewriter}
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection._
import org.neo4j.cypher.internal.frontend.v3_0.ast._

/*
This coordinates PlannerQuery planning and delegates work to the classes that do the actual planning of
QueryGraphs and EventHorizons
 */
case class PlanSingleQuery(planPart: (PlannerQuery, LogicalPlanningContext, Option[LogicalPlan]) => LogicalPlan = planPart,
                           planEventHorizon: LogicalPlanningFunction2[PlannerQuery, LogicalPlan, LogicalPlan] = PlanEventHorizon(),
                           expressionRewriterFactory: (LogicalPlanningContext => Rewriter) = ExpressionRewriterFactory,
                           planWithTail: LogicalPlanningFunction2[LogicalPlan, Option[PlannerQuery], LogicalPlan] = PlanWithTail()) extends LogicalPlanningFunction1[PlannerQuery, LogicalPlan] {

  override def apply(in: PlannerQuery)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val partPlan = countStorePlanPart(in, context, None).getOrElse(planPart(in, context, None))

    val projectedPlan = planEventHorizon(in, partPlan)
    val projectedContext = context.recurse(projectedPlan)
    val expressionRewriter = expressionRewriterFactory(projectedContext)
    val completePlan = projectedPlan.endoRewrite(expressionRewriter)

    val finalPlan = planWithTail(completePlan, in.tail)(projectedContext)
    verifyBestPlan(finalPlan, in)
  }
}

case object countStorePlanPart extends ((PlannerQuery, LogicalPlanningContext, Option[LogicalPlan]) => Option[LogicalPlan]) {
  def apply(query: PlannerQuery, context: LogicalPlanningContext, leafPlan: Option[LogicalPlan]): Option[LogicalPlan] = {
    implicit val semanticTable = context.semanticTable
    query.horizon match {
      case AggregatingQueryProjection(groupingKeys, aggregatingExpressions, shuffle)
        if (groupingKeys.isEmpty && aggregatingExpressions.size == 1) => aggregatingExpressions.head match {

        case (aggregationIdent, a@FunctionInvocation(FunctionName("count"), false, Vector(Identifier(countName)))) =>

          query.graph match {

            case QueryGraph(patternRelationships, patternNodes, argumentIds, selections, Seq(), hints, shortestPathPatterns)
              if hints.isEmpty && shortestPathPatterns.isEmpty =>
                if (patternNodes.size == 1 && patternRelationships.isEmpty && patternNodes.head.name == countName) {
                  def planNodeAggr(label: Option[LazyLabel]) =
                    Some(context.logicalPlanProducer.planCountStoreNodeAggregation(query, IdName(aggregationIdent), label, argumentIds)(context))
                  convertToLabels(patternNodes.head, selections) match {
                    // MATCH (n)
                    case (Nil) => planNodeAggr(None)
                    // MATCH (n:X)
                    case (nodeLabel :: Nil) => planNodeAggr(Some(nodeLabel))
                    // MATCH (n:X:Y) - UNSUPPORTED by stats
                    case _ => None
                  }
                } else if(patternRelationships.size == 1) {
                  // MATCH ()-[r]->(), MATCH ()-[r:X]->(), MATCH ()-[r:X|Y]->()
                  patternRelationships.head match {

                    case PatternRelationship(relId, (startNodeId, endNodeId), direction, types, SimplePatternLength) =>

                      def planRelAggr(fromLabel: Option[LazyLabel], toLabel: Option[LazyLabel]) =
                        Some(context.logicalPlanProducer.planCountStoreRelationshipAggregation(query, IdName(aggregationIdent), fromLabel, types, toLabel, argumentIds)(context))

                      (convertToLabels(startNodeId, selections), direction, convertToLabels(endNodeId, selections)) match {
                        case (Nil,               _,        Nil)             => planRelAggr(None, None)
                        case (Nil,               OUTGOING, endLabel :: Nil) => planRelAggr(None, Some(endLabel))
                        case (startLabel :: Nil, OUTGOING, Nil)             => planRelAggr(Some(startLabel), None)
                        case (Nil,               INCOMING, endLabel :: Nil) => planRelAggr(Some(endLabel), None)
                        case (startLabel :: Nil, INCOMING, Nil)             => planRelAggr(None, Some(startLabel))
                        case _ => None
                      }

                    case _ => None
                  }
                } else None

            case _ => None
          }

        case _ => None
      }

      case _ => None
    }
  }
  def convertToLabels(nodeId: IdName, selections: Selections)(implicit semanticTable: SemanticTable): Seq[LazyLabel] =
    selections.predicates.collectFirst {
      case Predicate(nIds,h:HasLabels) if nIds.head.name == nodeId.name =>
        h.labels.map(n => LazyLabel(n.name))
    }.getOrElse(Seq.empty[LazyLabel])
}



/*case a: Aggregation => a.left match {
// MATCH (n) RETURN count(n)
case AllNodesScan(nodeId, argumentIds) if (countName == nodeId.name) =>
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
}*/


/*
*/
