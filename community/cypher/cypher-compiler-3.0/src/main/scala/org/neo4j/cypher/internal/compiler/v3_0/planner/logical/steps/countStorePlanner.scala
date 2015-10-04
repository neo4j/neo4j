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

import org.neo4j.cypher.internal.compiler.v3_0.pipes.{LazyLabel, LazyTypes}
import org.neo4j.cypher.internal.compiler.v3_0.planner._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection.{BOTH, INCOMING, OUTGOING}
import org.neo4j.cypher.internal.frontend.v3_0.ast._

case object countStorePlanner {

  def apply(query: PlannerQuery)(implicit context: LogicalPlanningContext): Option[LogicalPlan] = {
    implicit val semanticTable = context.semanticTable
    query.horizon match {
      case AggregatingQueryProjection(groupingKeys, aggregatingExpressions, shuffle)
        if groupingKeys.isEmpty && aggregatingExpressions.size == 1 => aggregatingExpressions.head match {

        case (aggregationIdent, FunctionInvocation(FunctionName("count"), false, Vector(Identifier(countName)))) =>

          query.queryGraph match {

            case QueryGraph(patternRelationships, patternNodes, argumentIds, selections, Seq(), hints, shortestPathPatterns)
              if hints.isEmpty && shortestPathPatterns.isEmpty =>
                if (patternNodes.size == 1 && patternRelationships.isEmpty && patternNodes.head.name == countName && noWrongPredicates(Set(patternNodes.head), selections)) {
                  // MATCH (n), MATCH (n:A)
                  Some(context.logicalPlanProducer.planCountStoreNodeAggregation(
                    query, IdName(aggregationIdent), findLabel(patternNodes.head, selections), argumentIds)(context))
                } else if (patternRelationships.size == 1) {
                  // MATCH ()-[r]->(), MATCH ()-[r:X]->(), MATCH ()-[r:X|Y]->()
                  patternRelationships.head match {

                    case PatternRelationship(relId, (startNodeId, endNodeId), direction, types, SimplePatternLength)
                      if relId.name == countName && noWrongPredicates(Set(startNodeId, endNodeId), selections) =>

                      def planRelAggr(fromLabel: Option[LazyLabel], toLabel: Option[LazyLabel], bothDirections: Boolean = false) =
                        Some(context.logicalPlanProducer.planCountStoreRelationshipAggregation(query, IdName(aggregationIdent), fromLabel, LazyTypes(types.map(_.name)), toLabel, bothDirections, argumentIds)(context))

                      (findLabel(startNodeId, selections), direction, findLabel(endNodeId, selections)) match {
                        case (None,       BOTH,     None)     => planRelAggr(None, None, bothDirections = true)
                        case (None,       BOTH,     endLabel) => planRelAggr(endLabel, None, bothDirections = true)
                        case (startLabel, BOTH,     None)     => planRelAggr(None, startLabel, bothDirections = true)
                        case (None,       _,        None)     => planRelAggr(None, None)
                        case (None,       OUTGOING, endLabel) => planRelAggr(None, endLabel)
                        case (startLabel, OUTGOING, None)     => planRelAggr(startLabel, None)
                        case (None,       INCOMING, endLabel) => planRelAggr(endLabel, None)
                        case (startLabel, INCOMING, None)     => planRelAggr(None, startLabel)
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

  def noWrongPredicates(nodeIds: Set[IdName], selections: Selections): Boolean = {
    val (labelPredicates, other) = selections.predicates.partition {
      case Predicate(nIds, h: HasLabels) if nIds.forall(nodeIds.contains) && h.labels.size == 1 => true
      case _ => false
    }
    labelPredicates.size <= 1 && other.isEmpty
  }

  def findLabel(nodeId: IdName, selections: Selections): Option[LazyLabel] = selections.predicates.collectFirst {
    case Predicate(nIds, h: HasLabels) if nIds == Set(nodeId) && h.labels.size == 1 => LazyLabel(h.labels.head.name)
  }
}
