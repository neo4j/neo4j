/**
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
package org.neo4j.cypher.internal.compiler.v1_9.executionplan.builders

import org.neo4j.cypher.internal.compiler.v1_9.commands._
import org.neo4j.cypher.internal.compiler.v1_9.pipes.{RelationshipStartPipe, NodeStartPipe, Pipe, EntityProducer}
import org.neo4j.graphdb.{Relationship, Node, GraphDatabaseService}
import org.neo4j.cypher.MissingIndexException
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{ExecutionPlanInProgress, PlanBuilder}

class IndexQueryBuilder(graph: GraphDatabaseService) extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val p = plan.pipe

    val item = q.start.filter(filter).head

    val newPipe = createStartPipe(p, item.token)

    plan.copy(pipe = newPipe, query = q.copy(start = q.start.filterNot(_ == item) :+ item.solve))
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = plan.query.start.exists(filter)

  private def filter(q: QueryToken[_]): Boolean = q match {
    case Unsolved(NodeByIndexQuery(_, _, _))         => true
    case Unsolved(NodeByIndex(_, _, _, _))           => true
    case Unsolved(RelationshipByIndexQuery(_, _, _)) => true
    case Unsolved(RelationshipByIndex(_, _, _, _))   => true
    case _                                           => false
  }

  private def createStartPipe(lastPipe: Pipe, item: StartItem): Pipe = item match {
    case NodeByIndex(varName, idxName, key, value) =>
      new NodeStartPipe(lastPipe, varName, IndexQueryBuilder.getNodeGetter(item, graph))

    case RelationshipByIndex(varName, idxName, key, value) =>
      new RelationshipStartPipe(lastPipe, varName, IndexQueryBuilder.getRelationshipGetter(item, graph))

    case NodeByIndexQuery(varName, idxName, query) =>
      new NodeStartPipe(lastPipe, varName, IndexQueryBuilder.getNodeGetter(item, graph))

    case RelationshipByIndexQuery(varName, idxName, query) =>
      new RelationshipStartPipe(lastPipe, varName, IndexQueryBuilder.getRelationshipGetter(item, graph))
  }

  def priority: Int = PlanBuilder.IndexQuery
}

object IndexQueryBuilder {
  def getNodeGetter(startItem: StartItem, graph: GraphDatabaseService): EntityProducer[Node] =
    startItem match {

      case NodeByIndex(varName, idxName, key, value) =>
        checkNodeIndex(idxName, graph)
        (m, state) => {
          val keyVal = key(m)(state).toString
          val valueVal = value(m)(state)
          state.query.nodeOps.indexGet(idxName,keyVal, valueVal)
        }

      case NodeByIndexQuery(varName, idxName, query) =>
        checkNodeIndex(idxName, graph)
        (m, state) => {
          val queryText = query(m)(state)
          state.query.nodeOps.indexQuery(idxName, queryText)
        }

      case NodeById(varName, ids) =>
        (m, state) =>
          GetGraphElements.getElements[Node](ids(m)(state), varName, state.query.nodeOps.getById)
    }

  def getRelationshipGetter(startItem: StartItem, graph: GraphDatabaseService): EntityProducer[Relationship] =
    startItem match {

      case RelationshipByIndex(varName, idxName, key, value) =>
        checkRelIndex(idxName, graph)
        (m, state) => {
          val keyVal = key(m)(state).toString
          val valueVal = value(m)(state)
          state.query.relationshipOps.indexGet(idxName, keyVal, valueVal)
        }

      case RelationshipByIndexQuery(varName, idxName, query) =>
        checkRelIndex(idxName, graph)
        (m, state) => {
          val queryText = query(m)(state)
          state.query.relationshipOps.indexQuery(idxName, queryText)
        }
    }

  private def checkNodeIndex(idxName: String, graph: GraphDatabaseService) {
    if (!graph.index.existsForNodes(idxName)) throw new MissingIndexException(idxName)
  }

  private def checkRelIndex(idxName: String, graph: GraphDatabaseService) {
    if (!graph.index.existsForRelationships(idxName)) throw new MissingIndexException(idxName)
  }
}
