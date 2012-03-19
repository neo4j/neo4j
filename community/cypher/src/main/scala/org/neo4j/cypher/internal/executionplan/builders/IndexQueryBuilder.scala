/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.executionplan.{QueryToken, Unsolved, PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.pipes.{RelationshipStartPipe, NodeStartPipe, Pipe}
import org.neo4j.graphdb.{Relationship, Node, GraphDatabaseService}
import collection.JavaConverters._
import java.lang.{Iterable=>JIterable}

class IndexQueryBuilder(graph: GraphDatabaseService) extends PlanBuilder {
  def apply(pipe: Pipe, q: PartiallySolvedQuery) = {
    val item = q.start.filter(filter).head

    val newPipe = createStartPipe(pipe, item.token)

    (newPipe, q.copy(start = q.start.filterNot(_ == item) :+ item.solve))
  }

  private def filter(q: QueryToken[_]): Boolean = q match {
    case Unsolved(NodeByIndexQuery(_, _, _)) => true
    case Unsolved(NodeByIndex(_, _, _, _)) => true
    case Unsolved(RelationshipByIndexQuery(_, _, _)) => true
    case Unsolved(RelationshipByIndex(_, _, _, _)) => true
    case _ => false
  }

  private def createStartPipe(lastPipe: Pipe, item: StartItem): Pipe = item match {
    case NodeByIndex(varName, idxName, key, value) =>
      new NodeStartPipe(lastPipe, varName, m => {
        val keyVal = key(m).toString
        val valueVal = value(m)
        val indexHits: JIterable[Node] = graph.index.forNodes(idxName).get(keyVal, valueVal)
        indexHits.asScala
      })

    case RelationshipByIndex(varName, idxName, key, value) =>
      new RelationshipStartPipe(lastPipe, varName, m => {
        val keyVal = key(m).toString
        val valueVal = value(m)
        val indexHits: JIterable[Relationship] = graph.index.forRelationships(idxName).get(keyVal, valueVal)
        indexHits.asScala
      })

    case NodeByIndexQuery(varName, idxName, query) =>
      new NodeStartPipe(lastPipe, varName, m => {
        val queryText = query(m)
        val indexHits: JIterable[Node] = graph.index.forNodes(idxName).query(queryText)
        indexHits.asScala
      })

    case RelationshipByIndexQuery(varName, idxName, query) =>
      new RelationshipStartPipe(lastPipe, varName, m => {
        val queryText = query(m)
        val indexHits: JIterable[Relationship] = graph.index.forRelationships(idxName).query(queryText)
        indexHits.asScala
      })
  }


  def isDefinedAt(pipe: Pipe, q: PartiallySolvedQuery): Boolean = q.start.exists(filter)

  def priority: Int = PlanBuilder.IndexQuery
}