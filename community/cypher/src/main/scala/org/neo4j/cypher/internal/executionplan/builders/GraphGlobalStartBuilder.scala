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
import org.neo4j.graphdb.GraphDatabaseService
import collection.JavaConverters._
import org.neo4j.tooling.GlobalGraphOperations

class GraphGlobalStartBuilder(graph: GraphDatabaseService) extends PlanBuilder {
  def apply(pipe: Pipe, q: PartiallySolvedQuery) = {
    val item = q.start.filter(filter).head

    val newPipe = createStartPipe(pipe, item.token)

    (newPipe, q.copy(start = q.start.filterNot(_ == item) :+ item.solve))
  }

  private def filter(q: QueryToken[_]) = q match {
    case Unsolved(AllNodes(_)) => true
    case Unsolved(AllRelationships(_)) => true
    case _ => false
  }

  private def createStartPipe(lastPipe: Pipe, item: StartItem): Pipe = item match {
    case AllNodes(identifierName) => new NodeStartPipe(lastPipe, identifierName, m => GlobalGraphOperations.at(graph).getAllNodes.asScala)
    case AllRelationships(identifierName) => new RelationshipStartPipe(lastPipe, identifierName, m => GlobalGraphOperations.at(graph).getAllRelationships.asScala)
  }

  def isDefinedAt(p: Pipe, q: PartiallySolvedQuery) = q.start.exists(filter)

  def priority = PlanBuilder.GlobalStart
}