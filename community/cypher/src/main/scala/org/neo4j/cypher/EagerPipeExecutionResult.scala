/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher

import internal.pipes.QueryState
import org.neo4j.graphdb.GraphDatabaseService
import collection.Map
import javacompat.{PlanDescription => JPlanDescription}

class EagerPipeExecutionResult(result: Iterator[Map[String, Any]],
                               columns: List[String],
                               state: QueryState,
                               db: GraphDatabaseService,
                               planDescriptor: () => JPlanDescription)
  extends PipeExecutionResult(result, columns, state, planDescriptor) {

  val (eagerResult,timeTaken) = super.createTimedResults
  lazy val inner = eagerResult.iterator

  override def next() = inner.next().toMap
  override def hasNext = inner.hasNext

  override def queryStatistics = {
    QueryStatistics(
      nodesCreated = state.createdNodes.count,
      relationshipsCreated = state.createdRelationships.count,
      propertiesSet = state.propertySet.count,
      deletedNodes = state.deletedNodes.count,
      deletedRelationships = state.deletedRelationships.count)
  }

  override def createTimedResults = (eagerResult,timeTaken)
}