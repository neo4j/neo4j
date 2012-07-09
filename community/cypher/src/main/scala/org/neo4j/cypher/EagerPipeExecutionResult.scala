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
package org.neo4j.cypher

import internal.pipes.QueryState
import internal.symbols.SymbolTable
import org.neo4j.graphdb.{Transaction, GraphDatabaseService}
import org.neo4j.kernel.GraphDatabaseAPI
import collection.Map

class EagerPipeExecutionResult(r: => Traversable[Map[String, Any]],
                               symbols: SymbolTable,
                               columns: List[String],
                               state: QueryState,
                               db: GraphDatabaseService)
  extends PipeExecutionResult(r, symbols, columns) {

  override lazy val queryStatistics = QueryStatistics(
    nodesCreated = state.createdNodes.count,
    relationshipsCreated = state.createdRelationships.count,
    propertiesSet = state.propertySet.count,
    deletedNodes = state.deletedNodes.count,
    deletedRelationships = state.deletedRelationships.count)

  override val createTimedResults = {
    val start = System.currentTimeMillis()
    val eagerResult = immutableResult.toList

    val ms = System.currentTimeMillis() - start

    (eagerResult, ms.toString)
  }
}