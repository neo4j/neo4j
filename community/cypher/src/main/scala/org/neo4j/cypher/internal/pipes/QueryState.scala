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
package org.neo4j.cypher.internal.pipes

import org.neo4j.graphdb.{Transaction, GraphDatabaseService}
import org.neo4j.cypher.internal.spi.gdsimpl.TransactionBoundQueryContext
import org.neo4j.cypher.internal.spi.{UpdateCountingQueryContext, QueryContext}
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.cypher.ParameterNotFoundException

case class QueryState(db: GraphDatabaseService,
                      inner: QueryContext,
                      params: Map[String, Any],
                      decorator: PipeDecorator,
                      var transaction: Option[Transaction] = None) {

  private val updateTrackingQryCtx: UpdateCountingQueryContext = new UpdateCountingQueryContext(inner)
  val query: QueryContext = updateTrackingQryCtx


  def graphDatabaseAPI: GraphDatabaseAPI = if (db.isInstanceOf[GraphDatabaseAPI])
    db.asInstanceOf[GraphDatabaseAPI]
  else
    throw new IllegalStateException("Graph database does not implement GraphDatabaseAPI")

  def getParam(key: String): Any =
    params.getOrElse(key, throw new ParameterNotFoundException("Expected a parameter named " + key))

  def getStatistics = updateTrackingQryCtx.getStatistics
}
