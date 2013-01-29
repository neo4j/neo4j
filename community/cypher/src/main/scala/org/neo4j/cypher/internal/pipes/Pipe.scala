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

import org.neo4j.cypher.internal.symbols.SymbolTable
import org.neo4j.graphdb.GraphDatabaseService
import scala.collection.JavaConverters._
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.cypher.internal.spi.{UpdateCountingQueryContext, QueryContext}
import org.neo4j.cypher.internal.spi.gdsimpl.TransactionBoundQueryContext
import org.neo4j.cypher.internal.ExecutionContext

/**
 * Pipe is a central part of Cypher. Most pipes are decorators - they
 * wrap another pipe. ParamPipe and NullPipe the only exception to this.
 * Pipes are combined to form an execution plan, and when iterated over,
 * the execute the query.
 */
trait Pipe {
  def createResults(state: QueryState): Iterator[ExecutionContext]

  def symbols: SymbolTable

  def executionPlanDescription(): String
}

class NullPipe extends Pipe {
  def createResults(state: QueryState) = Seq(ExecutionContext.empty).toIterator

  def symbols: SymbolTable = new SymbolTable()

  def executionPlanDescription(): String = ""
}

object MutableMaps {
  def empty = collection.mutable.Map[String, Any]()

  def create(size: Int) = new java.util.HashMap[String, Any](size).asScala

  def create(input: scala.collection.Map[String, Any]) = new java.util.HashMap[String, Any](input.asJava).asScala

  def create(input: (String, Any)*) = {
    val m: java.util.HashMap[String, Any] = new java.util.HashMap[String, Any]()
    input.foreach {
      case (k, v) => m.put(k, v)
    }
    m.asScala
  }
}

object QueryState {
  def apply() = new QueryState(null, null, Map.empty)
  def apply(db: GraphDatabaseAPI) = new QueryState(db, new TransactionBoundQueryContext(db), Map.empty)
}

class QueryState(val db: GraphDatabaseService,
                 inner: QueryContext,
                 val params: Map[String, Any]) {

  private val updateTrackingQryCtx: UpdateCountingQueryContext = new UpdateCountingQueryContext(inner)
  val queryContext: QueryContext = updateTrackingQryCtx

  def getStatistics = updateTrackingQryCtx.getStatistics

  def graphDatabaseAPI: GraphDatabaseAPI = if (db.isInstanceOf[GraphDatabaseAPI])
    db.asInstanceOf[GraphDatabaseAPI]
  else
    throw new IllegalStateException("Graph database does not implement GraphDatabaseAPI")
}
