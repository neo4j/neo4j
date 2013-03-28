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
import org.neo4j.cypher.internal.spi.QueryContext
import org.neo4j.kernel.GraphDatabaseAPI
import java.util.concurrent.atomic.AtomicInteger
import org.neo4j.cypher.ParameterNotFoundException
import org.neo4j.cypher.internal.spi.gdsimpl.GDSBackedQueryContext


object QueryState {
  def empty: QueryState = apply(null)

  def apply(): QueryState = empty

  def apply(db: GraphDatabaseService): QueryState =
    new QueryState(db, new GDSBackedQueryContext(db), Map.empty, NullDecorator, None, new TimeReader)
}

case class QueryState(db: GraphDatabaseService,
                      query: QueryContext,
                      params: Map[String, Any],
                      decorator: PipeDecorator,
                      var transaction: Option[Transaction] = None,
                      timeReader: TimeReader = new TimeReader) {
  def readTimeStamp(): Long = timeReader.getTime


  val createdNodes = new Counter
  val createdRelationships = new Counter
  val propertySet = new Counter
  val deletedNodes = new Counter
  val deletedRelationships = new Counter

  def graphDatabaseAPI: GraphDatabaseAPI = if (db.isInstanceOf[GraphDatabaseAPI])
    db.asInstanceOf[GraphDatabaseAPI]
  else
    throw new IllegalStateException("Graph database does not implement GraphDatabaseAPI")

  def getParam(key: String): Any =
    params.getOrElse(key, throw new ParameterNotFoundException("Expected a parameter named " + key))
}

class Counter {
  private val counter: AtomicInteger = new AtomicInteger()

  def count: Int = counter.get()

  def increase() {
    counter.incrementAndGet()
  }
}

class TimeReader {
  lazy val getTime = System.currentTimeMillis()
}