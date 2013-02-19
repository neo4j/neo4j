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
import org.neo4j.graphdb.{GraphDatabaseService, Transaction}
import scala.collection.JavaConverters._
import org.neo4j.kernel.GraphDatabaseAPI
import java.util.concurrent.atomic.AtomicInteger
import org.neo4j.cypher.internal.spi.QueryContext
import org.neo4j.cypher.internal.spi.gdsimpl.GDSBackedQueryContext
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.PlanDescription

/**
 * Pipe is a central part of Cypher. Most pipes are decorators - they
 * wrap another pipe. ParamPipe and NullPipe the only exception to this.
 * Pipes are combined to form an execution plan, and when iterated over,
 * the execute the query.
 */
trait Pipe {
  def createResults(state: QueryState): Iterator[ExecutionContext]

  def symbols: SymbolTable

  def executionPlanDescription: PlanDescription
}

class NullPipe extends Pipe {
  def createResults(state: QueryState) = Seq(ExecutionContext.empty).toIterator

  def symbols: SymbolTable = new SymbolTable()

  def executionPlanDescription = PlanDescription("Null")
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
  def apply(db: GraphDatabaseService) = new QueryState(db, new GDSBackedQueryContext(db), Map.empty, None)
}

class QueryState(val db: GraphDatabaseService,
                 val query: QueryContext,
                 val params: Map[String, Any],
                 var transaction: Option[Transaction] = None) {
  val createdNodes = new Counter
  val createdRelationships = new Counter
  val propertySet = new Counter
  val deletedNodes = new Counter
  val deletedRelationships = new Counter

  def graphDatabaseAPI: GraphDatabaseAPI = if (db.isInstanceOf[GraphDatabaseAPI])
    db.asInstanceOf[GraphDatabaseAPI]
  else
    throw new IllegalStateException("Graph database does not implement GraphDatabaseAPI")
}

class Counter {
  private val counter: AtomicInteger = new AtomicInteger()

  def count: Int = counter.get()

  def increase() {
    counter.incrementAndGet()
  }
}
