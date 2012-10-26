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
package org.neo4j.cypher.internal.pipes

import java.lang.String
import org.neo4j.cypher.internal.symbols.SymbolTable
import collection.Iterator
import org.neo4j.cypher.internal.mutation.UpdateAction
import org.neo4j.graphdb.{GraphDatabaseService, Transaction}
import collection.mutable.{Queue, Map => MutableMap}
import scala.collection.JavaConverters._
import java.util.HashMap
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.cypher.ParameterNotFoundException
import java.util.concurrent.atomic.AtomicInteger

/**
 * Pipe is a central part of Cypher. Most pipes are decorators - they
 * wrap another pipe. ParamPipe and NullPipe the only exception to this.
 * Pipes are combined to form an execution plan, and when iterated over,
 * the execute the query.
 */
trait Pipe {
  def createResults(state: QueryState): Traversable[ExecutionContext]

  def symbols: SymbolTable

  def executionPlan(): String
}

class NullPipe extends Pipe {
  def createResults(state: QueryState) = Seq(ExecutionContext.empty)

  def symbols: SymbolTable = new SymbolTable()

  def executionPlan(): String = ""
}

object MutableMaps {
  def empty = collection.mutable.Map[String, Any]()

  def create(size: Int) = new java.util.HashMap[String, Any](size).asScala

  def create(input: scala.collection.Map[String, Any]) = new java.util.HashMap[String, Any](input.asJava).asScala

  def create(input: (String, Any)*) = {
    val m: HashMap[String, Any] = new java.util.HashMap[String, Any]()
    input.foreach {
      case (k, v) => m.put(k, v)
    }
    m.asScala
  }
}

object QueryState {
  def apply() = new QueryState(null, Map.empty)
}

class QueryState(val db: GraphDatabaseService,
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

object ExecutionContext {
  def empty = new ExecutionContext()

  def from(x: (String, Any)*) = new ExecutionContext().newWith(x)
}

case class ExecutionContext(m: MutableMap[String, Any] = MutableMaps.empty,
                            mutationCommands: Queue[UpdateAction] = Queue.empty,
                            params: Map[String, Any] = Map.empty)
  extends MutableMap[String, Any] {
  def get(key: String): Option[Any] = m.get(key)

  def getParam(key: String): Any =
    params.getOrElse(key, throw new ParameterNotFoundException("Expected a parameter named " + key))

  def iterator: Iterator[(String, Any)] = m.iterator

  override def size = m.size

  def ++(other: ExecutionContext): ExecutionContext = copy(m = m ++ other.m)

  override def foreach[U](f: ((String, Any)) => U) {
    m.foreach(f)
  }

  def +=(kv: (String, Any)) = {
    m += kv
    this
  }

  def -=(key: String) = {
    m -= key
    this
  }

  def newWith(newEntries: Seq[(String, Any)]) = {
    copy(m = (MutableMaps.create(this.m) ++= newEntries))
  }

  def newWith(newEntries: scala.collection.Map[String, Any]) = {
    copy(m = (MutableMaps.create(this.m) ++= newEntries))
  }

  def newFrom(newEntries: Seq[(String, Any)]) = {
    copy(m = MutableMaps.create(newEntries: _*))
  }

  def newFrom(newEntries: scala.collection.Map[String, Any]) = {
    copy(m = MutableMaps.create(newEntries))
  }

  def newWith(newEntry: (String, Any)) = {
    copy(m = (MutableMaps.create(this.m) += newEntry))
  }

  override def clone: ExecutionContext = newFrom(m)
}