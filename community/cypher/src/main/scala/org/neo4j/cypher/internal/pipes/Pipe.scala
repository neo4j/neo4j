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

object QueryState {
  def apply() = new QueryState(MutableMap())
}

class QueryState( val params: MutableMap[String, Any],
                  var transaction: Option[Transaction] = None) {
  val createdNodes = new Counter
  val createdRelationships = new Counter
  val propertySet = new Counter
  val deletedNodes = new Counter
  val deletedRelationships = new Counter
}

class Counter {
  private var counter = 0L;
  def count = counter
  def increase() { counter += 1 }
}

object ExecutionContext {
  def empty = new ExecutionContext(null, MutableMap())
}

case class ExecutionContext(db: GraphDatabaseService,
                            m: MutableMap[String, Any],
                            mutationCommands: Queue[UpdateAction] = Queue[UpdateAction]())
  extends MutableMap[String, Any] {
  def get(key: String): Option[Any] = m.get(key)

  def iterator: Iterator[(String, Any)] = m.iterator

  def +=(kv: (String, Any)) = {
    m += kv
    this
  }

  def -=(key: String) = {
    m -= key
    this
  }
}