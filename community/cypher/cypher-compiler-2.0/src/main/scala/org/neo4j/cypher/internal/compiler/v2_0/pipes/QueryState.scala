/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.cypher.internal.compiler.v2_0.spi.{QueryContext, UpdateCountingQueryContext}
import org.neo4j.cypher.ParameterNotFoundException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.GraphDatabaseAPI
import scala.collection.mutable.ListBuffer

case class QueryState(db: GraphDatabaseService,
                      inner: QueryContext,
                      params: Map[String, Any],
                      decorator: PipeDecorator,
                      collectStatistics: Boolean = true,
                      timeReader: TimeReader = new TimeReader,
                      var initialContext: Option[ExecutionContext] = None,
                      _cleanupTasks: ListBuffer[() => Unit] = ListBuffer.empty) extends CleanupTaskList {
  def readTimeStamp(): Long = timeReader.getTime

  private val wrappedContext = if (collectStatistics) new UpdateCountingQueryContext(inner) else inner

  val query: QueryContext = wrappedContext

  def graphDatabaseAPI: GraphDatabaseAPI = db match {
    case i: GraphDatabaseAPI => i
    case _                   => throw new IllegalStateException("Graph database does not implement GraphDatabaseAPI")
  }

  def getParam(key: String): Any =
    params.getOrElse(key, throw new ParameterNotFoundException("Expected a parameter named " + key))

  def getStatistics = wrappedContext.getOptStatistics.get

  def cleanupTasks: Seq[() => Unit] = _cleanupTasks.toSeq

  def addCleanupTask(task: () => Unit) {
    _cleanupTasks += task
  }
}

class TimeReader {
  lazy val getTime = System.currentTimeMillis()
}

