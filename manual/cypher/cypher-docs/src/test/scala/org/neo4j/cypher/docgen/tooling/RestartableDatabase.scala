/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.docgen.tooling

import org.neo4j.cypher.ExecutionEngineHelper
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.test.TestGraphDatabaseFactory

import scala.util.Try

/* I exist so my users can have a restartable database that is lazily created */
class RestartableDatabase(init: Seq[String], factory: TestGraphDatabaseFactory = new TestGraphDatabaseFactory())
 extends GraphIcing with ExecutionEngineHelper {

  var graph: GraphDatabaseCypherService = null
  var eengine: ExecutionEngine = null
  private var _failures: Seq[QueryRunResult] = null
  private var _markedForRestart = false

  /*
  This is the public way of controlling when it's safe to restart the database
   */
  def nowIsASafePointToRestartDatabase() = if(_markedForRestart) restart()

  private def createAndStartIfNecessary() {
    if (graph == null) {
      graph = new GraphDatabaseCypherService(factory.newImpermanentDatabase())
      eengine = new ExecutionEngine(graph)
      _failures = initialize(init)
    }
  }

  def failures = {
    createAndStartIfNecessary()
    _failures
  }

  def getInnerDb = {
    createAndStartIfNecessary()
    graph
  }

  def shutdown() {
    restart()
  }

  def execute(q: String): InternalExecutionResult = {
    createAndStartIfNecessary()
    val executionResult: InternalExecutionResult = try {
      execute(q, Seq.empty:_*)
    } catch {
      case e: Throwable => _markedForRestart = true; throw e
    }
    _markedForRestart = executionResult.queryStatistics().containsUpdates
    executionResult
  }

  private def restart() {
    if (graph == null) return
    graph.getGraphDatabaseService.shutdown()
    graph = null
    _markedForRestart = false
  }

  private def initialize(init: Seq[String]): Seq[QueryRunResult] =
    init.flatMap { q =>
      val result = Try(execute(q, Seq.empty:_*))
      result.failed.toOption.map((e: Throwable) => QueryRunResult(q, new ErrorPlaceHolder(), Left(e)))
    }
}
