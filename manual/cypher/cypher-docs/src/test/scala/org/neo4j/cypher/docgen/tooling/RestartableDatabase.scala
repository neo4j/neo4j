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

/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.test.TestGraphDatabaseFactory

import scala.util.Try

/* I exist so my users can have a restartable database that is lazily created */
class RestartableDatabase(init: Seq[String], factory: TestGraphDatabaseFactory = new TestGraphDatabaseFactory()) {
  private var _db: GraphDatabaseQueryService = null
  private var _engine: ExecutionEngine = null
  private var _failures: Seq[QueryRunResult] = null
  private var _markedForRestart = false

  /*
  This is the public way of controlling when it's safe to restart the database
   */
  def nowIsASafePointToRestartDatabase() = if(_markedForRestart) restart()

  private def createAndStartIfNecessary() {
    if (_db == null) {
      _db = new GraphDatabaseCypherService(factory.newImpermanentDatabase())
      _engine = new ExecutionEngine(_db)
      _failures = initialize(_engine, init)
    }
  }

  def failures = {
    createAndStartIfNecessary()
    _failures
  }

  def getInnerDb = {
    createAndStartIfNecessary()
    _db
  }

  def shutdown() {
    restart()
  }

  def execute(q: String) = {
    createAndStartIfNecessary()
    val executionResult = try {
      _engine.execute(q)
    } catch {
      case e: Throwable => _markedForRestart = true; throw e
    }
    _markedForRestart = executionResult.queryStatistics().containsUpdates
    executionResult
  }

  private def restart() {
    if (_db == null) return
    _db.getGraphDatabaseService.shutdown()
    _db = null
    _markedForRestart = false
  }

  private def initialize(engine: ExecutionEngine, init: Seq[String]): Seq[QueryRunResult] =
    init.flatMap { q =>
      val result = Try(engine.execute(q))
      result.failed.toOption.map((e: Throwable) => QueryRunResult(q, new ErrorPlaceHolder(), Left(e)))
    }

}
