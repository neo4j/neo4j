/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.GqlStatusObject
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.notifications.NotificationImplementation
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.Matcher

import java.nio.file.Path

abstract class ExecutionEngineFunSuite
    extends CypherFunSuite
    with GraphDatabaseTestSupport
    with ExecutionEngineTestSupport
    with QueryPlanTestSupport {

  def containNotifications(notifications: NotificationImplementation*): Matcher[RewindableExecutionResult] = {
    contain.allElementsOf(notifications).matcher[Iterable[NotificationImplementation]].compose(
      (r: RewindableExecutionResult) => r.notifications
    )
      .and(contain.allElementsOf(notifications).matcher[Iterable[GqlStatusObject]].compose(
        (r: RewindableExecutionResult) => r.gqlStatusObjects
      ))
  }
}

abstract class ExecutionEngineWithoutRestartFunSuite
    extends ExecutionEngineFunSuite
    with BeforeAndAfterAll {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    startGraphDatabase()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    if (managementService != null) {
      managementService.shutdown()
    }
  }

  override protected def beforeEach(): Unit = {
    // not calling super.beforeEach() to avoid restarting the database
  }

  override protected def afterEach(): Unit = {
    resetGraphDatabase()
    eengine.clearQueryCaches()
  }

  final override protected def restartWithConfig(
    config: Map[Setting[_], Object],
    maybeExternalPath: Option[Path]
  ): Unit = {
    fail {
      s"""restartWithConfig is not safe to use with ${classOf[
          ExecutionEngineWithoutRestartFunSuite
        ].getSimpleName} tests.
         |Use restartWithConfigScoped instead, or create a new test suite and override databaseConfig.""".stripMargin
    }
  }

  /**
   * Restarts the database with `config`.
   * After `runTest` is finished, restarts the database for the next test using the default config.
   */
  protected def restartWithConfigScoped(config: Map[Setting[_], Object])(runTest: => Unit): Unit = {
    try {
      super.restartWithConfig(config)
      runTest
    } finally {
      super.restartWithConfig()
    }
  }

  private def resetGraphDatabase(): Unit = {
    if (tx != null) {
      tx.close()
      tx = null
    }

    // Don't clear any "real" databases!
    if (externalDatabase.nonEmpty)
      return

    clearDatabase()
    clearProcedures()
  }

  private def clearDatabase(): Unit = {
    // always re-creating lookup indexes turned out to be expensive
    var nodeLookupIsMissing = true
    var relLookupIsMissing = true
    withTx { tx =>
      tx.schema().getConstraints().forEach(_.drop())
      tx.schema().getIndexes().forEach { i =>
        if (i.getIndexType != IndexType.LOOKUP) {
          i.drop()
        } else if (i.isNodeIndex) {
          nodeLookupIsMissing = false
        } else {
          relLookupIsMissing = false
        }
      }
    }
    deleteAllEntities()
    if (nodeLookupIsMissing) {
      graph.createLookupIndex(isNodeIndex = true)
    }
    if (relLookupIsMissing) {
      graph.createLookupIndex(isNodeIndex = false)
    }
  }

  private def clearProcedures(): Unit = {
    val procs = globalProcedures
    registeredCallables.foreach(procs.unregister)
    registeredCallables.clear()
  }
}
