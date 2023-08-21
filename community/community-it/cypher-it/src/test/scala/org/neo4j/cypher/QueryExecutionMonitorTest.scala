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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.javacompat.ResultSubscriber
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.Result.ResultRow
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.monitoring.Monitors
import org.neo4j.test.TestDatabaseManagementServiceBuilder
import org.neo4j.values.virtual.MapValue

import scala.collection.immutable.Map
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.language.implicitConversions

class QueryExecutionMonitorTest extends CypherFunSuite with GraphIcing with GraphDatabaseTestSupport
    with ExecutionEngineTestSupport {
  implicit def contextQuery(context: TransactionalContext): ExecutingQuery = context.executingQuery()

  val defaultFunction: ResultSubscriber => Unit = { _: ResultSubscriber => }

  private def runQuery(query: String, f: ResultSubscriber => Unit = defaultFunction): ExecutingQuery = {
    db.withTx(tx => {
      val context = db.transactionalContext(tx, query = query -> Map.empty)
      val result = new ResultSubscriber(context)
      val executionResult = engine.execute(query, MapValue.EMPTY, context, profile = false, prePopulate = false, result)
      result.init(executionResult)
      try {
        f(result)
      } finally {
        result.close()
      }
      context.executingQuery()
    })
  }

  test("monitor is not called if iterator not exhausted") {
    // when
    runQuery(
      "RETURN 42",
      _ => {
        verify(monitor, never()).endSuccess(any())
      }
    )
  }

  test("monitor is called when exhausted") {
    // when
    def iterate(result: ResultSubscriber): Unit = {
      while (result.hasNext) {
        result.next()
      }
    }
    val query = runQuery("RETURN 42", iterate)

    // then
    verify(monitor).endSuccess(query)
  }

  test("monitor is called when using dumpToString") {
    // when
    val query = runQuery("RETURN 42", r => r.resultAsString())

    // then
    verify(monitor).endSuccess(query)
  }

  test("monitor is called when using columnAs[]") {
    // when
    val query = runQuery("RETURN 42 as x", r => r.columnAs[Number]("x").asScala.toSeq)

    // then
    verify(monitor).endSuccess(query)
  }

  test("monitor is called when using columnAs[] from Java and explicitly closing") {
    // when
    val query = runQuery(
      "RETURN 42 as x",
      r => {
        r.columnAs[Number]("x")
        r.close()
      }
    )

    // then
    verify(monitor).endSuccess(query)
  }

  test("monitor is called when using columnAs[] from Java and emptying") {
    // when
    val query = runQuery(
      "RETURN 42 as x",
      r => {
        val res = r.columnAs[Number]("x")
        while (res.hasNext) res.next()
      }
    )

    // then
    verify(monitor).endSuccess(query)
  }

  test("monitor is called directly when return is empty") {
    val context = runQuery("CREATE ()")

    // then
    verify(monitor).endSuccess(context)
  }

  test("monitor is not called multiple times even if result is closed multiple times") {
    val context = runQuery(
      "CREATE ()",
      r => {
        r.close()
        r.close()
      }
    )

    // then
    verify(monitor).endSuccess(context)
  }

  test("monitor is called directly when proc return is void") {
    db.withTx(tx => tx.execute("CREATE INDEX `MyIndex` FOR (n:Person) ON (n.name)").close())

    val context = runQuery("CALL db.awaitIndex('MyIndex')")

    // then
    verify(monitor).endSuccess(context)
  }

  test("monitor is called when iterator closes") {
    // given
    val context = runQuery("RETURN 42", r => r.close())

    // then
    verify(monitor).endSuccess(context)
  }

  test("monitor is not called when next on empty iterator") {
    // given
    val context = runQuery(
      "RETURN 42",
      r => {
        // when
        r.next()
        intercept[Throwable](r.next())
      }
    )

    // then, since the result was successfully emptied
    verify(monitor).endSuccess(context)
    verify(monitor, never()).endFailure(any(classOf[ExecutingQuery]), any(classOf[Throwable]))
  }

  test("check so that profile triggers monitor") {
    // when
    val context = runQuery(
      "RETURN [1, 2, 3, 4, 5]",
      r => {
        // then
        while (r.hasNext) {
          r.next()
        }
      }
    )

    verify(monitor).endSuccess(context)
  }

  test("check that monitoring is correctly done when using visitor") {
    // when
    val context = runQuery(
      "RETURN [1, 2, 3, 4, 5]",
      r => {
        // then
        r.accept((_: ResultRow) => true)
      }
    )

    verify(monitor).endSuccess(context)
  }

  var db: GraphDatabaseQueryService = _
  var monitor: QueryExecutionMonitor = _
  var engine: ExecutionEngine = _

  override protected def beforeEach(): Unit = {
    db = new GraphDatabaseCypherService(
      new TestDatabaseManagementServiceBuilder().impermanent().build().database(DEFAULT_DATABASE_NAME)
    )
    monitor = mock[QueryExecutionMonitor]
    val monitors = db.getDependencyResolver.resolveDependency(classOf[Monitors])
    monitors.addMonitorListener(monitor)
    engine = createEngine(db)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    if (managementService != null) {
      managementService.shutdown()
    }
  }
}
