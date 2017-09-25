/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.internal.{ExecutionEngine}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}
import org.neo4j.test.TestGraphDatabaseFactory
import scala.collection.JavaConverters._

import scala.collection.immutable.Map
import scala.language.implicitConversions

class QueryExecutionMonitorTest extends CypherFunSuite with GraphIcing with GraphDatabaseTestSupport with ExecutionEngineTestSupport {
  implicit def contextQuery(context: TransactionalContext): ExecutingQuery = context.executingQuery()

  private def runQuery(query: String): (ExecutingQuery, Result) = {
    val context = db.transactionalContext(query = query -> Map.empty)
    val executingQuery = context.executingQuery()
    val executionResult = engine.execute(executingQuery.queryText(), executingQuery.queryParameters(), context)
    (executingQuery, executionResult)
  }

  test("monitor is not called if iterator not exhausted") {
    // when
    val (query, result) = runQuery("RETURN 42")

    // then
    verify(monitor, times(1)).startQueryExecution(query)
    verify(monitor, never()).endSuccess(query)
  }

  test("monitor is called when exhausted") {
    // when
    val (query, result) = runQuery("RETURN 42")

    while (result.hasNext) {
      verify(monitor, never).endSuccess(query)
      result.next()
    }

    // then
    verify(monitor, times(1)).startQueryExecution(query)
    verify(monitor, times(1)).endSuccess(query)
  }

  test("monitor is called when using dumpToString") {
    // when
    val (query, result) = runQuery("RETURN 42")


    val textResult = result.resultAsString()

    // then
    verify(monitor, times(1)).startQueryExecution(query)
    verify(monitor, times(1)).endSuccess(query)
  }

  test("monitor is called when using columnAs[]") {
    // when
    val (query, result) = runQuery("RETURN 42 as x")


    result.columnAs[Number]("x").asScala.toSeq

    // then
    verify(monitor, times(1)).startQueryExecution(query)
    verify(monitor, times(1)).endSuccess(query)
  }

  test("monitor is called when using columnAs[] from Java and explicitly closing") {
    // when
    val (query, result) = runQuery("RETURN 42 as x")


    val res = result.columnAs[Number]("x")
    res.close()

    // then
    verify(monitor, times(1)).startQueryExecution(query)
    verify(monitor, times(1)).endSuccess(query)
  }

  test("monitor is called when using columnAs[] from Java and emptying") {
    // when
    val (query, result) = runQuery("RETURN 42 as x")


    val res = result.columnAs[Number]("x")
    while(res.hasNext) res.next()

    // then
    verify(monitor, times(1)).startQueryExecution(query)
    verify(monitor, times(1)).endSuccess(query)
  }

  test("monitor is called directly when return is empty") {
    val (context, result) = runQuery("CREATE ()")

    // then
    verify(monitor, times(1)).startQueryExecution(context)
    verify(monitor, times(1)).endSuccess(context)
  }

  test("monitor is not called multiple times even if result is closed multiple times") {
    val (context, result) = runQuery("CREATE ()")
    result.close()
    result.close()

    // then
    verify(monitor, times(1)).startQueryExecution(context)
    verify(monitor, times(1)).endSuccess(context)
  }

  test("monitor is called directly when proc return is void") {
    db.execute("CREATE INDEX ON :Person(name)").close()

    val (context, result) = runQuery("CALL db.awaitIndex(':Person(name)')")

    // then
    verify(monitor, times(1)).startQueryExecution(context)
    verify(monitor, times(1)).endSuccess(context)
  }

  test("monitor is called when iterator closes") {
   // given
   val (context, result) = runQuery("RETURN 42")

    // when
    result.close()

    // then
    verify(monitor, times(1)).startQueryExecution(context)
    verify(monitor, times(1)).endSuccess(context)
  }

  test("monitor is not called when next on empty iterator") {
    // given
    val (context, result) = runQuery("RETURN 42")

    // when
    result.next()

    intercept[Throwable](result.next())

    // then, since the result was successfully emptied
    verify(monitor, times(1)).startQueryExecution(context)
    verify(monitor, times(1)).endSuccess(context)
    verify(monitor, never()).endFailure(any(classOf[ExecutingQuery]), any(classOf[Throwable]))
  }

  test("check so that profile triggers monitor") {
    // when
    val (context, result) = runQuery("RETURN [1, 2, 3, 4, 5]")

    //then
    verify(monitor, times(1)).startQueryExecution(context)
    while (result.hasNext) {
      verify(monitor, never).endSuccess(context)
      result.next()
    }
    verify(monitor, times(1)).endSuccess(context)
  }

  test("check that monitoring is correctly done when using visitor") {
    // when
    val (context, result) = runQuery("RETURN [1, 2, 3, 4, 5]")

    //then
    result.accept(new ResultVisitor[Exception] {
      override def visit(row: ResultRow): Boolean = true
    })

    verify(monitor, times(1)).startQueryExecution(context)
    verify(monitor, times(1)).endSuccess(context)
  }

  test("triggering monitor in 2.3") {
    // given
    val (query, result) = runQuery("CYPHER 2.3 RETURN [1, 2, 3, 4, 5]")


    //then
    verify(monitor, times(1)).startQueryExecution(query)
    while (result.hasNext) {
      verify(monitor, never).endSuccess(query)
      result.next()
    }
    verify(monitor, times(1)).endSuccess(query)
  }

  test("monitor is called when iterator closes in 2.3") {
    // given
    val (query, result) = runQuery("CYPHER 2.3 RETURN 42")

    // when
    result.close()

    // then
    verify(monitor, times(1)).startQueryExecution(query)
    verify(monitor, times(1)).endSuccess(query)
  }

  test("monitor is called when next on empty iterator in 2.3") {
    // given
    val (query, result) = runQuery("CYPHER 2.3 RETURN 42")

    // when
    val iterator = result
    iterator.next()
    intercept[NoSuchElementException] { iterator.next() }

    // then
    verify(monitor, times(1)).startQueryExecution(query)
    verify(monitor, times(1)).endSuccess(query)
  }

  test("monitor is called directly when return is empty in 2.3") {
    // given
    val context = db.transactionalContext(query = "CYPHER 2.3 CREATE()" -> Map.empty)

    // when
    val result = engine.execute(context.queryText(), context.queryParameters(), context)

    // then
    verify(monitor, times(1)).startQueryExecution(context)
    verify(monitor, times(1)).endSuccess(context)
  }

  test("triggering monitor in 3.1") {
    // given
    val context = db.transactionalContext(query = "CYPHER 3.1 RETURN [1, 2, 3, 4, 5]" -> Map.empty)

    // when
    val result = engine.profile(context.queryText(), context.queryParameters(), context)

    //then
    verify(monitor, times(1)).startQueryExecution(context)
    while (result.hasNext) {
      verify(monitor, never).endSuccess(context)
      result.next()
    }
    verify(monitor, times(1)).endSuccess(context)
  }

  test("monitor is called when iterator closes in 3.1") {
    // given
    val (query, result) = runQuery("CYPHER 3.1 RETURN 42")

    // when
    val iterator = result
    iterator.close()

    // then
    verify(monitor, times(1)).startQueryExecution(query)
    verify(monitor, times(1)).endSuccess(query)
  }

  test("monitor is called when next on empty iterator in 3.1") {
    // given
    val (query, result) = runQuery("CYPHER 3.1 RETURN 42")

    // when
    val iterator = result
    iterator.next()
    intercept[NoSuchElementException] { iterator.next() }

    // then
    verify(monitor, times(1)).startQueryExecution(query)
    verify(monitor, times(1)).endSuccess(query)
  }

  test("monitor is called directly when return is empty in 3.1") {
    // given
    val (query, result) = runQuery("CYPHER 3.1 CREATE()")

    // then
    verify(monitor, times(1)).startQueryExecution(query)
    verify(monitor, times(1)).endSuccess(query)
  }

  var db: GraphDatabaseQueryService = null
  var monitor: QueryExecutionMonitor = null
  var engine: ExecutionEngine = null

  override protected def beforeEach(): Unit = {
    db = new GraphDatabaseCypherService(new TestGraphDatabaseFactory().newImpermanentDatabase())
    monitor = mock[QueryExecutionMonitor]
    val monitors = db.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.monitoring.Monitors])
    monitors.addMonitorListener(monitor)
    engine = createEngine(db)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    if (db != null) {
      db.shutdown()
    }
  }
}
