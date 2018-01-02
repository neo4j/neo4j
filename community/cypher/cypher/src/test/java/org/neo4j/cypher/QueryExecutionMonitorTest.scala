/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.util.Collections

import org.mockito.Mockito._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.impl.query.{QueryEngineProvider, QueryExecutionMonitor}
import org.neo4j.kernel.monitoring.Monitors
import org.neo4j.test.TestGraphDatabaseFactory

class QueryExecutionMonitorTest extends CypherFunSuite {

  test("monitor is not called if iterator not exhausted") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    engine.execute("RETURN 42", Map.empty[String, Any], session)

    // then
    verify(monitor, times(1)).startQueryExecution(session, "RETURN 42", Collections.emptyMap())
    verify(monitor, never()).endSuccess(session)
  }

  test("monitor is called when exhausted") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val result = engine.execute("RETURN 42", Map.empty[String, Any], session).javaIterator
    while (result.hasNext) {
      verify(monitor, never).endSuccess(session)
      result.next()
    }

    // then
    verify(monitor, times(1)).startQueryExecution(session, "RETURN 42", Collections.emptyMap())
    verify(monitor, times(1)).endSuccess(session)
  }

  test("monitor is called directly when return is empty") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val result = engine.execute("CREATE()", Map.empty[String, Any], session).javaIterator

    // then
    verify(monitor, times(1)).startQueryExecution(session, "CREATE()", Collections.emptyMap())
    verify(monitor, times(1)).endSuccess(session)
  }

  test("monitor really not called until result is exhausted") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val result = engine.execute("RETURN [1, 2, 3, 4, 5]", Map.empty[String, Any], session).javaIterator

    //then
    verify(monitor, times(1)).startQueryExecution(session, "RETURN [1, 2, 3, 4, 5]", Collections.emptyMap())
    while (result.hasNext) {
      verify(monitor, never).endSuccess(session)
      result.next()
    }
    verify(monitor, times(1)).endSuccess(session)
  }

  test("nothing breaks when no monitor is there") {
    // given
    val engine = new ExecutionEngine(graph)
    val session = QueryEngineProvider.embeddedSession()

    // when
    engine.execute("RETURN 42", Map.empty[String, Any], session).toList
  }

  test("monitor is called when iterator closes") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val result = engine.execute("RETURN 42", Map.empty[String, Any], session).javaIterator.close()

    // then
    verify(monitor, times(1)).startQueryExecution(session, "RETURN 42", Collections.emptyMap())
    verify(monitor, times(1)).endSuccess(session)
  }

  test("monitor is called when next on empty iterator") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val iterator = engine.execute("RETURN 42", Map.empty[String, Any], session).javaIterator
    iterator.next()
    var throwable: Throwable = null
    try {
      iterator.next()
      fail("we expect an exception here")
    }
    catch {
      case e: Throwable => throwable = e
    }

    // then
    verify(monitor, times(1)).startQueryExecution(session, "RETURN 42", Collections.emptyMap())
    verify(monitor, times(1)).endFailure(session, throwable)
  }

  test("check so that profile triggers monitor") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val result = engine.profile("RETURN [1, 2, 3, 4, 5]", Map.empty[String, Any], session).javaIterator

    //then
    verify(monitor, times(1)).startQueryExecution(session, "RETURN [1, 2, 3, 4, 5]", Collections.emptyMap())
    while (result.hasNext) {
      verify(monitor, never).endSuccess(session)
      result.next()
    }
    verify(monitor, times(1)).endSuccess(session)
  }

  test("triggering monitor in 2.2") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val result = engine.profile("CYPHER 2.2 RETURN [1, 2, 3, 4, 5]", Map.empty[String, Any], session).javaIterator

    //then
    verify(monitor, times(1)).startQueryExecution(session, "CYPHER 2.2 RETURN [1, 2, 3, 4, 5]", Collections.emptyMap())
    while (result.hasNext) {
      verify(monitor, never).endSuccess(session)
      result.next()
    }
    verify(monitor, times(1)).endSuccess(session)
  }

  test("monitor is called when iterator closes in 2.2") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val result = engine.execute("CYPHER 2.2 RETURN 42", Map.empty[String, Any], session).javaIterator.close()

    // then
    verify(monitor, times(1)).startQueryExecution(session, "CYPHER 2.2 RETURN 42", Collections.emptyMap())
    verify(monitor, times(1)).endSuccess(session)
  }

  test("monitor is called when next on empty iterator in 2.2") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val iterator = engine.execute("CYPHER 2.2 RETURN 42", Map.empty[String, Any], session).javaIterator
    iterator.next()
    var throwable: Throwable = null
    try {
      iterator.next()
      fail("we expect an exception here")
    }
    catch {
      case e: Throwable => throwable = e
    }

    // then
    verify(monitor, times(1)).startQueryExecution(session, "CYPHER 2.2 RETURN 42", Collections.emptyMap())
    verify(monitor, times(1)).endFailure(session, throwable)
  }

  test("monitor is called directly when return is empty in 2.2") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val result = engine.execute("CYPHER 2.2 CREATE()", Map.empty[String, Any], session).javaIterator

    // then
    verify(monitor, times(1)).startQueryExecution(session, "CYPHER 2.2 CREATE()", Collections.emptyMap())
    verify(monitor, times(1)).endSuccess(session)
  }

  test("triggering monitor in 1.9") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val result = engine.profile("CYPHER 1.9 CREATE() RETURN [1, 2, 3, 4, 5]", Map.empty[String, Any], session).javaIterator

    //then
    verify(monitor, times(1)).startQueryExecution(session, "CYPHER 1.9 CREATE() RETURN [1, 2, 3, 4, 5]", Collections.emptyMap())
    while (result.hasNext) {
      verify(monitor, never).endSuccess(session)
      result.next()
    }
    verify(monitor, times(1)).endSuccess(session)
  }

  test("monitor is called when iterator closes in 1.9") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val result = engine.execute("CYPHER 1.9 CREATE() RETURN 42", Map.empty[String, Any], session).javaIterator.close()

    // then
    verify(monitor, times(1)).startQueryExecution(session, "CYPHER 1.9 CREATE() RETURN 42", Collections.emptyMap())
    verify(monitor, times(1)).endSuccess(session)
  }

  test("monitor is called when next on empty iterator in 1.9") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val iterator = engine.execute("CYPHER 1.9 CREATE() RETURN 42", Map.empty[String, Any], session).javaIterator
    iterator.next()
    var throwable: Throwable = null
    try {
      iterator.next()
      fail("we expect an exception here")
    }
    catch {
      case e: Throwable => throwable = e
    }

    // then
    verify(monitor, times(1)).startQueryExecution(session, "CYPHER 1.9 CREATE() RETURN 42", Collections.emptyMap())
    verify(monitor, times(1)).endFailure(session, throwable)
  }

  test("monitor is called directly when return is empty in 1.9 ") {
    // given
    val session = QueryEngineProvider.embeddedSession()

    // when
    val result = engine.execute("CYPHER 1.9 CREATE()", Map.empty[String, Any], session).javaIterator

    // then
    verify(monitor, times(1)).startQueryExecution(session, "CYPHER 1.9 CREATE()", Collections.emptyMap())
    verify(monitor, times(1)).endSuccess(session)
  }

  var graph: GraphDatabaseService = null
  var monitor: QueryExecutionMonitor = null
  var engine: ExecutionEngine = null

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    graph = new TestGraphDatabaseFactory().newImpermanentDatabase()
    monitor = mock[QueryExecutionMonitor]
    monitors(graph).addMonitorListener(monitor)
    engine = new ExecutionEngine(graph)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    if (graph != null) graph.shutdown()
  }

  private def monitors(graph: GraphDatabaseService): Monitors = {
    val graphAPI = graph.asInstanceOf[GraphDatabaseAPI]
    graphAPI.getDependencyResolver.resolveDependency(classOf[org.neo4j.kernel.monitoring.Monitors])
  }
}
