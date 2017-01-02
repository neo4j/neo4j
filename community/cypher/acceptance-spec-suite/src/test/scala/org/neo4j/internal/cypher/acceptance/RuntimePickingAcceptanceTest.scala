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
package org.neo4j.internal.cypher.acceptance

import java.util

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.frontend.v3_2.notification.RuntimeUnsupportedNotification
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.test.TestGraphDatabaseFactory

import scala.collection.JavaConverters._

class RuntimePickingAcceptanceTest extends ExecutionEngineFunSuite {

  private val QUERY_NOT_SUPPORTED_BY_CRT = "cypher runtime=compiled explain match (n) optional match (b) return n"

  test("default should be compiled runtime") {
    val result: Result = graph.execute("match (n) return n")
    empty(result)
    result.getExecutionPlanDescription.getArguments.get("runtime-impl") should equal("COMPILED")

    result.close()
  }

  test("no runtime has been requested, and the query is not handled by the compiled RT, no warnings or failures should occur") {
    val result: Result = graph.execute(QUERY_NOT_SUPPORTED_BY_CRT)
    empty(result)
    result.getExecutionPlanDescription.getArguments.get("runtime-impl") should equal("INTERPRETED")

    result.close()
  }

  test("when explicitly asking for compiled, compiled should be used") {
    val result = graph.execute("cypher runtime=compiled match (n) return n")
    empty(result)
    result.getExecutionPlanDescription.getArguments.get("runtime-impl") should equal("COMPILED")

    result.close()
  }

  test("when explicitly asking for interpreted, interpreted should be used") {
    val result = graph.execute("cypher runtime=interpreted match (n) return n")
    empty(result)
    result.getExecutionPlanDescription.getArguments.get("runtime-impl") should equal("INTERPRETED")

    result.close()
  }

  test("when explicitly asking for compiled, but it is still not supported, yield warning by default") {
    val result = execute(QUERY_NOT_SUPPORTED_BY_CRT)
    result.notifications.toList should equal(List(RuntimeUnsupportedNotification))
    result.close()
  }

  test("when explicitly asking for compiled, but it is still not supported, throw exception if configure to use errors") {
    graph.shutdown()
    val config: util.Map[Setting[_], String] = Map[Setting[_], String](GraphDatabaseSettings.cypher_hints_error -> "true").asJava

    val db = new TestGraphDatabaseFactory().newImpermanentDatabase( config)
    graph = new GraphDatabaseCypherService(db)
    eengine = new ExecutionEngine(graph)
    intercept[org.neo4j.cypher.InvalidArgumentException](
      execute(QUERY_NOT_SUPPORTED_BY_CRT)
    )
  }

  private def empty(result: Result) = while (result.hasNext) result.next()
}
