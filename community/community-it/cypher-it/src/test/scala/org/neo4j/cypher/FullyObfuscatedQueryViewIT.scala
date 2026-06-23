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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.cypher.internal.javacompat.ResultSubscriber
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

import java.util.concurrent.atomic.AtomicReference

abstract class FullyObfuscatedQueryViewITBase extends CypherITTestSuite with GraphIcing
    with GraphDatabaseTestSupport with ExecutionEngineTestSupport {

  protected def runQuery(query: String): ExecutingQuery = {
    graph.withTx(tx => {
      val context = graph.transactionalContext(tx, query = query -> Map.empty)
      val result = new ResultSubscriber(context)
      val executionResult =
        eengine.execute(query, MapValue.EMPTY, context, profile = false, prePopulate = false, result)
      result.init(executionResult)
      try {
        while (result.hasNext) result.next()
      } finally {
        result.close()
      }
      context.executingQuery()
    })
  }
}

class FullyObfuscatedQueryViewIT extends FullyObfuscatedQueryViewITBase {

  test("at default config, the all-literals view redacts ordinary literals while the default view keeps them") {
    val query = runQuery("RETURN 'ordinary' AS x")

    val fullyObfuscatedText = query.fullyObfuscatedQueryText()
    fullyObfuscatedText should include("******")
    fullyObfuscatedText should not include "'ordinary'"
    query.obfuscatedQueryText().get() should include("'ordinary'")
  }
}

class FullyObfuscatedQueryViewFailSafeIT extends FullyObfuscatedQueryViewITBase {

  override def databaseConfig(): Map[Setting[?], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseInternalSettings.expose_fully_obfuscated_query_view -> java.lang.Boolean.FALSE
  )

  test("with the fail-safe off, the all-literals view is absent while the default view is unaffected") {
    val query = runQuery("RETURN 'ordinary' AS x")

    query.fullyObfuscatedQueryText() shouldBe ""
    query.obfuscatedQueryText().get() should include("'ordinary'")
  }

  test("with the fail-safe off, system database commands expose no all-literals view either") {
    // Admin commands get their obfuscator through QueryStatementLifecycles (the fabric stack) rather than
    // CypherPlanner, so this covers the other obfuscator-construction path. The command carries literals,
    // so this pins the real-obfuscator branch, not just the empty-metadata passthrough edge.
    val systemDb = managementService.database(SYSTEM_DATABASE_NAME).asInstanceOf[GraphDatabaseAPI]
    val monitors = systemDb.getDependencyResolver.resolveDependency(classOf[Monitors])
    val captured = new AtomicReference[String]
    val listener = new QueryExecutionMonitor {
      override def startProcessing(query: ExecutingQuery): Unit = {}
      override def startExecution(query: ExecutingQuery): Unit = {}
      override def endFailure(query: ExecutingQuery, failure: Throwable): Unit = {}

      override def endFailure(
        query: ExecutingQuery,
        reason: String,
        status: Status,
        errorGqlStatusObject: ErrorGqlStatusObject
      ): Unit = {}

      override def endSuccess(query: ExecutingQuery): Unit =
        captured.set(query.fullyObfuscatedQueryText())
    }
    monitors.addMonitorListener(listener)
    try {
      systemDb.executeTransactionally("CREATE USER probe SET PASSWORD 'probe-password-123'")

      captured.get() shouldBe ""
    } finally {
      monitors.removeMonitorListener(listener)
    }
  }
}
