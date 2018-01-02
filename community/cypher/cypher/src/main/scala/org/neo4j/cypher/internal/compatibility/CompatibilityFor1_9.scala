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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{ExecutionPlan => ExecutionPlan_v1_9}
import org.neo4j.cypher.internal.compiler.v1_9.{CypherCompiler => CypherCompiler1_9}
import org.neo4j.cypher.internal.compiler.v2_3
import org.neo4j.cypher.internal.compiler.v2_3.CompilationPhaseTracer
import org.neo4j.cypher.internal.spi.v1_9.{GDSBackedQueryContext => QueryContext_v1_9}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, QuerySession}
import org.neo4j.kernel.monitoring.Monitors

case class CompatibilityFor1_9(graph: GraphDatabaseService, queryCacheSize: Int,  kernelMonitors: Monitors) {
  private val queryCache1_9 = new v2_3.LRUCache[String, Object](queryCacheSize)
  private val compiler1_9   = new CypherCompiler1_9(graph, (q, f) => queryCache1_9.getOrElseUpdate(q, f))
  implicit val executionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  def parseQuery(statementAsText: String) = new ParsedQuery {
    def plan(statement: Statement, tracer: CompilationPhaseTracer): (ExecutionPlan, Map[String, Any]) = {
      val planImpl = compiler1_9.prepare(statementAsText)
      (new ExecutionPlanWrapper(planImpl), Map.empty)
    }

    def isPeriodicCommit = false

    //we lack the knowledge whether or not this query is correct
    def hasErrors = false
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v1_9) extends ExecutionPlan {

    private def queryContext(graph: GraphDatabaseAPI) =
      new QueryContext_v1_9(graph)

    def run(graph: GraphDatabaseAPI, txInfo: TransactionInfo, executionMode: CypherExecutionMode, params: Map[String, Any], session: QuerySession) = executionMode match {
      case CypherExecutionMode.normal   => execute(graph, txInfo, params, session)
      case CypherExecutionMode.profile  => profile(graph, txInfo, params, session)
      case _  => throw new UnsupportedOperationException(s"${CypherVersion.v1_9.name}: $executionMode is unsupported")
    }

    private def execute(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any], session: QuerySession) = {
      implicit val qs = session
      LegacyExecutionResultWrapper(inner.execute(queryContext(graph), txInfo.tx, params), planDescriptionRequested = false, CypherVersion.v1_9)
    }

    private def profile(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any], session: QuerySession) = {
      implicit val qs = session
      LegacyExecutionResultWrapper(inner.profile(queryContext(graph), txInfo.tx, params), planDescriptionRequested = true, CypherVersion.v1_9)
    }

    def isPeriodicCommit = false

    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, statement: Statement): Boolean = false

    def notifications = Iterable.empty
  }
}
