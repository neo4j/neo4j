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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compiler.v2_0.CypherCompiler
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{ExecutionPlan => ExecutionPlan_v2_0}
import org.neo4j.cypher.internal.compiler.v2_0.spi.{ExceptionTranslatingQueryContext => ExceptionTranslatingQueryContext_v2_0}
import org.neo4j.cypher.internal.compiler.v2_2.{ProfileMode, NormalMode, ExecutionMode}
import org.neo4j.cypher.internal.spi.v2_0.{TransactionBoundPlanContext => PlanContext_v2_0, TransactionBoundQueryContext => QueryContext_v2_0}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, QuerySession}
import org.neo4j.kernel.monitoring.Monitors

case class CompatibilityFor2_0(graph: GraphDatabaseService, queryCacheSize: Int, kernelMonitors: Monitors) {
  private val queryCache = new LRUCache[Object, Object](queryCacheSize)
  private val compiler   = new CypherCompiler(graph, (q, f) => queryCache.getOrElseUpdate(q, f))
  implicit val executionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  def parseQuery(statementAsText: String) = new ParsedQuery {
    override def plan(statement: Statement): (ExecutionPlan, Map[String, Any]) = {
      val planImpl = compiler.prepare(statementAsText, new PlanContext_v2_0(statement, graph))
      (new ExecutionPlanWrapper(planImpl), Map.empty)
    }

    def isPeriodicCommit = false

    //we lack the knowledge whether or not this query is correct
    def hasErrors = false
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v2_0) extends ExecutionPlan {

    private def queryContext(graph: GraphDatabaseAPI, txInfo: TransactionInfo) = {
      val ctx = new QueryContext_v2_0(graph, txInfo.tx, txInfo.statement)
      new ExceptionTranslatingQueryContext_v2_0(ctx)
    }

    def run(graph: GraphDatabaseAPI, txInfo: TransactionInfo, executionMode: ExecutionMode, params: Map[String, Any], session: QuerySession) = executionMode match {
      case NormalMode   => execute(graph, txInfo, params, session)
      case ProfileMode  => profile(graph, txInfo, params, session)
      case _            => throw new UnsupportedOperationException(s"${CypherVersion.v2_0.name}: $executionMode is unsupported")
    }

    private def execute(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any], session: QuerySession) = {
      implicit val qs = session
      LegacyExecutionResultWrapper(inner.execute(queryContext(graph, txInfo), params), planDescriptionRequested = false, CypherVersion.v2_0)
    }

    private def profile(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any], session: QuerySession) = {
      implicit val qs = session
      LegacyExecutionResultWrapper(inner.profile(queryContext(graph, txInfo), params), planDescriptionRequested = true, CypherVersion.v2_0)
    }

    def isPeriodicCommit = false

    def isStale(lastTxId: () => Long, statement: Statement): Boolean = false
  }
}
