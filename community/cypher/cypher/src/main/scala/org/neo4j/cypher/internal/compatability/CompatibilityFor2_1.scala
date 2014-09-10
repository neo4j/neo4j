/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatability

import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compiler.v2_1.CypherCompilerFactory
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.{ExecutionPlan => ExecutionPlan_v2_1}
import org.neo4j.cypher.internal.compiler.v2_1.spi.{ExceptionTranslatingQueryContext => ExceptionTranslatingQueryContext_v2_1}
import org.neo4j.cypher.internal.spi.v2_1.{TransactionBoundPlanContext => PlanContext_v2_1, TransactionBoundQueryContext => QueryContext_v2_1}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.{KernelAPI, Statement}
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

import scala.util.Try

case class CompatibilityFor2_1(graph: GraphDatabaseService, queryCacheSize: Int, kernelMonitors: KernelMonitors, kernelAPI: KernelAPI) {
  private val compiler = CypherCompilerFactory.legacyCompiler(graph, queryCacheSize, kernelMonitors)

  def parseQuery(statementAsText: String, profiled: Boolean) = new ParsedQuery {
    val preparedQueryForV_2_1 = Try(compiler.prepareQuery(statementAsText))

    override def plan(statement: Statement): (ExecutionPlan, Map[String, Any]) = {
      val planContext = new PlanContext_v2_1(statement, kernelAPI, graph)
      val (planImpl, extractedParameters) = compiler.planPreparedQuery(preparedQueryForV_2_1.get, planContext)
      (new ExecutionPlanWrapper(planImpl, profiled), extractedParameters)
    }

    def isPeriodicCommit = preparedQueryForV_2_1.map(_.isPeriodicCommit).getOrElse(false)
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v2_1, profile: Boolean) extends ExecutionPlan {

    private def queryContext(graph: GraphDatabaseAPI, txInfo: TransactionInfo) = {
      val ctx = new QueryContext_v2_1(graph, txInfo.tx, txInfo.isTopLevelTx, txInfo.statement)
      new ExceptionTranslatingQueryContext_v2_1(ctx)
    }

    def profile(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
      LegacyExecutionResultWrapper(inner.profile(queryContext(graph, txInfo), params), planDescriptionRequested = true, CypherVersion.v2_1)

    def execute(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) = if (profile)
      profile(graph, txInfo, params)
    else
      LegacyExecutionResultWrapper(inner.execute(queryContext(graph, txInfo), params), planDescriptionRequested = false, CypherVersion.v2_1)

    def isPeriodicCommit = inner.isPeriodicCommit
  }
}
