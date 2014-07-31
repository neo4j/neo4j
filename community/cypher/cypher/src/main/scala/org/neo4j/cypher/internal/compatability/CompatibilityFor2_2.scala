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

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compiler.v2_2
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{ExecutionPlan => ExecutionPlan_v2_2}
import org.neo4j.cypher.internal.compiler.v2_2.spi.{ExceptionTranslatingQueryContext => ExceptionTranslatingQueryContext_v2_2}
import org.neo4j.cypher.internal.compiler.v2_2.CypherCompilerFactory
import org.neo4j.cypher.internal.spi.v2_2.{TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.{KernelAPI, Statement}
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

import scala.util.Try

abstract class CompatibilityFor2_2(graph: GraphDatabaseService, queryCacheSize: Int, kernelMonitors: KernelMonitors, kernelAPI: KernelAPI) {
  protected val compiler: v2_2.CypherCompiler

  def produceParsedQuery(statementAsText: String, planType: PlanType) = new ParsedQuery {
    val preparedQueryForV_2_2 = Try(compiler.prepareQuery(statementAsText, planType))

    def isPeriodicCommit = preparedQueryForV_2_2.map(_.isPeriodicCommit).getOrElse(false)

    def plan(statement: Statement): (ExecutionPlan, Map[String, Any]) = {
      val planContext = new TransactionBoundPlanContext(statement, kernelAPI, graph)
      val (planImpl, extractedParameters) = compiler.planPreparedQuery(preparedQueryForV_2_2.get, planContext)
      (new ExecutionPlanWrapper(planImpl), extractedParameters)
    }
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v2_2) extends ExecutionPlan {

    private def queryContext(graph: GraphDatabaseAPI, txInfo: TransactionInfo) = {
      val ctx = new TransactionBoundQueryContext(graph, txInfo.tx, txInfo.isTopLevelTx, txInfo.statement)
      new ExceptionTranslatingQueryContext_v2_2(ctx)
    }

    def profile(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
      inner.profile(queryContext(graph, txInfo), params)

    def execute(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
      inner.execute(queryContext(graph, txInfo), params)

    def isPeriodicCommit = inner.isPeriodicCommit
  }

}

case class CompatibilityFor2_2Experimental(graph: GraphDatabaseService,
                                           queryCacheSize: Int,
                                           kernelMonitors: KernelMonitors,
                                           kernelAPI: KernelAPI) extends CompatibilityFor2_2(graph, queryCacheSize, kernelMonitors, kernelAPI) {
  protected val compiler = CypherCompilerFactory.ronjaCompiler(graph, queryCacheSize, kernelMonitors)
}
case class CompatibilityFor2_2Legacy(graph: GraphDatabaseService,
                                           queryCacheSize: Int,
                                           kernelMonitors: KernelMonitors,
                                           kernelAPI: KernelAPI) extends CompatibilityFor2_2(graph, queryCacheSize, kernelMonitors, kernelAPI) {
  protected val compiler = CypherCompilerFactory.legacyCompiler(graph, queryCacheSize, kernelMonitors)
}
