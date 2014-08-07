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
import org.neo4j.cypher.internal.compiler.v2_0.CypherCompiler
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{ExecutionPlan => ExecutionPlan_v2_0}
import org.neo4j.cypher.internal.compiler.v2_0.spi.{ExceptionTranslatingQueryContext => ExceptionTranslatingQueryContext_v2_0}
import org.neo4j.cypher.internal.spi.v2_0.{TransactionBoundPlanContext => PlanContext_v2_0, TransactionBoundQueryContext => QueryContext_v2_0}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.Statement

case class CompatibilityFor2_0(graph: GraphDatabaseService, queryCacheSize: Int) {
  private val queryCache = new LRUCache[Object, Object](queryCacheSize)
  private val compiler   = new CypherCompiler(graph, (q, f) => queryCache.getOrElseUpdate(q, f))

  def parseQuery(statementAsText: String, profiled: Boolean) = new ParsedQuery {
    override def plan(statement: Statement): (ExecutionPlan, Map[String, Any]) = {
      val planImpl = compiler.prepare(statementAsText, new PlanContext_v2_0(statement, graph))
      (new ExecutionPlanWrapper(planImpl, profiled), Map.empty)
    }

    def isPeriodicCommit = false
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v2_0, profiled: Boolean) extends ExecutionPlan {

    private def queryContext(graph: GraphDatabaseAPI, txInfo: TransactionInfo) = {
      val ctx = new QueryContext_v2_0(graph, txInfo.tx, txInfo.statement)
      new ExceptionTranslatingQueryContext_v2_0(ctx)
    }

    def profile(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
      LegacyExecutionResultWrapper(inner.profile(queryContext(graph, txInfo), params), planDescriptionRequested = true)

    def execute(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) = if (profiled)
      profile(graph, txInfo, params)
    else
      LegacyExecutionResultWrapper(inner.execute(queryContext(graph, txInfo), params), planDescriptionRequested = false)

    def isPeriodicCommit = false
  }
}
