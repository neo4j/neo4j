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
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{ExecutionPlan => ExecutionPlan_v1_9}
import org.neo4j.cypher.internal.compiler.v1_9.{CypherCompiler => CypherCompiler1_9}
import org.neo4j.cypher.internal.spi.v1_9.{GDSBackedQueryContext => QueryContext_v1_9}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.Statement

case class CompatibilityFor1_9(graph: GraphDatabaseService, queryCacheSize: Int) {
  private val queryCache1_9 = new LRUCache[String, Object](queryCacheSize)
  private val compiler1_9   = new CypherCompiler1_9(graph, (q, f) => queryCache1_9.getOrElseUpdate(q, f))

  def parseQuery(statementAsText: String, profiled: Boolean) = new ParsedQuery {
    def plan(statement: Statement): (ExecutionPlan, Map[String, Any]) = {
      val planImpl = compiler1_9.prepare(statementAsText)
      (new ExecutionPlanWrapper(planImpl, profiled), Map.empty)
    }

    def isPeriodicCommit = false
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v1_9, profile: Boolean) extends ExecutionPlan {

    private def queryContext(graph: GraphDatabaseAPI) =
      new QueryContext_v1_9(graph)

    def profile(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
      LegacyExecutionResultWrapper(inner.profile(queryContext(graph), txInfo.tx, params), planDescriptionRequested = true, CypherVersion.v1_9)

    def execute(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) = if (profile)
      profile(graph, txInfo, params)
    else
      LegacyExecutionResultWrapper(inner.execute(queryContext(graph), txInfo.tx, params), planDescriptionRequested = false, CypherVersion.v1_9)

    def isPeriodicCommit = false
  }
}
