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
package org.neo4j.cypher.internal

import org.neo4j.cypher._
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.{GraphDatabaseAPI, InternalAbstractGraphDatabase}
import org.neo4j.cypher.internal.compiler.v2_2.{CypherCompilerFactory => CypherCompilerFactory2_2}
import org.neo4j.cypher.internal.compiler.v2_1.{CypherCompilerFactory => CypherCompilerFactory2_1}
import org.neo4j.cypher.internal.compiler.v2_0.{CypherCompiler => CypherCompiler2_0}
import org.neo4j.cypher.internal.compiler.v1_9.{CypherCompiler => CypherCompiler1_9}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{ExecutionPlan => ExecutionPlan_v2_2}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.{ExecutionPlan => ExecutionPlan_v2_1}
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{ExecutionPlan => ExecutionPlan_v2_0}
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{ExecutionPlan => ExecutionPlan_v1_9}
import org.neo4j.cypher.internal.spi.v2_2.{TransactionBoundQueryContext => QueryContext_v2_2}
import org.neo4j.cypher.internal.spi.v2_1.{TransactionBoundQueryContext => QueryContext_v2_1}
import org.neo4j.cypher.internal.spi.v2_0.{TransactionBoundQueryContext => QueryContext_v2_0}
import org.neo4j.cypher.internal.spi.v1_9.{GDSBackedQueryContext => QueryContext_v1_9}
import org.neo4j.cypher.internal.spi.v2_2.{TransactionBoundPlanContext => PlanContext_v2_2}
import org.neo4j.cypher.internal.spi.v2_1.{TransactionBoundPlanContext => PlanContext_v2_1}
import org.neo4j.cypher.internal.spi.v2_0.{TransactionBoundPlanContext => PlanContext_v2_0}
import org.neo4j.cypher.internal.compiler.v2_2.spi.{ExceptionTranslatingQueryContext => ExceptionTranslatingQueryContext_v2_2}
import org.neo4j.cypher.internal.compiler.v2_1.spi.{ExceptionTranslatingQueryContext => ExceptionTranslatingQueryContext_v2_1}
import org.neo4j.cypher.internal.compiler.v2_0.spi.{ExceptionTranslatingQueryContext => ExceptionTranslatingQueryContext_v2_0}
import org.neo4j.kernel.api.{KernelAPI, Statement}
import org.neo4j.kernel.monitoring.{Monitors=>KernelMonitors}

import scala.util.Try

object CypherCompiler {
  val DEFAULT_QUERY_CACHE_SIZE: Int = 128

  private val hasVersionDefined = """(?si)^\s*cypher\s*([^\s]+)\s*(.*)""".r
}

class CypherCompiler(graph: GraphDatabaseService,
                     kernelAPI: KernelAPI,
                     kernelMonitors: KernelMonitors,
                     defaultVersion: CypherVersion = CypherVersion.vDefault) {

  private val queryCacheSize: Int = getQueryCacheSize

  private val queryCache2_0 = new LRUCache[Object, Object](queryCacheSize)
  private val queryCache1_9 = new LRUCache[String, Object](queryCacheSize)

  val ronjaCompiler2_2 = CypherCompilerFactory2_2.ronjaCompiler(graph, queryCacheSize, kernelMonitors)
  val legacyCompiler2_2 = CypherCompilerFactory2_2.legacyCompiler(graph, queryCacheSize, kernelMonitors)
  val legacyCompiler2_1 = CypherCompilerFactory2_1.legacyCompiler(graph, queryCacheSize, kernelMonitors)
  val compiler2_0 = new CypherCompiler2_0(graph, (q, f) => queryCache2_0.getOrElseUpdate(q, f))
  val compiler1_9 = new CypherCompiler1_9(graph, (q, f) => queryCache1_9.getOrElseUpdate(q, f))

  @throws(classOf[SyntaxException])
  def prepareQuery(queryText: String): PreparedQuery = {
    val (version, remainingQueryText) = versionedQuery(queryText)

    version match {
      case CypherVersion.experimental =>
        val preparedQueryForV_experimental = Try(ronjaCompiler2_2.prepareQuery(remainingQueryText))
        new PreparedQuery(queryText, version) {
          def isPeriodicCommit = preparedQueryForV_experimental.map(_.isPeriodicCommit).getOrElse(false)
          def plan(context: GraphDatabaseService, statement: Statement) = {
            val planContext = new PlanContext_v2_2(statement, kernelAPI, context)
            val (planImpl, extractedParameters) = ronjaCompiler2_2.planPreparedQuery(preparedQueryForV_experimental.get, planContext)
            (new ExecutionPlanWrapperForV2_2( planImpl ), extractedParameters)
          }
        }

      case CypherVersion.v2_2 =>
        new PreparedQuery(queryText, version) {
          val preparedQueryForV_2_2 = Try(legacyCompiler2_2.prepareQuery(remainingQueryText))
          def isPeriodicCommit = preparedQueryForV_2_2.map(_.isPeriodicCommit).getOrElse(false)

          def plan(context: GraphDatabaseService, statement: Statement): (ExecutionPlan, Map[String, Any]) = {
            val planContext = new PlanContext_v2_2(statement, kernelAPI, context)
            val (planImpl, extractedParameters) = legacyCompiler2_2.planPreparedQuery(preparedQueryForV_2_2.get, planContext)
            (new ExecutionPlanWrapperForV2_2( planImpl ), extractedParameters)
          }
        }

      case CypherVersion.v2_1 =>
        new PreparedQuery(queryText, version) {
          val preparedQueryForV_2_1 = Try(legacyCompiler2_1.prepareQuery(remainingQueryText))
          override def plan(context: GraphDatabaseService, statement: Statement): (ExecutionPlan, Map[String, Any]) = {
            val planContext = new PlanContext_v2_1(statement, kernelAPI, context)
            val (planImpl, extractedParameters) = legacyCompiler2_1.planPreparedQuery(preparedQueryForV_2_1.get, planContext)
            (new ExecutionPlanWrapperForV2_1( planImpl ), extractedParameters)
          }

          override def isPeriodicCommit: Boolean = preparedQueryForV_2_1.map(_.isPeriodicCommit).getOrElse(false)
        }

      case CypherVersion.v2_0 =>
        new PreparedQuery(queryText, version) {
          override def plan(context: GraphDatabaseService, statement: Statement): (ExecutionPlan, Map[String, Any]) = {
            val planImpl = compiler2_0.prepare(remainingQueryText, new PlanContext_v2_0(statement, context))
            (new ExecutionPlanWrapperForV2_0(planImpl), Map.empty)
          }

          override def isPeriodicCommit: Boolean = false
        }

      case CypherVersion.v1_9 =>
        new PreparedQuery(queryText, version) {
          override def plan(context: GraphDatabaseService, statement: Statement): (ExecutionPlan, Map[String, Any]) = {
            val planImpl = compiler1_9.prepare(remainingQueryText)
            (new ExecutionPlanWrapperForV1_9(planImpl), Map.empty)
          }

          override def isPeriodicCommit: Boolean = false
        }
    }
  }

  private def versionedQuery(query: String): (CypherVersion, String) = query match {
      case CypherCompiler.hasVersionDefined(versionName, tail) => (CypherVersion(versionName), tail)
      case _                                                   => (defaultVersion, query)
    }

  private def getQueryCacheSize : Int =
    optGraphAs[InternalAbstractGraphDatabase]
      .andThen(_.getConfig.get(GraphDatabaseSettings.query_cache_size))
      .andThen({
      case v: java.lang.Integer => v.intValue()
      case _                    => CypherCompiler.DEFAULT_QUERY_CACHE_SIZE
    })
      .applyOrElse(graph, (_: GraphDatabaseService) => CypherCompiler.DEFAULT_QUERY_CACHE_SIZE)

  private def optGraphAs[T <: GraphDatabaseService : Manifest]: PartialFunction[GraphDatabaseService, T] = {
    case (db: T) => db
  }
}

class ExecutionPlanWrapperForV2_2(inner: ExecutionPlan_v2_2) extends ExecutionPlan {

  private def queryContext(graph: GraphDatabaseAPI, txInfo: TransactionInfo) = {
    val ctx = new QueryContext_v2_2(graph, txInfo.tx, txInfo.isTopLevelTx, txInfo.statement)
    new ExceptionTranslatingQueryContext_v2_2(ctx)
  }

  def profile(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
    inner.profile(queryContext(graph, txInfo), params)

  def execute(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
    inner.execute(queryContext(graph, txInfo), params)

  def isPeriodicCommit: Boolean = inner.isPeriodicCommit
}

class ExecutionPlanWrapperForV2_1(inner: ExecutionPlan_v2_1) extends ExecutionPlan {

  private def queryContext(graph: GraphDatabaseAPI, txInfo: TransactionInfo) = {
    val ctx = new QueryContext_v2_1(graph, txInfo.tx, txInfo.isTopLevelTx, txInfo.statement)
    new ExceptionTranslatingQueryContext_v2_1(ctx)
  }

  def profile(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
    inner.profile(queryContext(graph, txInfo), params)

  def execute(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
    inner.execute(queryContext(graph, txInfo), params)

  def isPeriodicCommit: Boolean = inner.isPeriodicCommit
}

class ExecutionPlanWrapperForV2_0(inner: ExecutionPlan_v2_0) extends ExecutionPlan {

  private def queryContext(graph: GraphDatabaseAPI, txInfo: TransactionInfo) = {
    val ctx = new QueryContext_v2_0(graph, txInfo.tx, txInfo.statement)
    new ExceptionTranslatingQueryContext_v2_0(ctx)
  }

  def profile(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
    inner.profile(queryContext(graph, txInfo), params)

  def execute(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
    inner.execute(queryContext(graph, txInfo), params)

  def isPeriodicCommit: Boolean = false
}

class ExecutionPlanWrapperForV1_9(inner: ExecutionPlan_v1_9) extends ExecutionPlan {

  private def queryContext(graph: GraphDatabaseAPI) =
    new QueryContext_v1_9(graph)

  def profile(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
    inner.profile(queryContext(graph), txInfo.tx, params)

  def execute(graph: GraphDatabaseAPI, txInfo: TransactionInfo, params: Map[String, Any]) =
    inner.execute(queryContext(graph), txInfo.tx, params)

  def isPeriodicCommit: Boolean = false
}

