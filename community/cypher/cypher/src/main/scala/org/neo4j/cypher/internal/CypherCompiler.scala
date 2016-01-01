/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v1_9.executionplan.{ExecutionPlan => ExecutionPlan_v1_9}
import org.neo4j.cypher.internal.compiler.v1_9.{CypherCompiler => CypherCompiler1_9}
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{ExecutionPlan => ExecutionPlan_v2_0}
import org.neo4j.cypher.internal.compiler.v2_0.spi.{ExceptionTranslatingQueryContext => ExceptionTranslatingQueryContext_v2_0}
import org.neo4j.cypher.internal.compiler.v2_0.{CypherCompiler => CypherCompiler2_0}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.{ExecutionPlan => ExecutionPlan_v2_1}
import org.neo4j.cypher.internal.compiler.v2_1.spi.{ExceptionTranslatingQueryContext => ExceptionTranslatingQueryContext_v2_1}
import org.neo4j.cypher.internal.compiler.v2_1.{CypherCompilerFactory => CypherCompilerFactory2_1}
import org.neo4j.cypher.internal.spi.v1_9.{GDSBackedQueryContext => QueryContext_v1_9}
import org.neo4j.cypher.internal.spi.v2_0.{TransactionBoundPlanContext => PlanContext_v2_0, TransactionBoundQueryContext => QueryContext_v2_0}
import org.neo4j.cypher.internal.spi.v2_1.{TransactionBoundPlanContext => PlanContext_v2_1, TransactionBoundQueryContext => QueryContext_v2_1}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.kernel.api.{KernelAPI, Statement}
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.kernel.{GraphDatabaseAPI, InternalAbstractGraphDatabase}

import scala.util.Try

object CypherCompiler {
  val DEFAULT_QUERY_CACHE_SIZE: Int = 128
}

case class PreParsedQuery(statement: String, version: CypherVersion)

class CypherCompiler(graph: GraphDatabaseService,
                     kernelAPI: KernelAPI,
                     kernelMonitors: KernelMonitors,
                     defaultVersion: CypherVersion = CypherVersion.vDefault,
                     optionParser: CypherOptionParser) {

  private val queryCacheSize: Int = getQueryCacheSize

  private val queryCache2_0 = new LRUCache[Object, Object](queryCacheSize)
  private val queryCache1_9 = new LRUCache[String, Object](queryCacheSize)

  val ronjaCompiler2_1 = CypherCompilerFactory2_1.ronjaCompiler(graph, queryCacheSize, kernelMonitors)
  val legacyCompiler2_1 = CypherCompilerFactory2_1.legacyCompiler(graph, queryCacheSize, kernelMonitors)
  val compiler2_0 = new CypherCompiler2_0(graph, (q, f) => queryCache2_0.getOrElseUpdate(q, f))
  val compiler1_9 = new CypherCompiler1_9(graph, (q, f) => queryCache1_9.getOrElseUpdate(q, f))

  @throws(classOf[SyntaxException])
  def parseQuery(queryText: String): ParsedQuery = {
    val queryWithOptions = optionParser(queryText)
    val preParsedQuery = preParse(queryWithOptions)
    val version = preParsedQuery.version
    val statementAsText = preParsedQuery.statement

    version match {
      case CypherVersion.experimental =>
        val preparedQueryForV_experimental = Try(ronjaCompiler2_1.prepareQuery(statementAsText))
        new ParsedQuery {
          def isPeriodicCommit = preparedQueryForV_experimental.map(_.isPeriodicCommit).getOrElse(false)
          def plan(statement: Statement) = {
            val planContext = new PlanContext_v2_1(statement, kernelAPI, graph)
            val (planImpl, extractedParameters) = ronjaCompiler2_1.planPreparedQuery(preparedQueryForV_experimental.get, planContext)
            (new ExecutionPlanWrapperForV2_1( planImpl ), extractedParameters)
          }
        }

      case CypherVersion.v2_1 =>
        new ParsedQuery {
          val preparedQueryForV_2_1 = Try(ronjaCompiler2_1.prepareQuery(statementAsText))
          override def plan(statement: Statement): (ExecutionPlan, Map[String, Any]) = {
            val planContext = new PlanContext_v2_1(statement, kernelAPI, graph)
            val (planImpl, extractedParameters) = legacyCompiler2_1.planPreparedQuery(preparedQueryForV_2_1.get, planContext)
            (new ExecutionPlanWrapperForV2_1( planImpl ), extractedParameters)
          }

          override def isPeriodicCommit: Boolean = preparedQueryForV_2_1.map(_.isPeriodicCommit).getOrElse(false)
        }

      case CypherVersion.v2_0 =>
        new ParsedQuery {
          override def plan(statement: Statement): (ExecutionPlan, Map[String, Any]) = {
            val planImpl = compiler2_0.prepare(statementAsText, new PlanContext_v2_0(statement, graph))
            (new ExecutionPlanWrapperForV2_0(planImpl), Map.empty)
          }

          override def isPeriodicCommit: Boolean = false
        }

      case CypherVersion.v1_9 =>
        new ParsedQuery {
          override def plan(statement: Statement): (ExecutionPlan, Map[String, Any]) = {
            val planImpl = compiler1_9.prepare(statementAsText)
            (new ExecutionPlanWrapperForV1_9(planImpl), Map.empty)
          }

          override def isPeriodicCommit: Boolean = false
        }
    }
  }

  private def preParse(queryWithOption: CypherQueryWithOptions): PreParsedQuery = {
    val versionOptions = collectSingle( queryWithOption.options ) ({ case VersionOption( v ) => v })
    val version = versionOptions match {
      case Right(Some(v)) => CypherVersion(v)
      case Right(None)    => defaultVersion
      case Left(versions) => throw new SyntaxException(s"You must specify only one version for a query (found: $versions)")
    }
    PreParsedQuery(queryWithOption.statement, version)
  }

  private def collectSingle[A, B](input: Seq[A])(pf: PartialFunction[A, B]): Either[Seq[B], Option[B]] =
    input.collect(pf).toList match {
      case Nil      => Right(None)
      case x :: Nil => Right(Some(x))
      case matches  => Left(matches)
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

