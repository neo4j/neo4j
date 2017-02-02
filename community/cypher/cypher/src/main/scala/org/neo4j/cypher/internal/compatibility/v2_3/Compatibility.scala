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
package org.neo4j.cypher.internal.compatibility.v2_3

import java.util.Collections.emptyList

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compiler.v2_3
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{EntityAccessor, ExecutionPlan => ExecutionPlan_v2_3}
import org.neo4j.cypher.internal.compiler.v2_3.spi.{PlanContext, QueryContext}
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v2_3.{InfoLogger, ExplainMode => ExplainModev2_3, NormalMode => NormalModev2_3, ProfileMode => ProfileModev2_3, _}
import org.neo4j.cypher.internal.frontend.v3_2
import org.neo4j.cypher.internal.spi.v2_3.{TransactionBoundGraphStatistics, TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.cypher.internal.spi.v3_2.TransactionalContextWrapper
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.ExecutingQuery.PlannerInfo
import org.neo4j.kernel.api.{IndexUsage, KernelAPI}
import org.neo4j.kernel.impl.core.NodeManager
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

import scala.util.Try


trait Compatibility {

  val graph: GraphDatabaseQueryService
  val queryCacheSize: Int
  val kernelMonitors: KernelMonitors
  val kernelAPI: KernelAPI

  protected val rewriterSequencer: (String) => RewriterStepSequencer = {
    import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v2_3.CypherCompiler

  implicit val executionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  def produceParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer, preParsingNotifications: Set[org.neo4j.graphdb.Notification]) = {
    import org.neo4j.cypher.internal.compatibility.v2_3.helpers.as2_3
    val notificationLogger = new RecordingNotificationLogger
    val preparedQueryForV_2_3 =
      Try(compiler.prepareQuery(preParsedQuery.statement,
        preParsedQuery.rawStatement,
        notificationLogger,
        preParsedQuery.planner.name,
        Some(as2_3(preParsedQuery.offset)), tracer))
    new ParsedQuery {
      def plan(transactionalContext: TransactionalContextWrapper, tracer: v3_2.phases.CompilationPhaseTracer): (org.neo4j.cypher.internal.ExecutionPlan, Map[String, Any]) = exceptionHandler.runSafely {
        val planContext: PlanContext = new TransactionBoundPlanContext(transactionalContext)
        val (planImpl, extractedParameters) = compiler.planPreparedQuery(preparedQueryForV_2_3.get, planContext, as2_3(tracer))

        // Log notifications/warnings from planning
        planImpl.notifications(planContext).foreach(notificationLogger += _)

        (new ExecutionPlanWrapper(planImpl, preParsingNotifications), extractedParameters)
      }

      override protected val trier: Try[PreparedQuery] = preparedQueryForV_2_3
    }
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v2_3, preParsingNotifications: Set[org.neo4j.graphdb.Notification])
    extends org.neo4j.cypher.internal.ExecutionPlan {

    private def queryContext(transactionalContext: TransactionalContextWrapper): QueryContext =
      new ExceptionTranslatingQueryContext(new TransactionBoundQueryContext(transactionalContext))

    def run(transactionalContext: TransactionalContextWrapper, executionMode: CypherExecutionMode, params: Map[String, Any]): ExecutionResult = {
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainModev2_3
        case CypherExecutionMode.profile => ProfileModev2_3
        case CypherExecutionMode.normal => NormalModev2_3
      }

      val query = transactionalContext.tc.executingQuery()

      exceptionHandler.runSafely {
        val innerResult = inner.run(queryContext(transactionalContext), transactionalContext.statement, innerExecutionMode, params)
        new ClosingExecutionResult(
          query,
          new ExecutionResultWrapper(innerResult, inner.plannerUsed, inner.runtimeUsed, preParsingNotifications),
          exceptionHandler.runSafely
        )
      }
    }

    def isPeriodicCommit = inner.isPeriodicCommit

    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapper): Boolean =
      inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(ctx.readOperations))

    override def plannerInfo = new PlannerInfo(inner.plannerUsed.name, inner.runtimeUsed.name, emptyList[IndexUsage])
  }

}

class StringInfoLogger(log: Log) extends InfoLogger {
  def info(message: String) {
    log.info(message)
  }
}

class EntityAccessorWrapper(nodeManager: NodeManager) extends EntityAccessor {
  override def newNodeProxyById(id: Long): Node = nodeManager.newNodeProxyById(id)

  override def newRelationshipProxyById(id: Long): Relationship = nodeManager.newRelationshipProxyById(id)
}
