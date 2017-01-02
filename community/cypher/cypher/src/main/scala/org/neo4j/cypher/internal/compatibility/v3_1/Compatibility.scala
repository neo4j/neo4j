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
package org.neo4j.cypher.internal.compatibility.v3_1

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_1.helpers._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{ExecutionPlan => ExecutionPlan_v3_1}
import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v3_1.{CompilationPhaseTracer, InfoLogger, ExplainMode => ExplainModev3_1, NormalMode => NormalModev3_1, ProfileMode => ProfileModev3_1, _}
import org.neo4j.cypher.internal.compiler.{v3_1, v3_2}
import org.neo4j.cypher.internal.spi.v3_1.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.v3_1.{TransactionalContextWrapper => TransactionalContextWrapperV3_1, _}
import org.neo4j.cypher.internal.spi.v3_2.{TransactionalContextWrapper => TransactionalContextWrapperV3_2}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
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
    import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v3_1.CypherCompiler

  implicit val executionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  def produceParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer) = {
    val notificationLogger = new RecordingNotificationLogger
    val preparedSyntacticQueryForV_3_1 =
      Try(compiler.prepareSyntacticQuery(preParsedQuery.statement,
        preParsedQuery.rawStatement,
        notificationLogger,
        preParsedQuery.planner.name,
        Some(as3_1(preParsedQuery.offset)), tracer))
    new ParsedQuery {
      def isPeriodicCommit = preparedSyntacticQueryForV_3_1.map(_.isPeriodicCommit).getOrElse(false)

      def plan(transactionalContext: TransactionalContextWrapperV3_2, tracer: v3_2.CompilationPhaseTracer): (ExecutionPlan, Map[String, Any]) =
        exceptionHandler.runSafely {
          val tc = TransactionalContextWrapperV3_1(transactionalContext.tc)
          val planContext = new ExceptionTranslatingPlanContext(new TransactionBoundPlanContext(tc, notificationLogger))
          val syntacticQuery = preparedSyntacticQueryForV_3_1.get
          val (planImpl, extractedParameters) = compiler.planPreparedQuery(syntacticQuery, notificationLogger, planContext, Some(as3_1(preParsedQuery.offset)), as3_1(tracer))

          // Log notifications/warnings from planning
          planImpl.notifications(planContext).foreach(notificationLogger += _)

          (new ExecutionPlanWrapper(planImpl), extractedParameters)
        }

      override def hasErrors = preparedSyntacticQueryForV_3_1.isFailure
    }
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v3_1) extends ExecutionPlan {

    private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])

    private def queryContext(transactionalContext: TransactionalContextWrapperV3_2) = {
      val ctx = new TransactionBoundQueryContext(TransactionalContextWrapperV3_1(transactionalContext.tc))(searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    def run(transactionalContext: TransactionalContextWrapperV3_2, executionMode: CypherExecutionMode, params: Map[String, Any]): ExecutionResult = {
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainModev3_1
        case CypherExecutionMode.profile => ProfileModev3_1
        case CypherExecutionMode.normal => NormalModev3_1
      }
      exceptionHandler.runSafely {
        val innerParams = typeConversions.asPrivateMap(params)
        val innerResult = inner.run(queryContext(transactionalContext), innerExecutionMode, innerParams)
        new ClosingExecutionResult(
          transactionalContext.tc.executingQuery(),
          new ExecutionResultWrapper(innerResult, inner.plannerUsed, inner.runtimeUsed),
          exceptionHandler.runSafely
        )
      }
    }

    def isPeriodicCommit = inner.isPeriodicCommit

    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapperV3_2): Boolean =
      inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(ctx.readOperations))
  }
}

class StringInfoLogger(log: Log) extends InfoLogger {
  def info(message: String) {
    log.info(message)
  }
}
