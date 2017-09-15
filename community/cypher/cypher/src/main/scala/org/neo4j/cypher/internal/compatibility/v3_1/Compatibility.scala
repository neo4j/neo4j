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

import java.util.Collections.emptyList
import java.util.function.BiConsumer

import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_1.helpers._
import org.neo4j.cypher.internal.compiler.v3_1
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{ExecutionPlan => ExecutionPlan_v3_1}
import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v3_1.{CompilationPhaseTracer, InfoLogger, ExplainMode => ExplainModev3_1, NormalMode => NormalModev3_1, ProfileMode => ProfileModev3_1, _}
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.spi.v3_1.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.v3_1.{TransactionalContextWrapper => TransactionalContextWrapperV3_1, _}
import org.neo4j.cypher.internal.spi.v3_3.{TransactionalContextWrapper => TransactionalContextWrapperV3_3}
import org.neo4j.cypher.internal.{frontend, _}
import org.neo4j.graphdb.Result
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.api.query.{IndexUsage, PlannerInfo}
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable
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

  implicit val executionMonitor: QueryExecutionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  def produceParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer,
                         preParsingNotifications: Set[org.neo4j.graphdb.Notification]): ParsedQuery = {
    val notificationLogger = new RecordingNotificationLogger
    val preparedSyntacticQueryForV_3_1: Try[PreparedQuerySyntax] =
      Try(compiler.prepareSyntacticQuery(preParsedQuery.statement,
                                         preParsedQuery.rawStatement,
                                         notificationLogger,
                                         preParsedQuery.planner.name,
                                         Some(helpers.as3_1(preParsedQuery.offset)), tracer))
    new ParsedQuery {
      override def plan(transactionalContext: TransactionalContextWrapperV3_3,
                        tracer: frontend.v3_3.phases.CompilationPhaseTracer):
      (ExecutionPlan, Map[String, Any]) =
        exceptionHandler.runSafely {
          val tc = TransactionalContextWrapperV3_1(transactionalContext.tc)
          val planContext = new ExceptionTranslatingPlanContext(new TransactionBoundPlanContext(tc, notificationLogger))
          val syntacticQuery = preparedSyntacticQueryForV_3_1.get
          val (planImpl, extractedParameters) = compiler
            .planPreparedQuery(syntacticQuery, notificationLogger, planContext, Some(as3_1(preParsedQuery.offset)),
                               as3_1(tracer))

          // Log notifications/warnings from planning
          planImpl.notifications(planContext).foreach(notificationLogger += _)

          (new ExecutionPlanWrapper(planImpl, preParsingNotifications, as3_1(preParsedQuery.offset)), extractedParameters)
        }

      override protected val trier: Try[PreparedQuerySyntax] = preparedSyntacticQueryForV_3_1
    }
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v3_1, preParsingNotifications: Set[org.neo4j.graphdb.Notification], offSet: frontend.v3_1.InputPosition)
    extends ExecutionPlan {

    private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])

    private def queryContext(transactionalContext: TransactionalContextWrapperV3_3) = {
      val ctx = new TransactionBoundQueryContext(TransactionalContextWrapperV3_1(transactionalContext.tc))(
        searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    def run(transactionalContext: TransactionalContextWrapperV3_3, executionMode: CypherExecutionMode,
            params: Map[String, Any]): Result = {
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainModev3_1
        case CypherExecutionMode.profile => ProfileModev3_1
        case CypherExecutionMode.normal => NormalModev3_1
      }
      exceptionHandler.runSafely {
        val innerParams = typeConversions.asPrivateMap(params)
        val innerResult: executionplan.InternalExecutionResult = inner
          .run(queryContext(transactionalContext), innerExecutionMode, innerParams)
        new ExecutionResult(
          new ClosingExecutionResult(
            transactionalContext.tc.executingQuery(),
            new ExecutionResultWrapper(innerResult, inner.plannerUsed, inner.runtimeUsed, preParsingNotifications, Some(offSet)),
            exceptionHandler.runSafely)
        )
      }
    }

    def isPeriodicCommit: Boolean = inner.isPeriodicCommit

    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapperV3_3): Boolean =
      inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(ctx.readOperations))

    override val plannerInfo = new PlannerInfo(inner.plannerUsed.name, inner.runtimeUsed.name, emptyList[IndexUsage])


    override def run(transactionalContext: TransactionalContextWrapperV3_3, executionMode: CypherExecutionMode, params: MapValue): Result = {
      var map: mutable.Map[String, Any] = mutable.Map[String, Any]()
      params.foreach(new BiConsumer[String, AnyValue] {
        override def accept(t: String, u: AnyValue): Unit = map.put(t, valueHelper.fromValue(u))
      })

      run(transactionalContext, executionMode, map.toMap)
    }
  }

}

class StringInfoLogger(log: Log) extends InfoLogger {

  def info(message: String) {
    log.info(message)
  }
}
