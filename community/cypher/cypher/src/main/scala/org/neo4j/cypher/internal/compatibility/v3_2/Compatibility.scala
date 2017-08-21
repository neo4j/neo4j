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
package org.neo4j.cypher.internal.compatibility.v3_2

import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compiler.v3_2
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{LegacyNodeIndexUsage, LegacyRelationshipIndexUsage, SchemaIndexScanUsage, SchemaIndexSeekUsage, ExecutionPlan => ExecutionPlan_v3_2}
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilerContext
import org.neo4j.cypher.internal.compiler.v3_2.{InfoLogger, ExplainMode => ExplainModev3_2, NormalMode => NormalModev3_2, ProfileMode => ProfileModev3_2}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.phases.{BaseState, CompilationPhaseTracer, RecordingNotificationLogger}
import org.neo4j.cypher.internal.frontend.v3_2.phases.{CompilationPhaseTracer, RecordingNotificationLogger}
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.spi.v3_2.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.v3_2.{ExceptionTranslatingPlanContext, TransactionBoundGraphStatistics, TransactionBoundPlanContext, TransactionBoundQueryContext, TransactionalContextWrapper => TransactionalContextWrapperV3_2}
import org.neo4j.cypher.internal.spi.v3_3.{TransactionalContextWrapper => TransactionalContextWrapperV3_3}
import org.neo4j.cypher.internal.{frontend, _}
import org.neo4j.graphdb.Result
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.api.query.IndexUsage.{legacyIndexUsage, schemaIndexUsage}
import org.neo4j.kernel.api.query.PlannerInfo
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

import scala.util.Try

trait Compatibility[C <: CompilerContext] {

  val queryCacheSize: Int
  val kernelMonitors: KernelMonitors
  val kernelAPI: KernelAPI

  protected val rewriterSequencer: (String) => RewriterStepSequencer = {
    import RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v3_2.CypherCompiler[C]

  implicit val executionMonitor: QueryExecutionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  def produceParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer,
                         preParsingNotifications: Set[org.neo4j.graphdb.Notification]): ParsedQuery = {
    val notificationLogger = new RecordingNotificationLogger
    val preparedSyntacticQueryForV_3_2 =
      Try(compiler.parseQuery(preParsedQuery.statement,
                              preParsedQuery.rawStatement,
                              notificationLogger,
                              preParsedQuery.planner.name,
                              preParsedQuery.debugOptions,
                              Some(helpers.as3_2(preParsedQuery.offset)), tracer))
    new ParsedQuery {
      override def plan(transactionalContext: TransactionalContextWrapperV3_3,
                        tracer: frontend.v3_3.phases.CompilationPhaseTracer):
      (ExecutionPlan, Map[String, Any]) = exceptionHandler.runSafely {
        val tc = TransactionalContextWrapperV3_2(transactionalContext.tc)
        val planContext = new ExceptionTranslatingPlanContext(new TransactionBoundPlanContext(tc, notificationLogger))
        val syntacticQuery = preparedSyntacticQueryForV_3_2.get
        val pos3_2 = helpers.as3_2(preParsedQuery.offset)
        val (planImpl, extractedParameters) = compiler.planPreparedQuery(syntacticQuery, notificationLogger, planContext,
                                                                         preParsedQuery.debugOptions,
                                                                         Some(pos3_2),
                                                                         helpers.as3_2(tracer))

        // Log notifications/warnings from planning
        planImpl.notifications(planContext).foreach(notificationLogger.log)

        (new ExecutionPlanWrapper(planImpl, transactionalContext,preParsingNotifications, pos3_2), extractedParameters)
      }

      override protected val trier: Try[BaseState] = preparedSyntacticQueryForV_3_2
    }
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v3_2, transactionalContext: TransactionalContextWrapperV3_3,
                             preParsingNotifications: Set[org.neo4j.graphdb.Notification], offSet: frontend.v3_2.InputPosition)
    extends ExecutionPlan {

    private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])

    private def queryContext(transactionalContext: TransactionalContextWrapperV3_3) = {
      val ctx = new TransactionBoundQueryContext(TransactionalContextWrapperV3_2(transactionalContext.tc))(
        searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    def run(transactionalContext: TransactionalContextWrapperV3_3, executionMode: CypherExecutionMode,
            params: Map[String, Any]): Result = {
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainModev3_2
        case CypherExecutionMode.profile => ProfileModev3_2
        case CypherExecutionMode.normal => NormalModev3_2
      }
      exceptionHandler.runSafely {
        val innerParams = typeConversions.asPrivateMap(params)
        val innerResult = inner.run(queryContext(transactionalContext), innerExecutionMode, innerParams)

        new ExecutionResult(
          new ClosingExecutionResult(
            transactionalContext.tc.executingQuery(),
            new ExecutionResultWrapper(innerResult,
                                       inner.plannerUsed,
                                       inner.runtimeUsed,
                                       preParsingNotifications,
                                       Some(offSet)),
            exceptionHandler.runSafely)
        )
      }
    }

    def isPeriodicCommit: Boolean = inner.isPeriodicCommit

    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapperV3_3): Boolean =
      inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(ctx.readOperations))

    override def plannerInfo: PlannerInfo = {
      import scala.collection.JavaConverters._
      new PlannerInfo(inner.plannerUsed.name, inner.runtimeUsed.name, inner.plannedIndexUsage.map {
        case SchemaIndexSeekUsage(identifier, label, propertyKeys) =>
          val labelId = transactionalContext.readOperations.labelGetForName(label)
          schemaIndexUsage(identifier, labelId, label, propertyKeys: _*)
        case SchemaIndexScanUsage(identifier, label, propertyKey) =>
          val labelId = transactionalContext.readOperations.labelGetForName(label)
          schemaIndexUsage(identifier, labelId, label, propertyKey)
        case LegacyNodeIndexUsage(identifier, index) => legacyIndexUsage(identifier, "NODE", index)
        case LegacyRelationshipIndexUsage(identifier, index) => legacyIndexUsage(identifier, "RELATIONSHIP", index)
      }.asJava)
    }
  }

}

class StringInfoLogger(log: Log) extends InfoLogger {

  def info(message: String) {
    log.info(message)
  }
}

