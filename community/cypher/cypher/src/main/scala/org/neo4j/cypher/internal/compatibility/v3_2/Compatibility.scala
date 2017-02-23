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

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compiler.v3_2
import org.neo4j.cypher.internal.compiler.v3_2.executionplan.{LegacyNodeIndexUsage, LegacyRelationshipIndexUsage, SchemaIndexScanUsage, SchemaIndexSeekUsage, ExecutionPlan => ExecutionPlan_v3_2}
import org.neo4j.cypher.internal.compiler.v3_2.phases.CompilerContext
import org.neo4j.cypher.internal.compiler.v3_2.{InfoLogger, ExplainMode => ExplainModev3_2, NormalMode => NormalModev3_2, ProfileMode => ProfileModev3_2}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_2.phases.{CompilationPhaseTracer, RecordingNotificationLogger}
import org.neo4j.cypher.internal.spi.v3_2.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.v3_2._
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
        Some(preParsedQuery.offset), tracer))
    new ParsedQuery {
      override def plan(transactionalContext: TransactionalContextWrapper, tracer: CompilationPhaseTracer):
        (ExecutionPlan, Map[String, Any]) = exceptionHandler.runSafely {
        val planContext = new ExceptionTranslatingPlanContext(new TransactionBoundPlanContext(transactionalContext, notificationLogger))
        val syntacticQuery = preparedSyntacticQueryForV_3_2.get
        val (planImpl, extractedParameters) = compiler.planPreparedQuery(syntacticQuery, notificationLogger, planContext, preParsedQuery.debugOptions, Some(preParsedQuery.offset), tracer)

        // Log notifications/warnings from planning
        planImpl.notifications(planContext).foreach(notificationLogger.log)

        (new ExecutionPlanWrapper(planImpl, preParsingNotifications), extractedParameters)
      }

      override protected val trier = preparedSyntacticQueryForV_3_2
    }
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v3_2, preParsingNotifications: Set[org.neo4j.graphdb.Notification])
    extends ExecutionPlan {

    private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])

    private def queryContext(transactionalContext: TransactionalContextWrapper) = {
      val ctx = new TransactionBoundQueryContext(transactionalContext)(searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    def run(transactionalContext: TransactionalContextWrapper, executionMode: CypherExecutionMode, params: Map[String, Any]): ExecutionResult = {
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainModev3_2
        case CypherExecutionMode.profile => ProfileModev3_2
        case CypherExecutionMode.normal => NormalModev3_2
      }
      exceptionHandler.runSafely {
        val innerParams = typeConversions.asPrivateMap(params)
        val innerResult = inner.run(queryContext(transactionalContext), innerExecutionMode, innerParams)
        new ClosingExecutionResult(
          transactionalContext.tc.executingQuery(),
          new ExecutionResultWrapper(innerResult, inner.plannerUsed, inner.runtimeUsed, preParsingNotifications),
          exceptionHandler.runSafely
        )
      }
    }

    def isPeriodicCommit = inner.isPeriodicCommit

    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapper): Boolean =
      inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(ctx.readOperations))

    override def plannerInfo = {
      import scala.collection.JavaConverters._
      new PlannerInfo(inner.plannerUsed.name, inner.runtimeUsed.name, inner.plannedIndexUsage.map {
        case SchemaIndexSeekUsage(identifier, label, propertyKey) => schemaIndexUsage(identifier, label, propertyKey)
        case SchemaIndexScanUsage(identifier, label, propertyKey) => schemaIndexUsage(identifier, label, propertyKey)
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

