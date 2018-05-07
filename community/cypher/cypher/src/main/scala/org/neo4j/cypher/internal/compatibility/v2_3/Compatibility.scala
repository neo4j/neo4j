/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.function.BiConsumer

import org.neo4j.cypher.CypherExecutionMode
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compiler.v2_3
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{EntityAccessor, ExecutionPlan => ExecutionPlan_v2_3}
import org.neo4j.cypher.internal.compiler.v2_3.spi.{PlanContext, QueryContext}
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v2_3.{InfoLogger, ExplainMode => ExplainModev2_3, NormalMode => NormalModev2_3, ProfileMode => ProfileModev2_3, _}
import org.neo4j.cypher.internal.compiler.v3_5.{CacheCheckResult, FineToReuse, NeedsReplan}
import org.neo4j.cypher.internal.frontend.v3_5
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.runtime.interpreted.{LastCommittedTxIdProvider, TransactionalContextWrapper}
import org.neo4j.cypher.internal.spi.v2_3.{TransactionBoundGraphStatistics, TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.graphdb.{Node, Relationship, Result}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.query.{IndexUsage, PlannerInfo}
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
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

  protected val rewriterSequencer: (String) => RewriterStepSequencer = {
    import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v2_3.CypherCompiler

  implicit val executionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  def produceParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer,
                         preParsingNotifications: Set[org.neo4j.graphdb.Notification]) = {
    import org.neo4j.cypher.internal.compatibility.v2_3.helpers.as2_3
    val notificationLogger = new RecordingNotificationLogger
    val preparedQueryForV_2_3: Try[PreparedQuery] =
      Try(compiler.prepareQuery(preParsedQuery.statement,
                                preParsedQuery.rawStatement,
                                notificationLogger,
                                preParsedQuery.planner.name,
                                Some(as2_3(preParsedQuery.offset)), tracer))
    new ParsedQuery {
      def plan(transactionalContext: TransactionalContextWrapper,
               tracer: v3_5.phases.CompilationPhaseTracer): (ExecutionPlan, Map[String, Any], Seq[String]) = exceptionHandler
        .runSafely {
          val planContext: PlanContext = new TransactionBoundPlanContext(transactionalContext)
          val (planImpl, extractedParameters) = compiler
            .planPreparedQuery(preparedQueryForV_2_3.get, planContext, as2_3(tracer))

          // Log notifications/warnings from planning
          planImpl.notifications(planContext).foreach(notificationLogger += _)

          (new ExecutionPlanWrapper(planImpl, preParsingNotifications, as2_3(preParsedQuery.offset)), extractedParameters, Seq.empty[String])
        }

      override protected val trier = preparedQueryForV_2_3
    }
  }

  class ExecutionPlanWrapper(inner: ExecutionPlan_v2_3, preParsingNotifications: Set[org.neo4j.graphdb.Notification], offSet: frontend.v2_3.InputPosition)
    extends ExecutionPlan {

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
        val innerResult = inner
          .run(queryContext(transactionalContext), transactionalContext.statement, innerExecutionMode, params)

        new ExecutionResult(
          new ClosingExecutionResult(
            query,
            new ExecutionResultWrapper(innerResult, inner.plannerUsed, inner.runtimeUsed, preParsingNotifications, Some(offSet)),
            exceptionHandler.runSafely)
        )
      }
    }

    def isPeriodicCommit = inner.isPeriodicCommit

    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapper): CacheCheckResult = {
      val stale = inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(ctx.dataRead, ctx.schemaRead))
      if (stale)
        NeedsReplan(0)
      else
        FineToReuse
    }

    override val plannerInfo = new PlannerInfo(inner.plannerUsed.name, inner.runtimeUsed.name, emptyList[IndexUsage])

    override def run(transactionalContext: TransactionalContextWrapper, executionMode: CypherExecutionMode, params: MapValue): Result = {
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

class EntityAccessorWrapper(proxySpi: EmbeddedProxySPI) extends EntityAccessor {
  override def newNodeProxyById(id: Long): Node = proxySpi.newNodeProxy(id)

  override def newRelationshipProxyById(id: Long): Relationship = proxySpi.newRelationshipProxy(id)
}
