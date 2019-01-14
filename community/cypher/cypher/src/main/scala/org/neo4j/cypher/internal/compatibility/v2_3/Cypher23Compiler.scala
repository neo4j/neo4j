/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.CypherExecutionMode
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v2_3.helpers.as2_3
import org.neo4j.cypher.internal.compiler.v2_3
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.{EntityAccessor, ExecutionPlan => ExecutionPlan_v2_3}
import org.neo4j.cypher.internal.compiler.v2_3.spi.{PlanContext, QueryContext}
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v2_3.{InfoLogger, ExplainMode => ExplainModev2_3, NormalMode => NormalModev2_3, ProfileMode => ProfileModev2_3, _}
import org.neo4j.cypher.internal.frontend.v2_3.InputPosition
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionalContextWrapper, ValueConversion}
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.spi.v2_3.{TransactionBoundGraphStatistics, TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.graphdb.{Node, Notification, Relationship, Result}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.query.{CompilerInfo, IndexUsage}
import org.neo4j.kernel.impl.core.EmbeddedProxySPI
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer

import scala.collection.mutable

trait Cypher23Compiler extends CachingPlanner[PreparedQuery] with Compiler {

  val graph: GraphDatabaseQueryService
  val queryCacheSize: Int
  val kernelMonitors: KernelMonitors

  override def parserCacheSize: Int = queryCacheSize

  protected val rewriterSequencer: (String) => RewriterStepSequencer = {
    import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v2_3.CypherCompiler

  private val executionMonitor: QueryExecutionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  class Cypher23ExecutableQuery(inner: ExecutionPlan_v2_3,
                                preParsingNotifications: Set[org.neo4j.graphdb.Notification],
                                offSet: frontend.v2_3.InputPosition,
                                override val paramNames: Seq[String],
                                override val extractedParams: MapValue) extends ExecutableQuery {

    private def queryContext(transactionalContext: TransactionalContextWrapper): QueryContext =
      new ExceptionTranslatingQueryContext(new TransactionBoundQueryContext(transactionalContext))

    private def run(transactionalContext: TransactionalContext, executionMode: CypherExecutionMode, params: Map[String, Any]): ExecutionResult = {
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainModev2_3
        case CypherExecutionMode.profile => ProfileModev2_3
        case CypherExecutionMode.normal => NormalModev2_3
      }

      val query = transactionalContext.executingQuery()

      exceptionHandler.runSafely {
        val innerResult = inner
          .run(queryContext(TransactionalContextWrapper(transactionalContext)), transactionalContext.statement, innerExecutionMode, params)

        new ExecutionResult(
          new CompatibilityClosingExecutionResult(
            query,
            new ExecutionResultWrapper(
              innerResult,
              inner.plannerUsed,
              inner.runtimeUsed,
              preParsingNotifications,
              Some(offSet)
            ),
            exceptionHandler.runSafely
          )(executionMonitor)
        )
      }
    }

    override def reusabilityState(lastCommittedTxId: () => Long, ctx: TransactionalContext): ReusabilityState = {
      val stale = inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(ctx))
      if (stale)
        NeedsReplan(0)
      else
        FineToReuse
    }

    override val compilerInfo = new CompilerInfo(inner.plannerUsed.name, inner.runtimeUsed.name, emptyList[IndexUsage])

    override def execute(transactionalContext: TransactionalContext,
                         preParsedQuery: PreParsedQuery,
                         params: MapValue): Result = {
      var map: mutable.Map[String, Any] = mutable.Map[String, Any]()
      params.foreach(new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
        override def accept(t: String, u: AnyValue): Unit = map.put(t, valueHelper.fromValue(u))
      })

      run(transactionalContext, preParsedQuery.executionMode, map.toMap)
    }

    override def planDescription(): InternalPlanDescription =
      InternalPlanDescription.error("Plan description is not available")
  }

  override def compile(preParsedQuery: PreParsedQuery,
                       tracer: CompilationPhaseTracer,
                       preParsingNotifications: Set[Notification],
                       transactionalContext: TransactionalContext,
                       params: MapValue
                      ): ExecutableQuery = {

    exceptionHandler.runSafely {
      val notificationLogger = new RecordingNotificationLogger
      val position2_3 = as2_3(preParsedQuery.offset)
      val tracer2_3 = as2_3(tracer)

      val preparedQuery2_3: PreparedQuery =
        getOrParse(preParsedQuery, new Parser2_3(compiler, notificationLogger, position2_3, tracer2_3))

      val planContext: PlanContext = new TransactionBoundPlanContext(TransactionalContextWrapper(transactionalContext))
      val (executionPlan2_3, extractedParameters) =
        compiler.planPreparedQuery(preparedQuery2_3, planContext, tracer2_3)

      // Log notifications/warnings from planning
      executionPlan2_3.notifications(planContext).foreach(notificationLogger += _)
      new Cypher23ExecutableQuery(
        executionPlan2_3,
        preParsingNotifications,
        position2_3,
        Seq.empty[String],
        ValueConversion.asValues(extractedParameters))
    }
  }
}

class Parser2_3(compiler: CypherCompiler,
                notificationLogger: InternalNotificationLogger,
                offset: InputPosition,
                tracer: v2_3.CompilationPhaseTracer
               ) extends Parser[PreparedQuery] {

  override def parse(preParsedQuery: PreParsedQuery): PreparedQuery = {
    compiler.prepareQuery(preParsedQuery.statement,
                          preParsedQuery.rawStatement,
                          notificationLogger,
                          preParsedQuery.planner.name,
                          Some(offset),
                          tracer)
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
