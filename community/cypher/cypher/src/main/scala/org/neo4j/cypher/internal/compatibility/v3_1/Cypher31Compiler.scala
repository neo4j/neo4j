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
package org.neo4j.cypher.internal.compatibility.v3_1

import java.util.Collections.emptyList

import org.neo4j.cypher.CypherExecutionMode
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_1.helpers._
import org.neo4j.cypher.internal.compiler.v3_1
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{ExecutionPlan => ExecutionPlan_v3_1, InternalExecutionResult => InternalExecutionResult3_1}
import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v3_1.{InfoLogger, ExplainMode => ExplainModev3_1, NormalMode => NormalModev3_1, ProfileMode => ProfileModev3_1, _}
import org.neo4j.cypher.internal.frontend.v3_1.{InputPosition => InputPosition3_1}
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.runtime.interpreted.ValueConversion
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.spi.v3_1.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.v3_1.{TransactionalContextWrapper => TransactionalContextWrapperV3_1, _}
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.graphdb.{Notification, Result}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.query.{CompilerInfo, IndexUsage}
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v3_5.frontend.phases

import scala.collection.mutable

trait Cypher31Compiler extends CachingPlanner[PreparedQuerySyntax] with Compiler {

  val graph: GraphDatabaseQueryService
  val queryCacheSize: Int
  val kernelMonitors: KernelMonitors

  override def parserCacheSize: Int = queryCacheSize

  protected val rewriterSequencer: (String) => RewriterStepSequencer = {
    import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v3_1.CypherCompiler

  private val executionMonitor: QueryExecutionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  class Cypher31ExecutableQuery(inner: ExecutionPlan_v3_1,
                                preParsingNotifications: Set[org.neo4j.graphdb.Notification],
                                offSet: InputPosition3_1,
                                override val paramNames: Seq[String],
                                override val extractedParams: MapValue) extends ExecutableQuery {

    private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])

    private def queryContext(transactionalContext: TransactionalContext) = {
      val ctx = new TransactionBoundQueryContext(TransactionalContextWrapperV3_1(transactionalContext))(
        searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    private def run(transactionalContext: TransactionalContext, executionMode: CypherExecutionMode,
            params: Map[String, Any]): Result = {
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainModev3_1
        case CypherExecutionMode.profile => ProfileModev3_1
        case CypherExecutionMode.normal => NormalModev3_1
      }
      exceptionHandler.runSafely {
        val innerParams = typeConversions.asPrivateMap(params)
        val innerResult: InternalExecutionResult3_1 =
          inner.run(queryContext(transactionalContext), innerExecutionMode, innerParams)
        new ExecutionResult( // javacompat
          new CompatibilityClosingExecutionResult( // closing
            transactionalContext.executingQuery(),
            new ExecutionResultWrapper( // 3.5 wrapping
              innerResult, // 3.1
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

    override def execute(transactionalContext: TransactionalContext, preParsedQuery: PreParsedQuery, params: MapValue): Result = {
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
                       tracer: phases.CompilationPhaseTracer,
                       preParsingNotifications: Set[Notification],
                       transactionalContext: TransactionalContext,
                       params: MapValue
                      ): ExecutableQuery = {

    exceptionHandler.runSafely {
      val notificationLogger = new RecordingNotificationLogger
      val position3_1 = as3_1(preParsedQuery.offset)
      val tracer3_1 = as3_1(tracer)

      val syntacticQuery3_1: PreparedQuerySyntax =
        getOrParse(preParsedQuery, new Parser3_1(compiler, notificationLogger, position3_1, tracer3_1))

      val tc = TransactionalContextWrapperV3_1(transactionalContext)
      val planContext = new ExceptionTranslatingPlanContext(new TransactionBoundPlanContext(tc, notificationLogger))
      val (executionPlan3_1, extractedParameters) =
        compiler.planPreparedQuery(syntacticQuery3_1, notificationLogger, planContext, Some(position3_1), tracer3_1)

      // Log notifications/warnings from planning
      executionPlan3_1.notifications(planContext).foreach(notificationLogger += _)

      new Cypher31ExecutableQuery(
        executionPlan3_1,
        preParsingNotifications,
        position3_1,
        Seq.empty[String],
        ValueConversion.asValues(extractedParameters))
    }
  }
}

class Parser3_1(compiler: CypherCompiler,
                notificationLogger: InternalNotificationLogger,
                offset: InputPosition3_1,
                tracer: v3_1.CompilationPhaseTracer
               ) extends Parser[PreparedQuerySyntax] {

  override def parse(preParsedQuery: PreParsedQuery): PreparedQuerySyntax = {
    compiler.prepareSyntacticQuery(preParsedQuery.statement,
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
