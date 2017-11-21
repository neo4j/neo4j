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
package org.neo4j.cypher.internal.compatibility.v3_3

import java.time.Clock

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility._
import org.neo4j.cypher.internal.compatibility.v3_4._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.procs.ProcedureCallOrSchemaCommandExecutionPlanBuilder
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{ExecutionPlan => ExecutionPlan_v3_4}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_3
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{idp => idpV3_3}
import org.neo4j.cypher.internal.compiler.v3_3.planner.{logical => logicalV3_3}
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.compiler.v3_4.phases.{CompilationContains, LogicalPlanState}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, Statement => StatementV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.phases
import org.neo4j.cypher.internal.frontend.v3_3.phases.{Monitors => MonitorsV3_3, RecordingNotificationLogger => RecordingNotificationLoggerV3_3}
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.phases._
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.runtime.{ExplainMode, InternalExecutionResult, NormalMode, ProfileMode}
import org.neo4j.cypher.internal.spi.v3_3.{ExceptionTranslatingPlanContext => ExceptionTranslatingPlanContextV3_3, TransactionBoundPlanContext => TransactionBoundPlanContextV3_3, TransactionalContextWrapper => TransactionalContextWrapperV3_3}
import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.v3_4.logical.plans.{ExplicitNodeIndexUsage, ExplicitRelationshipIndexUsage, SchemaIndexScanUsage, SchemaIndexSeekUsage}
import org.neo4j.cypher.{CypherExecutionMode, exceptionHandler}
import org.neo4j.graphdb.Result
import org.neo4j.kernel.api.query.IndexUsage.{explicitIndexUsage, schemaIndexUsage}
import org.neo4j.kernel.api.query.PlannerInfo
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._
import scala.util.Try

trait Compatibility[CONTEXT3_3 <: v3_3.phases.CompilerContext,
CONTEXT3_4 <: CommunityRuntimeContext,
T <: Transformer[CONTEXT3_4, LogicalPlanState, CompilationState]] {
  val kernelMonitors: KernelMonitors
  val clock: Clock
  val monitors: MonitorsV3_3
  val config: v3_3.CypherCompilerConfiguration
  val logger: InfoLogger
  val runtimeBuilder: RuntimeBuilder[T]
  val contextCreatorV3_3: v3_3.ContextCreator[CONTEXT3_3]
  val contextCreatorV3_4: ContextCreator[CONTEXT3_4]
  val maybePlannerName: Option[v3_3.CostBasedPlannerName]
  val maybeRuntimeName: Option[RuntimeName]
  val maybeUpdateStrategy: Option[v3_3.UpdateStrategy]
  val cacheMonitor: AstCacheMonitor
  val cacheAccessor: MonitoringCacheAccessor[StatementV3_3, ExecutionPlan_v3_4]
  val monitorTag = "cypher3.4"

  protected val rewriterSequencer: (String) => RewriterStepSequencer = {
    import RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v3_3.CypherCompiler[CONTEXT3_3]


  private def queryGraphSolver = Compatibility.createQueryGraphSolver(maybePlannerName.getOrElse(v3_3.CostBasedPlannerName.default), monitors, config)

  def createExecPlan: Transformer[CONTEXT3_4, LogicalPlanState, CompilationState] = {
      ProcedureCallOrSchemaCommandExecutionPlanBuilder andThen
      If((s: CompilationState) => s.maybeExecutionPlan.isEmpty)(
        runtimeBuilder.create(maybeRuntimeName, config.useErrorsOverWarnings).adds(CompilationContains[ExecutionPlan_v3_4])
      )
  }

  private val planCacheFactory = () => new LFUCache[StatementV3_3, ExecutionPlan_v3_4](config.queryCacheSize)

  implicit lazy val executionMonitor: QueryExecutionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  def produceParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer,
                         preParsingNotifications: Set[org.neo4j.graphdb.Notification]): ParsedQuery = {
    val inputPosition3_3 = helpers.as3_3(preParsedQuery.offset)
    val notificationLogger = new RecordingNotificationLoggerV3_3(Some(inputPosition3_3))

    val tracer3_3 = helpers.as3_3(tracer)
    val preparedSyntacticQueryForV_3_3: Try[phases.BaseState] =
      Try(compiler.parseQuery(preParsedQuery.statement,
        preParsedQuery.rawStatement,
        notificationLogger, preParsedQuery.planner.name,
        preParsedQuery.debugOptions,
        Some(helpers.as3_3(preParsedQuery.offset)), tracer3_3))
    new ParsedQuery {
      override def plan(transactionalContext: TransactionalContextWrapper, tracer: CompilationPhaseTracer):
      (ExecutionPlan, Map[String, Any]) = exceptionHandler.runSafely {
        val syntacticQuery = preparedSyntacticQueryForV_3_3.get

        //Context used for db communication during planning
        val tc = TransactionalContextWrapperV3_3(transactionalContext.tc)
        val planContext = new ExceptionTranslatingPlanContextV3_3(new TransactionBoundPlanContextV3_3(tc, notificationLogger))

        // TODO try to port the actual simpleExpressionEvaluator from 3.3
        def simpleExpressionEvaluator = new logicalV3_3.ExpressionEvaluator {
          override def evaluateExpression(expr: Expression): Option[Any] = None
        }

        //Context used to create logical plans
        val contextV3_3: CONTEXT3_3 = contextCreatorV3_3.create(tracer3_3, notificationLogger, planContext,
          syntacticQuery.queryText, preParsedQuery.debugOptions,
          Some(inputPosition3_3), monitors,
          logicalV3_3.CachedMetricsFactory(logicalV3_3.SimpleMetricsFactory), queryGraphSolver,
          config, maybeUpdateStrategy.getOrElse(v3_3.defaultUpdateStrategy),
          clock, simpleExpressionEvaluator)
        // TODO should we create a 3.4 context with the same parameters, or map the 3.3 context later
        val contextV3_4: CONTEXT3_4 = ???

        //Prepare query for caching
        val preparedQuery = compiler.normalizeQuery(syntacticQuery, contextV3_3)
        // TODO do we need two separate or one joined cache?
        val cache = provideCache(cacheAccessor, cacheMonitor, planContext, planCacheFactory)
        val statisticsV3_4 = GraphStatisticsWrapper(planContext.statistics)
        val isStale = (plan: ExecutionPlan_v3_4) => plan.isStale(planContext.txIdProvider, statisticsV3_4)

        //Just in the case the query is not in the cache do we want to do the full planning + creating executable plan
        def createPlan(): ExecutionPlan_v3_4 = {
          val logicalPlanStateV3_3 = compiler.planPreparedQuery(preparedQuery, contextV3_3)
          val logicalPlanStateV3_4 = helpers.as3_4(logicalPlanStateV3_3)
          // Here we switch from 3.3 to 3.4
          // TODO map 3.3 context to 3.4
          val result = createExecPlan.transform(logicalPlanStateV3_4, contextV3_4)
          result.maybeExecutionPlan.get
        }

        val executionPlan = if (preParsedQuery.debugOptions.isEmpty)
          cache.getOrElseUpdate(syntacticQuery.statement(), syntacticQuery.queryText, isStale, createPlan())._1
        else
          createPlan()

        // Log notifications/warnings from planning
        // TODO we don't need logging, do we?
        //executionPlan.notifications(planContext).foreach(notificationLogger.log)

        (new ExecutionPlanWrapper(executionPlan, preParsingNotifications, preParsedQuery.offset), preparedQuery.extractedParams())
      }

      override protected val trier = preparedSyntacticQueryForV_3_3
    }
  }

  protected def logStalePlanRemovalMonitor(log: InfoLogger) = new AstCacheMonitor {
    override def cacheDiscard(key: StatementV3_3, userKey: String) {
      log.info(s"Discarded stale query from the query cache: $userKey")
    }
  }

  private def provideCache(cacheAccessor: CacheAccessor[StatementV3_3, ExecutionPlan_v3_4],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[StatementV3_3, ExecutionPlan_v3_4]],
                           planContext: v3_3.spi.PlanContext,
                           planCacheFactory: () => LFUCache[StatementV3_3, ExecutionPlan_v3_4]): QueryCache[StatementV3_3, ExecutionPlan_v3_4] =
    planContext.getOrCreateFromSchemaState(cacheAccessor, {
      monitor.cacheFlushDetected(cacheAccessor)
      val lRUCache = planCacheFactory()
      new QueryCache(cacheAccessor, lRUCache)
    })

  class ExecutionPlanWrapper(inner: ExecutionPlan_v3_4, preParsingNotifications: Set[org.neo4j.graphdb.Notification], offset: InputPosition)
    extends ExecutionPlan {

    private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])

    private def queryContext(transactionalContext: TransactionalContextWrapper) = {
      val ctx = new TransactionBoundQueryContext(transactionalContext)(searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    def run(transactionalContext: TransactionalContextWrapper, executionMode: CypherExecutionMode,
            params: MapValue): Result = {
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainMode
        case CypherExecutionMode.profile => ProfileMode
        case CypherExecutionMode.normal => NormalMode
      }
      exceptionHandler.runSafely {

        val context = queryContext(transactionalContext)

        val innerResult: InternalExecutionResult = inner.run(context, innerExecutionMode, params)
        new ExecutionResult(new ClosingExecutionResult(
          transactionalContext.tc.executingQuery(),
          innerResult.withNotifications(preParsingNotifications.toSeq: _*),
          exceptionHandler.runSafely
        ))
      }
    }

    def isPeriodicCommit: Boolean = inner.isPeriodicCommit

    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapper): Boolean =
      inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(ctx.readOperations))

    override val plannerInfo: PlannerInfo = {
      new PlannerInfo(inner.plannerUsed.name, inner.runtimeUsed.name, inner.plannedIndexUsage.map {
        case SchemaIndexSeekUsage(identifier, labelId, label, propertyKeys) => schemaIndexUsage(identifier, labelId, label, propertyKeys: _*)
        case SchemaIndexScanUsage(identifier, labelId, label, propertyKey) => schemaIndexUsage(identifier, labelId, label, propertyKey)
        case ExplicitNodeIndexUsage(identifier, index) => explicitIndexUsage(identifier, "NODE", index)
        case ExplicitRelationshipIndexUsage(identifier, index) => explicitIndexUsage(identifier, "RELATIONSHIP", index)
      }.asJava)
    }
  }

}

object Compatibility {
  def createQueryGraphSolver(n: v3_3.CostBasedPlannerName, monitors: MonitorsV3_3,
                             config: v3_3.CypherCompilerConfiguration): logicalV3_3.QueryGraphSolver = n match {
    case v3_3.IDPPlannerName =>
      val monitor = monitors.newMonitor[idpV3_3.IDPQueryGraphSolverMonitor]()
      val solverConfig = new idpV3_3.ConfigurableIDPSolverConfig(
        maxTableSize = config.idpMaxTableSize,
        iterationDurationLimit = config.idpIterationDuration
      )
      val singleComponentPlanner = idpV3_3.SingleComponentPlanner(monitor, solverConfig)
      idpV3_3.IDPQueryGraphSolver(singleComponentPlanner, idpV3_3.cartesianProductsOrValueJoins, monitor)

    case v3_3.DPPlannerName =>
      val monitor = monitors.newMonitor[idpV3_3.IDPQueryGraphSolverMonitor]()
      val singleComponentPlanner = idpV3_3.SingleComponentPlanner(monitor, idpV3_3.DPSolverConfig)
      idpV3_3.IDPQueryGraphSolver(singleComponentPlanner, idpV3_3.cartesianProductsOrValueJoins, monitor)
  }
}

trait CypherCacheFlushingMonitor[T] {
  def cacheFlushDetected(justBeforeKey: T) {}
}

trait CypherCacheHitMonitor[T] {
  def cacheHit(key: T) {}

  def cacheMiss(key: T) {}

  def cacheDiscard(key: T, userKey: String) {}
}

trait InfoLogger {
  def info(message: String)
}

trait CypherCacheMonitor[T, E] extends CypherCacheHitMonitor[T] with CypherCacheFlushingMonitor[E]

trait AstCacheMonitor extends CypherCacheMonitor[StatementV3_3, CacheAccessor[StatementV3_3, ExecutionPlan_v3_4]]

class StringInfoLogger(log: Log) extends InfoLogger {
  def info(message: String) {
    log.info(message)
  }
}

