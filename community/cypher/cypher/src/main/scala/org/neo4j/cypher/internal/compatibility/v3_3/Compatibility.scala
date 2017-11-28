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
import org.neo4j.cypher.internal.compatibility.v3_3.helpers.as3_3
import org.neo4j.cypher.internal.compatibility.v3_4._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.procs.ProcedureCallOrSchemaCommandExecutionPlanBuilder
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan.{ExecutionPlan => ExecutionPlan_v3_4}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_3
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{idp => idpV3_3}
import org.neo4j.cypher.internal.compiler.v3_3.planner.{logical => logicalV3_3}
import org.neo4j.cypher.internal.compiler.v3_4._
import org.neo4j.cypher.internal.compiler.v3_4.phases.{CompilationContains, LogicalPlanState}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, Statement => StatementV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.phases
import org.neo4j.cypher.internal.frontend.v3_3.phases.{CompilationPhaseTracer => CompilationPhaseTracerV3_3, Monitors => MonitorsV3_3, RecordingNotificationLogger => RecordingNotificationLoggerV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.phases._
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{CommunityRuntimeContext => CommunityRuntimeContextV3_4}
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{CachedMetricsFactory, SimpleMetricsFactory}
import org.neo4j.cypher.internal.planner.v3_4.spi.CostBasedPlannerName
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
CONTEXT3_4 <: CommunityRuntimeContextV3_4,
T <: Transformer[CONTEXT3_4, LogicalPlanState, CompilationState]] {
  val kernelMonitors: KernelMonitors
  val clock: Clock
  val monitorsV3_3: MonitorsV3_3
  val monitorsV3_4: Monitors
  val configV3_4: CypherCompilerConfiguration
  val logger: InfoLogger
  val runtimeBuilder: RuntimeBuilder[T]
  val contextCreatorV3_3: v3_3.ContextCreator[CONTEXT3_3]
  val contextCreatorV3_4: ContextCreator[CONTEXT3_4]
  val maybePlannerName: Option[v3_3.CostBasedPlannerName]
  val maybeRuntimeName: Option[RuntimeName]
  val maybeUpdateStrategy: Option[v3_3.UpdateStrategy]
  val cacheMonitor: AstCacheMonitor
  val cacheAccessor: MonitoringCacheAccessor[StatementV3_3, ExecutionPlan_v3_4]
  val monitorTag = "cypher3.3"

  protected val rewriterSequencer: (String) => RewriterStepSequencer = {
    import RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v3_3.CypherCompiler[CONTEXT3_3]

  protected val configV3_3 = helpers.as3_3(configV3_4)


  private def queryGraphSolverV3_3 = Compatibility.createQueryGraphSolver(maybePlannerName.getOrElse(v3_3.CostBasedPlannerName.default), monitorsV3_3, configV3_3)
  private def queryGraphSolverV3_4 = compatibility.v3_4.Compatibility.createQueryGraphSolver(
    maybePlannerName.map(helpers.as3_4 _).getOrElse(CostBasedPlannerName.default).asInstanceOf[CostBasedPlannerName],
    monitorsV3_4,
    configV3_4)

  def createExecPlan: Transformer[CONTEXT3_4, LogicalPlanState, CompilationState] = {
      ProcedureCallOrSchemaCommandExecutionPlanBuilder andThen
      If((s: CompilationState) => s.maybeExecutionPlan.isEmpty)(
        runtimeBuilder.create(maybeRuntimeName, configV3_4.useErrorsOverWarnings).adds(CompilationContains[ExecutionPlan_v3_4])
      )
  }

  private val planCacheFactory = () => new LFUCache[StatementV3_3, ExecutionPlan_v3_4](configV3_4.queryCacheSize)

  implicit lazy val executionMonitor: QueryExecutionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])

  def produceParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer,
                         preParsingNotifications: Set[org.neo4j.graphdb.Notification]): ParsedQuery = {
    val inputPositionV3_3 = helpers.as3_3(preParsedQuery.offset)
    val inputPositionV3_4 = preParsedQuery.offset
    val notificationLoggerV3_3 = new RecordingNotificationLoggerV3_3(Some(inputPositionV3_3))
    val notificationLoggerV3_4 = new RecordingNotificationLogger(Some(inputPositionV3_4))

    val tracerV3_3 = as3_3(tracer)

    val preparedSyntacticQueryForV_3_3: Try[phases.BaseState] =
      Try(compiler.parseQuery(preParsedQuery.statement,
        preParsedQuery.rawStatement,
        notificationLoggerV3_3, preParsedQuery.planner.name,
        preParsedQuery.debugOptions,
        Some(helpers.as3_3(preParsedQuery.offset)), tracerV3_3))
    new ParsedQuery {
      override def plan(transactionalContext: TransactionalContextWrapper, tracerV3_4: CompilationPhaseTracer):
      (ExecutionPlan, Map[String, Any]) = exceptionHandler.runSafely {
        val syntacticQuery = preparedSyntacticQueryForV_3_3.get

        //Context used for db communication during planning
        val tcV3_3 = TransactionalContextWrapperV3_3(transactionalContext.tc)
        val tcV3_4 = TransactionalContextWrapper(transactionalContext.tc)
        val planContextV3_3 = new ExceptionTranslatingPlanContextV3_3(new TransactionBoundPlanContextV3_3(tcV3_3, notificationLoggerV3_3))
        val planContextV3_4 = new ExceptionTranslatingPlanContext(new TransactionBoundPlanContext(tcV3_4, notificationLoggerV3_4))

        // Only used during planning
        def simpleExpressionEvaluatorV3_3 = new logicalV3_3.ExpressionEvaluator {
          override def evaluateExpression(expr: Expression): Option[Any] = None
        }

        //Context used to create logical plans
        val contextV3_3: CONTEXT3_3 = contextCreatorV3_3.create(tracerV3_3, notificationLoggerV3_3, planContextV3_3,
          syntacticQuery.queryText, preParsedQuery.debugOptions,
          Some(inputPositionV3_3), monitorsV3_3,
          logicalV3_3.CachedMetricsFactory(logicalV3_3.SimpleMetricsFactory), queryGraphSolverV3_3,
          configV3_3, maybeUpdateStrategy.getOrElse(v3_3.defaultUpdateStrategy),
          clock, simpleExpressionEvaluatorV3_3)
        val contextV3_4: CONTEXT3_4 = contextCreatorV3_4.create(tracerV3_4, notificationLoggerV3_4, planContextV3_4,
          syntacticQuery.queryText, preParsedQuery.debugOptions,
          Some(inputPositionV3_4), monitorsV3_4,
          CachedMetricsFactory(SimpleMetricsFactory), queryGraphSolverV3_4,
          configV3_4, maybeUpdateStrategy.map(helpers.as3_4 _).getOrElse(defaultUpdateStrategy),
          clock, simpleExpressionEvaluator)

        //Prepare query for caching
        val preparedQuery = compiler.normalizeQuery(syntacticQuery, contextV3_3)
        val cache = provideCache(cacheAccessor, cacheMonitor, planContextV3_3, planCacheFactory)
        val statisticsV3_4 = GraphStatisticsWrapper(planContextV3_3.statistics)
        val isStale = (plan: ExecutionPlan_v3_4) => plan.isStale(planContextV3_3.txIdProvider, statisticsV3_4)

        //Just in the case the query is not in the cache do we want to do the full planning + creating executable plan
        def createPlan(): ExecutionPlan_v3_4 = {
          val logicalPlanStateV3_3 = compiler.planPreparedQuery(preparedQuery, contextV3_3)
          val logicalPlanStateV3_4 = helpers.as3_4(logicalPlanStateV3_3)
          // Here we switch from 3.3 to 3.4
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

trait InfoLogger {
  def info(message: String)
}

trait AstCacheMonitor extends CypherCacheMonitor[StatementV3_3, CacheAccessor[StatementV3_3, ExecutionPlan_v3_4]]

class StringInfoLogger(log: Log) extends InfoLogger {
  def info(message: String) {
    log.info(message)
  }
}

