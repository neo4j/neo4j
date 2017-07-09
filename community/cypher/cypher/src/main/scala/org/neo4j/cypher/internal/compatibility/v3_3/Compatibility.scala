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
import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.procs.ProcedureCallOrSchemaCommandExecutionPlanBuilder
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.executionplan.{ExecutionPlan => ExecutionPlan_v3_3}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.simpleExpressionEvaluator
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_3
import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.phases.{CompilationContains, LogicalPlanState}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.idp._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{LegacyNodeIndexUsage, LegacyRelationshipIndexUsage, SchemaIndexScanUsage, SchemaIndexSeekUsage}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{CachedMetricsFactory, QueryGraphSolver, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_3.phases._
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.spi.v3_3.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.v3_3._
import org.neo4j.graphdb.Result
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.api.query.IndexUsage.{legacyIndexUsage, schemaIndexUsage}
import org.neo4j.kernel.api.query.PlannerInfo
import org.neo4j.kernel.impl.query.QueryExecutionMonitor
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log

import scala.collection.JavaConverters._
import scala.util.Try

trait Compatibility[CONTEXT <: CommunityRuntimeContext,
                    T <: Transformer[CONTEXT, LogicalPlanState, CompilationState]] {
  val kernelMonitors: KernelMonitors
  val kernelAPI: KernelAPI
  val clock: Clock
  val monitors: Monitors
  val config: CypherCompilerConfiguration
  val logger: InfoLogger
  val runtimeBuilder: RuntimeBuilder[T]
  val contextCreator: ContextCreator[CONTEXT]
  val maybePlannerName: Option[CostBasedPlannerName]
  val maybeRuntimeName: Option[RuntimeName]
  val maybeUpdateStrategy: Option[UpdateStrategy]
  val cacheMonitor: AstCacheMonitor
  val cacheAccessor: MonitoringCacheAccessor[Statement, ExecutionPlan_v3_3]
  val monitorTag = "cypher3.3"

  protected val rewriterSequencer: (String) => RewriterStepSequencer = {
    import RewriterStepSequencer._
    import org.neo4j.helpers.Assertion._

    if (assertionsEnabled()) newValidating else newPlain
  }

  protected val compiler: v3_3.CypherCompiler[CONTEXT]

  private def queryGraphSolver = Compatibility.createQueryGraphSolver(maybePlannerName.getOrElse(CostBasedPlannerName.default), monitors, config)

  def createExecPlan: Transformer[CONTEXT, LogicalPlanState, CompilationState] = {
    ProcedureCallOrSchemaCommandExecutionPlanBuilder andThen
      If((s: CompilationState) => s.maybeExecutionPlan.isEmpty)(
        runtimeBuilder.create(maybeRuntimeName, config.useErrorsOverWarnings).adds(CompilationContains[ExecutionPlan])
      )
  }

  private val planCacheFactory = () => new LFUCache[Statement, ExecutionPlan_v3_3](config.queryCacheSize)

  implicit lazy val executionMonitor: QueryExecutionMonitor = kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])
  def produceParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer,
                         preParsingNotifications: Set[org.neo4j.graphdb.Notification]): ParsedQuery = {
    val notificationLogger = new RecordingNotificationLogger

    val preparedSyntacticQueryForV_3_2 =
      Try(compiler.parseQuery(preParsedQuery.statement,
                              preParsedQuery.rawStatement,
                              notificationLogger, preParsedQuery.planner.name,
                              preParsedQuery.debugOptions,
                              Some(preParsedQuery.offset), tracer))
    new ParsedQuery {
      override def plan(transactionalContext: TransactionalContextWrapper, tracer: CompilationPhaseTracer):
        (ExecutionPlan, Map[String, Any]) = exceptionHandler.runSafely {
        val syntacticQuery = preparedSyntacticQueryForV_3_2.get

        //Context used for db communication during planning
        val planContext = new ExceptionTranslatingPlanContext(new TransactionBoundPlanContext(transactionalContext, notificationLogger))
        //Context used to create logical plans
        val context = contextCreator.create(tracer, notificationLogger, planContext,
                                                        syntacticQuery.queryText, preParsedQuery.debugOptions,
                                                        Some(preParsedQuery.offset), monitors,
                                                        CachedMetricsFactory(SimpleMetricsFactory), queryGraphSolver,
                                                        config, maybeUpdateStrategy.getOrElse(defaultUpdateStrategy),
                                                        clock, simpleExpressionEvaluator)
        //Prepare query for caching
        val preparedQuery = compiler.normalizeQuery(syntacticQuery, context)
        val cache = provideCache(cacheAccessor, cacheMonitor, planContext, planCacheFactory)
        val isStale = (plan: ExecutionPlan_v3_3) => plan.isStale(planContext.txIdProvider, planContext.statistics)

        //Just in the case the query is not in the cache do we want to do the full planning + creating executable plan
        def createPlan(): ExecutionPlan_v3_3 = {
          val logicalPlanState = compiler.planPreparedQuery(preparedQuery, context)
          val result = createExecPlan.transform(logicalPlanState, context)
          result.maybeExecutionPlan.get
        }
        val executionPlan = if (preParsedQuery.debugOptions.isEmpty)
          cache.getOrElseUpdate(syntacticQuery.statement(), syntacticQuery.queryText, isStale, createPlan())._1
        else
          createPlan()

        // Log notifications/warnings from planning
       executionPlan.notifications(planContext).foreach(notificationLogger.log)

        (new ExecutionPlanWrapper(executionPlan, preParsingNotifications, preParsedQuery.offset), preparedQuery.extractedParams())
      }

      override protected val trier: Try[BaseState] = preparedSyntacticQueryForV_3_2
    }
  }

  protected def logStalePlanRemovalMonitor(log: InfoLogger) = new AstCacheMonitor {
    override def cacheDiscard(key: Statement, userKey: String) {
      log.info(s"Discarded stale query from the query cache: $userKey")
    }
  }

  private def provideCache(cacheAccessor: CacheAccessor[Statement, ExecutionPlan_v3_3],
                           monitor: CypherCacheFlushingMonitor[CacheAccessor[Statement, ExecutionPlan_v3_3]],
                           planContext: PlanContext,
                           planCacheFactory: () => LFUCache[Statement, ExecutionPlan_v3_3]): QueryCache[Statement, ExecutionPlan_v3_3] =
    planContext.getOrCreateFromSchemaState(cacheAccessor, {
      monitor.cacheFlushDetected(cacheAccessor)
      val lRUCache = planCacheFactory()
      new QueryCache(cacheAccessor, lRUCache)
    })

  class ExecutionPlanWrapper(inner: ExecutionPlan_v3_3, preParsingNotifications: Set[org.neo4j.graphdb.Notification], offset: InputPosition)
    extends ExecutionPlan {

    private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])

    private def queryContext(transactionalContext: TransactionalContextWrapper) = {
      val ctx = new TransactionBoundQueryContext(transactionalContext)(searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    def run(transactionalContext: TransactionalContextWrapper, executionMode: CypherExecutionMode,
            params: Map[String, Any]): Result = {
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
          innerResult.withNotifications(preParsingNotifications.toSeq:_*),
          exceptionHandler.runSafely
        ))
      }
    }

    def isPeriodicCommit: Boolean = inner.isPeriodicCommit

    def isStale(lastCommittedTxId: LastCommittedTxIdProvider, ctx: TransactionalContextWrapper): Boolean =
      inner.isStale(lastCommittedTxId, TransactionBoundGraphStatistics(ctx.readOperations))

    override def plannerInfo: PlannerInfo = {
      new PlannerInfo(inner.plannerUsed.name, inner.runtimeUsed.name, inner.plannedIndexUsage.map {
        case SchemaIndexSeekUsage(identifier, labelId, label, propertyKeys) => schemaIndexUsage(identifier, labelId, label, propertyKeys: _*)
        case SchemaIndexScanUsage(identifier, labelId, label, propertyKey) => schemaIndexUsage(identifier, labelId, label, propertyKey)
        case LegacyNodeIndexUsage(identifier, index) => legacyIndexUsage(identifier, "NODE", index)
        case LegacyRelationshipIndexUsage(identifier, index) => legacyIndexUsage(identifier, "RELATIONSHIP", index)
      }.asJava)
    }
  }
}

object Compatibility {
  def createQueryGraphSolver(n: CostBasedPlannerName, monitors: Monitors,
                             config: CypherCompilerConfiguration): QueryGraphSolver = n match {
    case IDPPlannerName =>
      val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
      val solverConfig = new ConfigurableIDPSolverConfig(
        maxTableSize = config.idpMaxTableSize,
        iterationDurationLimit = config.idpIterationDuration
      )
      val singleComponentPlanner = SingleComponentPlanner(monitor, solverConfig)
      IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)

    case DPPlannerName =>
      val monitor = monitors.newMonitor[IDPQueryGraphSolverMonitor]()
      val singleComponentPlanner = SingleComponentPlanner(monitor, DPSolverConfig)
      IDPQueryGraphSolver(singleComponentPlanner, cartesianProductsOrValueJoins, monitor)
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

trait AstCacheMonitor extends CypherCacheMonitor[Statement, CacheAccessor[Statement, ExecutionPlan_v3_3]]

class StringInfoLogger(log: Log) extends InfoLogger {
  def info(message: String) {
    log.info(message)
  }
}

