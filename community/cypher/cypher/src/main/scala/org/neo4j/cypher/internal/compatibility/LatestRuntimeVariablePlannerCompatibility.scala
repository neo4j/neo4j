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
package org.neo4j.cypher.internal.compatibility

import java.time.Clock

import org.neo4j.cypher._
import org.neo4j.cypher.exceptionHandler.RunSafely
import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.procs.ProcedureCallOrSchemaCommandExecutionPlanBuilder
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{ExecutionPlan => ExecutionPlan_v3_5}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.{CommunityRuntimeContext => CommunityRuntimeContextv3_5, _}
import org.neo4j.cypher.internal.compatibility.v3_5.{ExceptionTranslatingQueryContext, WrappedMonitors => WrappedMonitorsv3_5}
import org.neo4j.cypher.internal.compiler.v3_5._
import org.neo4j.cypher.internal.compiler.v3_5.phases.{CompilationContains, LogicalPlanState}
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp._
import org.opencypher.v9_0.frontend.phases._
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.planner.v3_5.spi.{CostBasedPlannerName, DPPlannerName, IDPPlannerName}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.runtime.{ExplainMode, InternalExecutionResult, NormalMode, ProfileMode}
import org.neo4j.cypher.internal.v3_5.logical.plans.{ExplicitNodeIndexUsage, ExplicitRelationshipIndexUsage, SchemaIndexScanUsage, SchemaIndexSeekUsage}
import org.neo4j.graphdb.Result
import org.neo4j.kernel.api.query.{ExplicitIndexUsage, PlannerInfo, SchemaIndexUsage}
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.logging.Log
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._

abstract class LatestRuntimeVariablePlannerCompatibility[CONTEXT3_4 <: CommunityRuntimeContextv3_5,
T <: Transformer[CONTEXT3_4, LogicalPlanState, CompilationState],
STATEMENT <: AnyRef](configv3_5: CypherCompilerConfiguration,
                     clock: Clock,
                     kernelMonitors: KernelMonitors,
                     log: Log,
                     planner: CypherPlanner,
                     runtime: CypherRuntime,
                     updateStrategy: CypherUpdateStrategy,
                     runtimeBuilder: RuntimeBuilder[T],
                     contextCreatorV3_5: ContextCreator[CONTEXT3_4],
                     txIdProvider: () => Long) {

  // abstract stuff
  protected val maybePlannerNamev3_5: Option[CostBasedPlannerName]
  protected val runSafelyDuringPlanning: RunSafely
  protected val runSafelyDuringRuntime: RunSafely

  def produceParsedQuery(preParsedQuery: PreParsedQuery, tracer: CompilationPhaseTracer,
                         preParsingNotifications: Set[org.neo4j.graphdb.Notification]): ParsedQuery

  // concrete stuff
  protected val logger: InfoLogger = new StringInfoLogger(log)
  protected val monitorsv3_5: Monitors = WrappedMonitorsv3_5(kernelMonitors)

  protected val cacheTracer: CacheTracer[STATEMENT] = monitorsv3_5.newMonitor[CacheTracer[STATEMENT]]("cypher3.4")

  protected lazy val planCache: AstQueryCache[STATEMENT] =
    new AstQueryCache(configv3_5.queryCacheSize,
                      cacheTracer,
                      clock,
                      configv3_5.statsDivergenceCalculator,
                      txIdProvider)

  protected def queryGraphSolverv3_5: QueryGraphSolver = LatestRuntimeVariablePlannerCompatibility.createQueryGraphSolver(
    maybePlannerNamev3_5.getOrElse(CostBasedPlannerName.default),
    monitorsv3_5,
    configv3_5)

  protected def createExecPlan: Transformer[CONTEXT3_4, LogicalPlanState, CompilationState] = {
    ProcedureCallOrSchemaCommandExecutionPlanBuilder andThen
      If((s: CompilationState) => s.maybeExecutionPlan.isFailure) {
        val maybeRuntimeName: Option[RuntimeName] = runtime match {
          case CypherRuntime.default => None
          case CypherRuntime.interpreted => Some(InterpretedRuntimeName)
          case CypherRuntime.slotted => Some(SlottedRuntimeName)
          case CypherRuntime.morsel => Some(MorselRuntimeName)
          case CypherRuntime.compiled => Some(CompiledRuntimeName)
        }
        runtimeBuilder.create(maybeRuntimeName, configv3_5.useErrorsOverWarnings).adds(CompilationContains[ExecutionPlan_v3_5])
      }
  }

  protected def logStalePlanRemovalMonitor(log: InfoLogger) = new CacheTracer[STATEMENT] {
    override def queryCacheStale(key: STATEMENT, secondsSinceReplan: Int, metaData: String) {
      log.info(s"Discarded stale query from the query cache after $secondsSinceReplan seconds: $metaData")
    }

    override def queryCacheHit(queryKey: STATEMENT, metaData: String): Unit = {}
    override def queryCacheMiss(queryKey: STATEMENT, metaData: String): Unit = {}
    override def queryCacheFlush(sizeOfCacheBeforeFlush: Long): Unit = {}
  }

  protected class ExecutionPlanWrapper(inner: ExecutionPlan_v3_5, preParsingNotifications: Set[org.neo4j.graphdb.Notification])
    extends ExecutionPlan {

    private val searchMonitor = kernelMonitors.newMonitor(classOf[IndexSearchMonitor])

    private def queryContext(transactionalContext: TransactionalContext) = {
      val ctx = new TransactionBoundQueryContext(TransactionalContextWrapper(transactionalContext))(searchMonitor)
      new ExceptionTranslatingQueryContext(ctx)
    }

    def run(transactionalContext: TransactionalContext, executionMode: CypherExecutionMode,
            params: MapValue): Result = {
      val innerExecutionMode = executionMode match {
        case CypherExecutionMode.explain => ExplainMode
        case CypherExecutionMode.profile => ProfileMode
        case CypherExecutionMode.normal => NormalMode
      }
      runSafelyDuringPlanning {

        val context = queryContext(transactionalContext)

        val innerResult: InternalExecutionResult = inner.run(context, innerExecutionMode, params)
        new ExecutionResult(new ClosingExecutionResult(
          transactionalContext.executingQuery(),
          innerResult.withNotifications(preParsingNotifications.toSeq: _*),
          runSafelyDuringRuntime
        )(kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])))
      }
    }

    def isPeriodicCommit: Boolean = inner.isPeriodicCommit

    def reusabilityState(lastCommittedTxId: () => Long, ctx: TransactionalContext): ReusabilityState = inner.reusability

    override val plannerInfo: PlannerInfo = {
      new PlannerInfo(inner.plannerUsed.name, inner.runtimeUsed.name, inner.plannedIndexUsage.map {
        case SchemaIndexSeekUsage(identifier, labelId, label, propertyKeys) => new SchemaIndexUsage(identifier, labelId, label, propertyKeys: _*)
        case SchemaIndexScanUsage(identifier, labelId, label, propertyKey) => new SchemaIndexUsage(identifier, labelId, label, propertyKey)
        case ExplicitNodeIndexUsage(identifier, index) => new ExplicitIndexUsage(identifier, "NODE", index)
        case ExplicitRelationshipIndexUsage(identifier, index) => new ExplicitIndexUsage(identifier, "RELATIONSHIP", index)
      }.asJava)
    }
  }

}

object LatestRuntimeVariablePlannerCompatibility {
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

trait CypherCacheFlushingMonitor {
  def cacheFlushDetected(sizeBeforeFlush: Long) {}
}

trait CypherCacheHitMonitor[T] {
  def cacheHit(key: T) {}
  def cacheMiss(key: T) {}
  def cacheDiscard(key: T, userKey: String, secondsSinceReplan: Int) {}
}

trait CypherCacheMonitor[T] extends CypherCacheHitMonitor[T] with CypherCacheFlushingMonitor
trait AstCacheMonitor[STATEMENT <: AnyRef] extends CypherCacheMonitor[STATEMENT]

trait InfoLogger {
  def info(message: String)
}

class StringInfoLogger(log: Log) extends InfoLogger {
  def info(message: String) {
    log.info(message)
  }
}
