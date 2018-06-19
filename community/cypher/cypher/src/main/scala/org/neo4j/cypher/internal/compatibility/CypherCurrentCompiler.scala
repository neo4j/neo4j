/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.cypher.exceptionHandler.runSafely
import org.neo4j.cypher.internal.compatibility.v3_5.ExceptionTranslatingQueryContext
import org.neo4j.cypher.{CypherException, CypherExecutionMode}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{ExecutionPlan => ExecutionPlan_v3_5}
import org.neo4j.cypher.internal.javacompat.ExecutionResult
import org.neo4j.cypher.internal.runtime.{ExplainMode, InternalExecutionResult, NormalMode, ProfileMode}
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.runtime.interpreted.{TransactionBoundQueryContext, TransactionalContextWrapper}
import org.neo4j.cypher.internal.v3_5.logical.plans.{ExplicitNodeIndexUsage, ExplicitRelationshipIndexUsage, SchemaIndexScanUsage, SchemaIndexSeekUsage}
import org.neo4j.cypher.internal._
import org.neo4j.graphdb.{Notification, Result}
import org.neo4j.kernel.api.query.{ExplicitIndexUsage, PlannerInfo, SchemaIndexUsage}
import org.neo4j.kernel.impl.query.{QueryExecutionMonitor, TransactionalContext}
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer

import scala.collection.JavaConverters._

/**
  * Composite compiler, which uses a CypherPlanner and CypherRuntime to compile
  * a preparsed query into a CacheableExecutableQuery.
  *
  * @param planner the planner
  * @param runtime the runtime
  * @param contextCreator the runtime context creator
  * @param kernelMonitors monitors support
  * @tparam CONTEXT type of runtime context used
  */
case class CypherCurrentCompiler[CONTEXT <: RuntimeContext](planner: CypherPlanner,
                                                            runtime: CypherRuntime[CONTEXT],
                                                            contextCreator: RuntimeContextCreator[CONTEXT],
                                                            kernelMonitors: KernelMonitors
                                                           ) extends org.neo4j.cypher.internal.Compiler {

  /**
    * Compile pre-parsed query into executable query.
    *
    * @param preParsedQuery          pre-parsed query to convert
    * @param tracer                  compilation tracer to which events of the compilation process are reported
    * @param preParsingNotifications notifications from pre-parsing
    * @param transactionalContext    transactional context to use during compilation (in logical and physical planning)
    * @throws CypherException public cypher exceptions on compilation problems
    * @return a compiled and executable query
    */
  override def compile(preParsedQuery: PreParsedQuery,
                       tracer: CompilationPhaseTracer,
                       preParsingNotifications: Set[Notification],
                       transactionalContext: TransactionalContext): ExecutableQuery = {

    val logicalPlanResult =
      planner.parseAndPlan(preParsedQuery, tracer, preParsingNotifications, transactionalContext)

    val logicalPlan = logicalPlanResult.logicalPlanState.logicalPlan

    val runtimeContext = contextCreator.create(logicalPlanResult.plannerContext.notificationLogger,
                                               logicalPlanResult.plannerContext.planContext,
                                               logicalPlanResult.plannerContext.clock,
                                               logicalPlanResult.plannerContext.debugOptions,
                                               logicalPlanResult.logicalPlanState.solveds(logicalPlan.id).readOnly)

    val executionPlan3_5 = runtime.compileToExecutable(logicalPlanResult.logicalPlanState, runtimeContext)

    new CypherExecutableQuery(
      executionPlan3_5,
      preParsingNotifications,
      logicalPlanResult.reusability,
      logicalPlanResult.paramNames,
      logicalPlanResult.extractedParams)
  }

  protected class CypherExecutableQuery(inner: ExecutionPlan_v3_5,
                                        preParsingNotifications: Set[org.neo4j.graphdb.Notification],
                                        reusabilityState: ReusabilityState,
                                        override val paramNames: Seq[String],
                                        override val extractedParams: MapValue) extends ExecutableQuery {

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
      runSafely {

        val context = queryContext(transactionalContext)

        val innerResult: InternalExecutionResult = inner.run(context, innerExecutionMode, params)
        new ExecutionResult(new ClosingExecutionResult(
          transactionalContext.executingQuery(),
          innerResult.withNotifications(preParsingNotifications.toSeq: _*),
          runSafely
        )(kernelMonitors.newMonitor(classOf[QueryExecutionMonitor])))
      }
    }

    def isPeriodicCommit: Boolean = inner.isPeriodicCommit

    def reusabilityState(lastCommittedTxId: () => Long, ctx: TransactionalContext): ReusabilityState = reusabilityState

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

