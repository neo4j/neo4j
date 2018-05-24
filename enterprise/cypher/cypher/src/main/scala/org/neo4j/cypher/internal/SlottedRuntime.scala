/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compatibility.v3_5.runtime.SlotAllocation.PhysicalPlan
import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{PeriodicCommitInfo, ExecutionPlan => ExecutionPlan_V35}
import org.neo4j.cypher.internal.compatibility.{InterpretedRuntime, TemporaryRuntime}
import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.planner.CantCompileQueryException
import org.neo4j.cypher.internal.runtime.compiled.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeExecutionBuilderContext
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.runtime.slotted.{SlottedExecutionResultBuilderFactory, SlottedPipeBuilder}
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InternalExecutionResult, QueryContext}
import org.neo4j.cypher.internal.v3_5.logical.plans.{IndexUsage, LogicalPlan}
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.opencypher.v9_0.frontend.PlannerName

class SlottedRuntime extends TemporaryRuntime[EnterpriseRuntimeContext] {

  @throws[CantCompileQueryException]
  override def googldiblopp(state: LogicalPlanState, context: EnterpriseRuntimeContext): ExecutionPlan_V35 = {
    //    try {
    //      if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
    //        printPlanInfo(state)
    //      }

    val (logicalPlan, physicalPlan) = rewritePlan(context, state.logicalPlan, state.semanticTable())

    //      if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
    //        printRewrittenPlanInfo(logicalPlan)
    //      }

    val converters = new ExpressionConverters(SlottedExpressionConverters, CommunityExpressionConverter)
    val pipeBuilderFactory = SlottedPipeBuilder.Factory(physicalPlan)
    val executionPlanBuilder = new PipeExecutionPlanBuilder(expressionConverters = converters,
      pipeBuilderFactory = pipeBuilderFactory)
    val readOnly = state.solveds(state.logicalPlan.id).readOnly
    val pipeBuildContext = PipeExecutionBuilderContext(state.semanticTable(), readOnly, state.cardinalities)
    val pipe = executionPlanBuilder.build(logicalPlan)(pipeBuildContext, context.planContext)
    val periodicCommitInfo = state.periodicCommit.map(x => PeriodicCommitInfo(x.batchSize))
    val columns = state.statement().returnColumns
    val resultBuilderFactory =
      new SlottedExecutionResultBuilderFactory(pipe, readOnly, columns, logicalPlan, physicalPlan.slotConfigurations)
    val func = InterpretedRuntime.getExecutionPlanFunction(periodicCommitInfo,
      resultBuilderFactory,
      context.notificationLogger,
      state.plannerName,
      SlottedRuntimeName,
      readOnly,
      state.cardinalities)

    val fp = PlanFingerprint.take(context.clock, context.planContext.txIdProvider, context.planContext.statistics)
    val fingerprint = new PlanFingerprintReference(fp)
    val periodicCommit = periodicCommitInfo.isDefined
    val indexes = logicalPlan.indexUsage
    SlottedExecutionPlan(fingerprint, periodicCommit, state.plannerName, indexes, func)
    //
    //      if (ENABLE_DEBUG_PRINTS) {
    //        if (!PRINT_PLAN_INFO_EARLY) {
    //          // Print after execution plan building to see any occurring exceptions first
    //          printPlanInfo(state)
    //          printRewrittenPlanInfo(logicalPlan)
    //        }
    //        printPipe(physicalPlan.slotConfigurations, pipe)
    //      }
    //
    //      new CompilationState(state, Success(execPlan))
    //    } catch {
    //      case e: CypherException =>
    //        if (ENABLE_DEBUG_PRINTS) {
    //          printFailureStackTrace(e)
    //          if (!PRINT_PLAN_INFO_EARLY) {
    //            printPlanInfo(state)
    //          }
    //        }
    //        new CompilationState(state, Failure(e))
    //    }
  }

  private def rewritePlan(context: EnterpriseRuntimeContext, beforeRewrite: LogicalPlan, semanticTable: SemanticTable): (LogicalPlan, PhysicalPlan) = {
    val physicalPlan: PhysicalPlan = SlotAllocation.allocateSlots(beforeRewrite, semanticTable)
    val slottedRewriter = new SlottedRewriter(context.planContext)
    val logicalPlan = slottedRewriter(beforeRewrite, physicalPlan.slotConfigurations)
    (logicalPlan, physicalPlan)
  }

  case class SlottedExecutionPlan(fingerprint: PlanFingerprintReference,
                                  isPeriodicCommit: Boolean,
                                  plannerUsed: PlannerName,
                                  override val plannedIndexUsage: Seq[IndexUsage],
                                  runFunction: (QueryContext, ExecutionMode, MapValue) => InternalExecutionResult
                                 ) extends ExecutionPlan_V35 {

    override def run(queryContext: QueryContext, planType: ExecutionMode,
                     params: MapValue): InternalExecutionResult =
      runFunction(queryContext, planType, params)

    override def reusability: ReusabilityState = MaybeReusable(fingerprint)

    override def runtimeUsed: RuntimeName = SlottedRuntimeName
  }

}
