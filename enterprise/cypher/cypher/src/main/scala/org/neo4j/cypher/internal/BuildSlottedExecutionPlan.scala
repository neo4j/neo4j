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
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.{ExecutionPlan => RuntimeExecutionPlan, _}
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.phases.CompilationState
import org.neo4j.cypher.internal.compiler.v3_5.phases.{CompilationContains, LogicalPlanState}
import org.opencypher.v9_0.frontend.PlannerName
import org.opencypher.v9_0.frontend.phases.CompilationPhaseTracer.CompilationPhase.PIPE_BUILDING
import org.opencypher.v9_0.frontend.phases.{CompilationPhaseTracer, Phase}
import org.opencypher.v9_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.planner.v3_5.spi.GraphStatistics
import org.neo4j.cypher.internal.runtime.compiled.EnterpriseRuntimeContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{Pipe, PipeExecutionBuilderContext}
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedExpressionConverters
import org.neo4j.cypher.internal.runtime.slotted.{SlottedExecutionResultBuilderFactory, SlottedPipeBuilder}
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InternalExecutionResult, QueryContext}
import org.opencypher.v9_0.util.CypherException
import org.neo4j.cypher.internal.v3_5.logical.plans.{IndexUsage, LogicalPlan}
import org.neo4j.values.virtual.MapValue

import scala.util.{Failure, Success}

object BuildSlottedExecutionPlan extends Phase[EnterpriseRuntimeContext, LogicalPlanState, CompilationState] with DebugPrettyPrinter {
  val ENABLE_DEBUG_PRINTS = false // NOTE: false toggles all debug prints off, overriding the individual settings below

  // Should we print query text and logical plan before we see any exceptions from execution plan building?
  // Setting this to true is useful if you want to see the query and logical plan while debugging a failure
  // Setting this to false is useful if you want to quickly spot the failure reason at the top of the output from tests
  val PRINT_PLAN_INFO_EARLY = true

  override val PRINT_QUERY_TEXT = true
  override val PRINT_LOGICAL_PLAN = true
  override val PRINT_REWRITTEN_LOGICAL_PLAN = true
  override val PRINT_PIPELINE_INFO = true
  override val PRINT_FAILURE_STACK_TRACE = true

  override def phase: CompilationPhaseTracer.CompilationPhase = PIPE_BUILDING

  override def description = "create slotted execution plan"

  override def postConditions = Set(CompilationContains[ExecutionPlan])

  override def process(from: LogicalPlanState, context: EnterpriseRuntimeContext): CompilationState = {
    createSlottedRuntimeExecPlan(from, context)
  }

  private def createSlottedRuntimeExecPlan(from: LogicalPlanState, context: EnterpriseRuntimeContext) = {
    try {
      if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
        printPlanInfo(from)
      }

      val (logicalPlan, physicalPlan) = rewritePlan(context, from.logicalPlan, from.semanticTable())

      if (ENABLE_DEBUG_PRINTS && PRINT_PLAN_INFO_EARLY) {
        printRewrittenPlanInfo(logicalPlan)
      }

      val converters = new ExpressionConverters(SlottedExpressionConverters, CommunityExpressionConverter)
      val pipeBuilderFactory = SlottedPipeBuilder.Factory(physicalPlan)
      val executionPlanBuilder = new PipeExecutionPlanBuilder(expressionConverters = converters,
                                                              pipeBuilderFactory = pipeBuilderFactory)
      val readOnly = from.solveds(from.logicalPlan.id).readOnly
      val pipeBuildContext = PipeExecutionBuilderContext(from.semanticTable(), readOnly, from.cardinalities)
      val pipe = executionPlanBuilder.build(logicalPlan)(pipeBuildContext, context.planContext)
      val periodicCommitInfo = from.periodicCommit.map(x => PeriodicCommitInfo(x.batchSize))
      val columns = from.statement().returnColumns
      val resultBuilderFactory =
        new SlottedExecutionResultBuilderFactory(pipe, readOnly, columns, logicalPlan, physicalPlan.slotConfigurations)
      val func = BuildInterpretedExecutionPlan.getExecutionPlanFunction(periodicCommitInfo,
                                                                        resultBuilderFactory,
                                                                        context.notificationLogger,
                                                                        from.plannerName,
                                                                        SlottedRuntimeName,
                                                                        readOnly,
                                                                        from.cardinalities)

      val fp = PlanFingerprint.take(context.clock, context.planContext.txIdProvider, context.planContext.statistics)
      val fingerprint = new PlanFingerprintReference(fp)
      val periodicCommit = periodicCommitInfo.isDefined
      val indexes = logicalPlan.indexUsage
      val execPlan = SlottedExecutionPlan(fingerprint, periodicCommit, from.plannerName, indexes, func)

      if (ENABLE_DEBUG_PRINTS) {
        if (!PRINT_PLAN_INFO_EARLY) {
          // Print after execution plan building to see any occurring exceptions first
          printPlanInfo(from)
          printRewrittenPlanInfo(logicalPlan)
        }
        printPipe(physicalPlan.slotConfigurations, pipe)
      }

      new CompilationState(from, Success(execPlan))
    } catch {
      case e: CypherException =>
        if (ENABLE_DEBUG_PRINTS) {
          printFailureStackTrace(e)
          if (!PRINT_PLAN_INFO_EARLY) {
            printPlanInfo(from)
          }
        }
        new CompilationState(from, Failure(e))
    }
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
                                 ) extends RuntimeExecutionPlan {

    override def run(queryContext: QueryContext, planType: ExecutionMode,
                     params: MapValue): InternalExecutionResult =
      runFunction(queryContext, planType, params)

    override def reusability: ReusabilityState = MaybeReusable(fingerprint)

    override def runtimeUsed: RuntimeName = SlottedRuntimeName
  }
}
