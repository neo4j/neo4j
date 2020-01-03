/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_5.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.v3_5.phases._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.debug.DebugPrinter
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.plans.rewriter.PlanRewriter
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps.replacePropertyLookupsWithVariables
import org.neo4j.cypher.internal.compiler.v3_5.planner.CheckForUnresolvedTokens
import org.neo4j.cypher.internal.compiler.v3_5.planner.ResolveTokens
import org.neo4j.cypher.internal.ir.v3_5.UnionQuery
import org.neo4j.cypher.internal.planner.v3_5.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.v3_5.spi.PlannerNameFor
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_5.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.v3_5.frontend.phases._
import org.neo4j.cypher.internal.v3_5.rewriting.{Deprecations, RewriterStepSequencer}
import org.neo4j.cypher.internal.v3_5.util.InputPosition

case class CypherPlanner[Context <: PlannerContext](monitors: Monitors,
                                                    sequencer: String => RewriterStepSequencer,
                                                    metricsFactory: MetricsFactory,
                                                    config: CypherPlannerConfiguration,
                                                    updateStrategy: UpdateStrategy,
                                                    clock: Clock,
                                                    contextCreation: ContextCreator[Context]) {

  def normalizeQuery(state: BaseState, context: Context): BaseState = prepareForCaching.transform(state, context)

  def planPreparedQuery(state: BaseState, context: Context): LogicalPlanState = {
    val pipeLine = if (context.debugOptions.contains("tostring"))
      planPipeLine andThen DebugPrinter
    else
      planPipeLine

    pipeLine.transform(state, context)
  }

  def parseQuery(queryText: String,
                 rawQueryText: String,
                 notificationLogger: InternalNotificationLogger,
                 plannerNameText: String = IDPPlannerName.name,
                 debugOptions: Set[String],
                 offset: Option[InputPosition],
                 tracer: CompilationPhaseTracer): BaseState = {
    val plannerName = PlannerNameFor(plannerNameText)
    val startState = InitialState(queryText, offset, plannerName)
    //TODO: these nulls are a short cut
    val context = contextCreation.create(tracer, notificationLogger, planContext = null, rawQueryText, debugOptions,
      offset, monitors, metricsFactory, null, config, updateStrategy, clock, logicalPlanIdGen = null, evaluator = null)
    CompilationPhases.parsing(sequencer, deprecations = Deprecations.V2).transform(startState, context)
  }

  val prepareForCaching: Transformer[PlannerContext, BaseState, BaseState] =
    RewriteProcedureCalls andThen
    ProcedureDeprecationWarnings andThen
    ProcedureWarnings

  val irConstruction: Transformer[PlannerContext, BaseState, LogicalPlanState] =
    ResolveTokens andThen
      CreatePlannerQuery.adds(CompilationContains[UnionQuery]) andThen
      OptionalMatchRemover

  val costBasedPlanning: Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] =
    QueryPlanner().adds(CompilationContains[LogicalPlan]) andThen
      PlanRewriter(sequencer) andThen
      replacePropertyLookupsWithVariables andThen
      If((s: LogicalPlanState) => s.unionQuery.readOnly)(
        CheckForUnresolvedTokens
      )

  val standardPipeline: Transformer[Context, BaseState, LogicalPlanState] =
    CompilationPhases.lateAstRewriting andThen
    irConstruction andThen
    costBasedPlanning

  val planPipeLine: Transformer[Context, BaseState, LogicalPlanState] =
    ProcedureCallOrSchemaCommandPlanBuilder andThen
    If((s: LogicalPlanState) => s.maybeLogicalPlan.isEmpty)(
      standardPipeline
    )
}

case class CypherPlannerConfiguration(queryCacheSize: Int,
                                      statsDivergenceCalculator: StatsDivergenceCalculator,
                                      useErrorsOverWarnings: Boolean,
                                      idpMaxTableSize: Int,
                                      idpIterationDuration: Long,
                                      errorIfShortestPathFallbackUsedAtRuntime: Boolean,
                                      errorIfShortestPathHasCommonNodesAtRuntime: Boolean,
                                      legacyCsvQuoteEscaping: Boolean,
                                      csvBufferSize: Int,
                                      nonIndexedLabelWarningThreshold: Long,
                                      planWithMinimumCardinalityEstimates: Boolean,
                                      lenientCreateRelationship: Boolean)
