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
package org.neo4j.cypher.internal.compiler.v3_4

import java.time.Clock

import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.compiler.v3_4.phases.{CompilerContext, _}
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.debug.DebugPrinter
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter.PlanRewriter
import org.neo4j.cypher.internal.compiler.v3_4.planner.{CheckForUnresolvedTokens, ResolveTokens}
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters.ASTRewriter
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.phases._
import org.neo4j.cypher.internal.ir.v3_4.UnionQuery
import org.neo4j.cypher.internal.planner.v3_4.spi.{IDPPlannerName, PlannerNameFor}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

case class CypherCompiler[Context <: CompilerContext](astRewriter: ASTRewriter,
                                                      monitors: Monitors,
                                                      sequencer: String => RewriterStepSequencer,
                                                      metricsFactory: MetricsFactory,
                                                      config: CypherCompilerConfiguration,
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
    CompilationPhases.parsing(sequencer).transform(startState, context)
  }

  val prepareForCaching: Transformer[CompilerContext, BaseState, BaseState] =
    RewriteProcedureCalls andThen
    ProcedureDeprecationWarnings andThen
    ProcedureWarnings

  val irConstruction: Transformer[CompilerContext, BaseState, LogicalPlanState] =
    ResolveTokens andThen
      CreatePlannerQuery.adds(CompilationContains[UnionQuery]) andThen
      OptionalMatchRemover

  val costBasedPlanning: Transformer[CompilerContext, LogicalPlanState, LogicalPlanState] =
    QueryPlanner().adds(CompilationContains[LogicalPlan]) andThen
    PlanRewriter(sequencer) andThen
    If((s: LogicalPlanState) => s.unionQuery.readOnly) (
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

case class CypherCompilerConfiguration(queryCacheSize: Int,
                                       statsDivergenceCalculator: StatsDivergenceCalculator,
                                       useErrorsOverWarnings: Boolean,
                                       idpMaxTableSize: Int,
                                       idpIterationDuration: Long,
                                       errorIfShortestPathFallbackUsedAtRuntime: Boolean,
                                       errorIfShortestPathHasCommonNodesAtRuntime: Boolean,
                                       legacyCsvQuoteEscaping: Boolean,
                                       nonIndexedLabelWarningThreshold: Long)
