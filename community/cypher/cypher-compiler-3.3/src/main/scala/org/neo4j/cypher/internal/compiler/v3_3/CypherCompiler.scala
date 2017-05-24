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
package org.neo4j.cypher.internal.compiler.v3_3

import java.time.Clock

import org.neo4j.cypher.internal.compiler.v3_3.phases.{CompilerContext, _}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.rewriter.PlanRewriter
import org.neo4j.cypher.internal.compiler.v3_3.planner.{CheckForUnresolvedTokens, ResolveTokens}
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.ASTRewriter
import org.neo4j.cypher.internal.frontend.v3_3.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_3.phases._
import org.neo4j.cypher.internal.ir.v3_3.UnionQuery

case class CypherCompiler[Context <: CompilerContext](astRewriter: ASTRewriter,
                                                      monitors: Monitors,
                                                      sequencer: String => RewriterStepSequencer,
                                                      metricsFactory: MetricsFactory,
                                                      config: CypherCompilerConfiguration,
                                                      updateStrategy: UpdateStrategy,
                                                      clock: Clock,
                                                      contextCreation: ContextCreator[Context]) {
  def normalizeQuery(state: BaseState, context: Context): BaseState = prepareForCaching.transform(state, context)

  def planPreparedQuery(state: BaseState,
                        context: Context): LogicalPlanState = planPipeLine.transform(state, context)


  def parseQuery(queryText: String,
                 rawQueryText: String,
                 notificationLogger: InternalNotificationLogger,
                 plannerNameText: String = IDPPlannerName.name,
                 debugOptions: Set[String],
                 offset: Option[InputPosition],
                 tracer: CompilationPhaseTracer): BaseState = {
    val plannerName = PlannerNameFor(plannerNameText)
    val startState = LogicalPlanState(queryText, offset, plannerName)
    //TODO: these nulls are a short cut
    val context = contextCreation.create(tracer, notificationLogger, planContext = null, rawQueryText, debugOptions,
      offset, monitors, metricsFactory, null, config, updateStrategy, clock, evaluator = null)
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
                                       statsDivergenceThreshold: Double,
                                       queryPlanTTL: Long,
                                       useErrorsOverWarnings: Boolean,
                                       idpMaxTableSize: Int,
                                       idpIterationDuration: Long,
                                       errorIfShortestPathFallbackUsedAtRuntime: Boolean,
                                       errorIfShortestPathHasCommonNodesAtRuntime: Boolean,
                                       legacyCsvQuoteEscaping: Boolean,
                                       nonIndexedLabelWarningThreshold: Long)
