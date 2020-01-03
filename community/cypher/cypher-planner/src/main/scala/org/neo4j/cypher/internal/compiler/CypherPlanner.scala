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
package org.neo4j.cypher.internal.compiler

import java.time.Clock

import org.neo4j.cypher.internal.compiler.phases.CompilationPhases._
import org.neo4j.cypher.internal.compiler.phases._
import org.neo4j.cypher.internal.compiler.planner.logical._
import org.neo4j.cypher.internal.compiler.planner.logical.debug.DebugPrinter
import org.neo4j.cypher.internal.planner.spi.{IDPPlannerName, PlannerNameFor}
import org.neo4j.cypher.internal.v4_0.frontend.phases.{CompilationPhases => _, _}
import org.neo4j.cypher.internal.v4_0.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.InnerVariableNamer
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.values.virtual.MapValue

case class CypherPlanner[Context <: PlannerContext](monitors: Monitors,
                                                    sequencer: String => RewriterStepSequencer,
                                                    metricsFactory: MetricsFactory,
                                                    config: CypherPlannerConfiguration,
                                                    updateStrategy: UpdateStrategy,
                                                    clock: Clock,
                                                    contextCreation: ContextCreator[Context]) {

  def normalizeQuery(state: BaseState, context: Context): BaseState = prepareForCaching.transform(state, context)

  def planPreparedQuery(state: BaseState, context: Context): LogicalPlanState = {
    val pipeLine = if(config.planSystemCommands)
      systemPipeLine
    else if (context.debugOptions.contains("tostring"))
      planPipeLine(sequencer) andThen DebugPrinter
    else
      planPipeLine(sequencer)

    pipeLine.transform(state, context)
  }

  def parseQuery(queryText: String,
                 rawQueryText: String,
                 notificationLogger: InternalNotificationLogger,
                 plannerNameText: String = IDPPlannerName.name,
                 debugOptions: Set[String],
                 offset: Option[InputPosition],
                 tracer: CompilationPhaseTracer,
                 innerVariableNamer: InnerVariableNamer,
                 params: MapValue,
                 compatibilityMode: Boolean): BaseState = {

    val plannerName = PlannerNameFor(plannerNameText)
    val startState = InitialState(queryText, offset, plannerName)
    //TODO: these nulls are a short cut
    val context = contextCreation.create(tracer,
                                         notificationLogger,
                                         planContext = null,
                                         rawQueryText,
                                         debugOptions,
                                         offset,
                                         monitors,
                                         metricsFactory,
                                         null,
                                         config,
                                         updateStrategy,
                                         clock,
                                         logicalPlanIdGen = null,
                                         evaluator = null,
                                         innerVariableNamer = innerVariableNamer,
                                         params )
    CompilationPhases.parsing(sequencer, context.innerVariableNamer, compatibilityMode).transform(startState, context)
  }

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
                                      planSystemCommands: Boolean)
