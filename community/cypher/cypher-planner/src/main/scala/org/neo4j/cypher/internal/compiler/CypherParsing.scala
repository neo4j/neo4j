/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler

import org.neo4j.configuration.GraphDatabaseInternalSettings.ExtractLiteral
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper
import org.neo4j.cypher.internal.compiler.helpers.SignatureResolver
import org.neo4j.cypher.internal.compiler.phases.BaseContextImpl
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.ParsingConfig
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.defaultSemanticFeatures
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlannerNameFor
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.values.virtual.MapValue

class CypherParsing(
  monitors: Monitors,
  config: CypherParsingConfig
) {

  def parseQuery(
    queryText: String,
    rawQueryText: String,
    notificationLogger: InternalNotificationLogger,
    plannerNameText: String = IDPPlannerName.name,
    offset: Option[InputPosition],
    tracer: CompilationPhaseTracer,
    params: MapValue,
    cancellationChecker: CancellationChecker
  ): BaseState = {
    val plannerName = PlannerNameFor(plannerNameText)
    val startState = InitialState(queryText, offset, plannerName, new AnonymousVariableNameGenerator)
    val context = BaseContextImpl(tracer, notificationLogger, rawQueryText, offset, monitors, cancellationChecker)
    val paramTypes = ParameterValueTypeHelper.asCypherTypeMap(params, config.useParameterSizeHint())
    CompilationPhases.parsing(ParsingConfig(
      extractLiterals = config.extractLiterals(),
      parameterTypeMapping = paramTypes,
      semanticFeatures = config.semanticFeatures(),
      obfuscateLiterals = config.obfuscateLiterals()
    )).transform(startState, context)
  }

  def queryRouterParseQuery(
    queryText: String,
    rawQueryText: String,
    notificationLogger: InternalNotificationLogger,
    plannerNameText: String = IDPPlannerName.name,
    offset: Option[InputPosition],
    tracer: CompilationPhaseTracer,
    params: MapValue,
    cancellationChecker: CancellationChecker,
    globalProcedures: GlobalProcedures
  ): BaseState = {
    val plannerName = PlannerNameFor(plannerNameText)
    val startState = InitialState(queryText, offset, plannerName, new AnonymousVariableNameGenerator)
    val context = BaseContextImpl(tracer, notificationLogger, rawQueryText, offset, monitors, cancellationChecker)
    val paramTypes = ParameterValueTypeHelper.asCypherTypeMap(params, config.useParameterSizeHint())

    CompilationPhases.fabricParsing(
      ParsingConfig(
        extractLiterals = config.extractLiterals(),
        parameterTypeMapping = paramTypes,
        semanticFeatures = config.semanticFeatures(),
        obfuscateLiterals = config.obfuscateLiterals()
      ),
      SignatureResolver.from(globalProcedures.getCurrentView)
    ).transform(startState, context)
  }

}

case class CypherParsingConfig(
  extractLiterals: () => ExtractLiteral = () => ExtractLiteral.ALWAYS,
  useParameterSizeHint: () => Boolean = () => true,
  semanticFeatures: () => Seq[SemanticFeature] = () => defaultSemanticFeatures,
  obfuscateLiterals: () => Boolean = () => false
) {

  def this() = this(
    () => ExtractLiteral.ALWAYS,
    () => true,
    () => defaultSemanticFeatures,
    () => false
  )

  def withUseGraphSelector(): CypherParsingConfig =
    withSemanticFeature(SemanticFeature.UseAsMultipleGraphsSelector)

  private def withSemanticFeature(feature: SemanticFeature): CypherParsingConfig =
    copy(semanticFeatures = () => semanticFeatures() :+ feature)
}

object CypherParsingConfig {

  def fromCypherPlannerConfiguration(plannerConfiguration: CypherPlannerConfiguration): CypherParsingConfig =
    CypherParsingConfig(
      extractLiterals = plannerConfiguration.extractLiterals,
      useParameterSizeHint = plannerConfiguration.useParameterSizeHint,
      semanticFeatures = plannerConfiguration.enabledSemanticFeatures,
      obfuscateLiterals = plannerConfiguration.obfuscateLiterals
    )
}
