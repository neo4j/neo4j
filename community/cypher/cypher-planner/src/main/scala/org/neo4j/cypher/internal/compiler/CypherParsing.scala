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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseInternalSettings.ExtractLiteral
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper
import org.neo4j.cypher.internal.compiler.phases.BaseContextImpl
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.ParsingConfig
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.defaultSemanticFeatures
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.FrontEndCompilationPhases
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignatureResolver
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.options.CypherVersion
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlannerNameFor
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.values.virtual.MapValue

class CypherParsing(
  monitors: Monitors,
  config: CypherParsingConfig,
  internalSyntaxUsageStats: InternalSyntaxUsageStats
) {

  def parseQuery(
    queryText: String,
    rawQueryText: String,
    cypherVersion: CypherVersion,
    notificationLogger: InternalNotificationLogger,
    plannerNameText: String = IDPPlannerName.name,
    offset: Option[InputPosition],
    tracer: CompilationPhaseTracer,
    params: MapValue,
    cancellationChecker: CancellationChecker,
    resolver: Option[ProcedureSignatureResolver] = None,
    sessionDatabase: DatabaseReference
  ): BaseState = {
    val plannerName = PlannerNameFor(plannerNameText)
    val startState = InitialState(queryText, plannerName, new AnonymousVariableNameGenerator)
    val context = BaseContextImpl(
      tracer,
      notificationLogger,
      rawQueryText,
      offset,
      monitors,
      cancellationChecker,
      internalSyntaxUsageStats,
      sessionDatabase
    )
    val paramTypes = ParameterValueTypeHelper.asCypherTypeMap(params, config.useParameterSizeHint)

    val features = CypherParsingConfig.getEnabledFeatures(
      config.semanticFeatures,
      if (sessionDatabase == null) None else Some(sessionDatabase.isComposite),
      config.queryRouterEnabled,
      config.queryRouterForCompositeEnabled
    )
    CompilationPhases.parsing(
      ParsingConfig(
        cypherVersion = convertCypherVersion(cypherVersion),
        extractLiterals = config.extractLiterals,
        parameterTypeMapping = paramTypes,
        semanticFeatures = features,
        obfuscateLiterals = config.obfuscateLiterals(),
        antlrParserEnabled = config.cypherParserAntlrEnabled()
      ),
      resolver = resolver
    ).transform(startState, context)
  }

  def convertCypherVersion(version: CypherVersion): FrontEndCompilationPhases.CypherVersion = version match {
    case CypherVersion.default => FrontEndCompilationPhases.CypherVersion.Default
    case CypherVersion.cypher5 => FrontEndCompilationPhases.CypherVersion.Cypher5
  }
}

case class CypherParsingConfig(
  extractLiterals: ExtractLiteral = ExtractLiteral.ALWAYS,
  useParameterSizeHint: Boolean = true,
  semanticFeatures: Seq[SemanticFeature] = defaultSemanticFeatures,
  obfuscateLiterals: () => Boolean = () => false,
  cypherParserAntlrEnabled: () => Boolean = () => false,
  queryRouterEnabled: Boolean = true,
  queryRouterForCompositeEnabled: Boolean = false
)

object CypherParsingConfig {

  def fromCypherConfiguration(cypherConfiguration: CypherConfiguration): CypherParsingConfig = {
    def obfuscateLiterals(): Boolean = {
      // Is dynamic, but documented to not affect caching.
      cypherConfiguration.obfuscateLiterals
    }

    val extractLiterals: ExtractLiteral = {
      AssertMacros.checkOnlyWhenAssertionsAreEnabled(
        !GraphDatabaseInternalSettings.extract_literals.dynamic()
      )
      cypherConfiguration.extractLiterals
    }

    val useParameterSizeHint: Boolean = {
      AssertMacros.checkOnlyWhenAssertionsAreEnabled(
        !GraphDatabaseInternalSettings.cypher_size_hint_parameters.dynamic()
      )
      cypherConfiguration.useParameterSizeHint
    }

    val enabledSemanticFeatures: Seq[SemanticFeature] = {
      AssertMacros.checkOnlyWhenAssertionsAreEnabled(
        !GraphDatabaseInternalSettings.cypher_enable_extra_semantic_features.dynamic()
      )
      CompilationPhases.enabledSemanticFeatures(
        cypherConfiguration.enableExtraSemanticFeatures ++ cypherConfiguration.toggledFeatures(Map(
          GraphDatabaseInternalSettings.show_setting -> SemanticFeature.ShowSetting.productPrefix,
          GraphDatabaseInternalSettings.composable_commands -> SemanticFeature.ComposableCommands.productPrefix,
          GraphDatabaseInternalSettings.linked_users -> SemanticFeature.LinkedUsers.productPrefix
        ))
      )
    }

    def cypherParserAntlrEnabled(): Boolean = {
      // Is dynamic, note that it needs to be combined with clearing query caches to take effect.
      cypherConfiguration.cypherParserAntlrEnabled
    }

    val queryRouterEnabled: Boolean = cypherConfiguration.useQueryRouterForRegularQueries

    val queryRouterForCompositeQueriesEnabled: Boolean = cypherConfiguration.allowCompositeQueries

    CypherParsingConfig(
      extractLiterals,
      useParameterSizeHint,
      enabledSemanticFeatures,
      () => obfuscateLiterals(),
      () => cypherParserAntlrEnabled(),
      queryRouterEnabled,
      queryRouterForCompositeQueriesEnabled
    )
  }

  def getEnabledFeatures(
    semanticFeatures: Seq[SemanticFeature],
    targetsCompositeInQueryRouter: Option[Boolean],
    queryRouterEnabled: Boolean,
    queryRouterForCompositeEnabled: Boolean
  ): Seq[SemanticFeature] = {
    if (queryRouterEnabled && queryRouterForCompositeEnabled && targetsCompositeInQueryRouter.getOrElse(false))
      semanticFeatures ++ Seq(SemanticFeature.UseAsMultipleGraphsSelector, SemanticFeature.MultipleGraphs)
    else if (queryRouterEnabled)
      semanticFeatures ++ Seq(SemanticFeature.UseAsSingleGraphSelector)
    else
      semanticFeatures
  }
}
