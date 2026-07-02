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
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper
import org.neo4j.cypher.internal.compiler.phases.BaseContextImpl
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases.defaultSemanticFeatures
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.FrontEndCompilationPhases.settingToFeatureMapping
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStats
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.frontend.phases.ScopedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.StrictResolveCallables
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.macros.AssertMacros3
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.parser.v25.Cypher25ParserUtil
import org.neo4j.cypher.internal.parser.v5.Cypher5ParserUtil
import org.neo4j.cypher.internal.planner.spi.IDPPlannerName
import org.neo4j.cypher.internal.planner.spi.PlannerNameFor
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.values.virtual.MapValue

import java.util.function.Consumer

class CypherParsing(
  monitors: Monitors,
  config: CypherParsingConfig,
  internalSyntaxUsageStats: InternalUsageStats
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
    resolver: ScopedProcedureSignatureResolver,
    sessionDatabase: DatabaseReference,
    isScopeQuery: Boolean,
    shadowedFunctions: Set[String]
  ): BaseState = {
    val (startState, context, parsingConfig) = prepareParsingContext(
      queryText,
      rawQueryText,
      cypherVersion,
      notificationLogger,
      plannerNameText,
      offset,
      tracer,
      params,
      cancellationChecker,
      resolver,
      sessionDatabase,
      isScopeQuery,
      shadowedFunctions
    )
    CompilationPhases.parsing(parsingConfig, parameters = params)
      .transform(startState, context)
  }

  /**
   * Run the pre-obfuscator portion of the parse pipeline. Obfuscation metadata is collected in the
   * post portion, so the obfuscator should be wired from the state returned by
   * [[parseQueryPostObfuscator]] once the whole pipeline has succeeded, not from this one.
   */
  def parseQueryPreObfuscator(
    queryText: String,
    rawQueryText: String,
    cypherVersion: CypherVersion,
    notificationLogger: InternalNotificationLogger,
    plannerNameText: String = IDPPlannerName.name,
    offset: Option[InputPosition],
    tracer: CompilationPhaseTracer,
    params: MapValue,
    cancellationChecker: CancellationChecker,
    resolver: ScopedProcedureSignatureResolver,
    sessionDatabase: DatabaseReference,
    isScopeQuery: Boolean,
    shadowedFunctions: Set[String]
  ): (BaseState, BaseContext, ParsingConfig) = {
    val (startState, context, parsingConfig) = prepareParsingContext(
      queryText,
      rawQueryText,
      cypherVersion,
      notificationLogger,
      plannerNameText,
      offset,
      tracer,
      params,
      cancellationChecker,
      resolver,
      sessionDatabase,
      isScopeQuery,
      shadowedFunctions
    )
    val preState = CompilationPhases.parsingPre(parsingConfig)
      .transform(startState, context)
    (preState, context, parsingConfig)
  }

  /**
   * Run the post-obfuscator portion of the parse pipeline on a state produced by
   * [[parseQueryPreObfuscator]].
   */
  def parseQueryPostObfuscator(
    preState: BaseState,
    context: BaseContext,
    parsingConfig: ParsingConfig,
    params: MapValue
  ): BaseState =
    CompilationPhases.parsingPost(parsingConfig, parameters = params)
      .transform(preState, context)

  /**
   * Java-friendly variant of [[parseQuery]] that invokes `onObfuscatorReady` with the collected
   * [[ObfuscationMetadata]] only after the whole parse pipeline (including semantic analysis) has
   * succeeded. Lets callers wire the obfuscator onto an [[org.neo4j.kernel.api.query.ExecutingQuery]]
   * (or any other consumer); if parsing or semantic analysis throws, the obfuscator is never wired,
   * so the query text is withheld from the debug/query log under obfuscation.
   */
  def parseQueryWithObfuscatorCallback(
    queryText: String,
    rawQueryText: String,
    cypherVersion: CypherVersion,
    notificationLogger: InternalNotificationLogger,
    plannerNameText: String,
    offset: Option[InputPosition],
    tracer: CompilationPhaseTracer,
    params: MapValue,
    cancellationChecker: CancellationChecker,
    resolver: ScopedProcedureSignatureResolver,
    sessionDatabase: DatabaseReference,
    isScopeQuery: Boolean,
    shadowedFunctions: Set[String],
    onObfuscatorReady: Consumer[ObfuscationMetadata]
  ): BaseState = {
    val (preState, context, parsingConfig) = parseQueryPreObfuscator(
      queryText,
      rawQueryText,
      cypherVersion,
      notificationLogger,
      plannerNameText,
      offset,
      tracer,
      params,
      cancellationChecker,
      resolver,
      sessionDatabase,
      isScopeQuery,
      shadowedFunctions
    )
    val postState = parseQueryPostObfuscator(preState, context, parsingConfig, params)
    onObfuscatorReady.accept(postState.maybeObfuscationMetadata.getOrElse(ObfuscationMetadata.empty()))
    postState
  }

  private def prepareParsingContext(
    queryText: String,
    rawQueryText: String,
    cypherVersion: CypherVersion,
    notificationLogger: InternalNotificationLogger,
    plannerNameText: String,
    offset: Option[InputPosition],
    tracer: CompilationPhaseTracer,
    params: MapValue,
    cancellationChecker: CancellationChecker,
    resolver: ScopedProcedureSignatureResolver,
    sessionDatabase: DatabaseReference,
    isScopeQuery: Boolean,
    shadowedFunctions: Set[String]
  ): (BaseState, BaseContext, ParsingConfig) = {
    val plannerName = PlannerNameFor(plannerNameText)
    val startState = InitialState(queryText, plannerName, new AnonymousVariableNameGenerator)

    val semanticFeatures = CypherParsingConfig.getEnabledFeatures(
      config.semanticFeatures,
      sessionDatabase != null && sessionDatabase.isComposite,
      config.queryRouterForCompositeEnabled
    )
    val context = BaseContextImpl(
      cypherVersion,
      tracer,
      notificationLogger,
      rawQueryText,
      offset,
      monitors,
      cancellationChecker,
      internalSyntaxUsageStats,
      sessionDatabase,
      semanticFeatures,
      isScopeQuery,
      shadowedFunctions,
      isDebugSession = false
    )
    val paramTypes = ParameterValueTypeHelper.asCypherTypeMap(params, config.useParameterSizeHint)

    val parsingConfig = ParsingConfig(
      resolveCallables = StrictResolveCallables(resolver),
      extractLiterals = config.extractLiterals,
      parameterTypeMapping = paramTypes,
      resolveSimpleDynamicExpressions = config.resolveSimpleDynamicExpressions,
      enabledVirtualGraph = config.useVirtualGraph
    )
    (startState, context, parsingConfig)
  }

  /*
   * The DFA cache is an internal ANTLR cache, of which it exist one instance per Cypher version.
   * This cache grow significantly when parsing a lot of queries covering large part of the language,
   * in worst case leading to OOM of the Java heap.
   */
  def clearDFACaches(): Unit = {
    Cypher5ParserUtil.clearDFACache()
    Cypher25ParserUtil.clearDFACache()
  }
}

case class CypherParsingConfig(
  extractLiterals: ExtractLiteral = ExtractLiteral.ALWAYS,
  useParameterSizeHint: Boolean = true,
  semanticFeatures: Seq[SemanticFeature] = defaultSemanticFeatures.map(SemanticFeature.fromString),
  obfuscateLiterals: () => Boolean = () => false,
  queryRouterForCompositeEnabled: Boolean = false,
  resolveSimpleDynamicExpressions: Boolean = false,
  useVirtualGraph: Boolean = false,
  exposeFullyObfuscatedQueryView: Boolean = true
)

object CypherParsingConfig {

  def fromCypherConfiguration(cypherConfiguration: CypherConfiguration): CypherParsingConfig = {
    val extractLiterals: ExtractLiteral = {
      AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
        !GraphDatabaseInternalSettings.extract_literals.dynamic()
      )
      cypherConfiguration.extractLiterals
    }

    val useParameterSizeHint: Boolean = {
      AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
        !GraphDatabaseInternalSettings.cypher_size_hint_parameters.dynamic()
      )
      cypherConfiguration.useParameterSizeHint
    }

    val resolveSimpleDynamicExpressions: Boolean = {
      AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
        !GraphDatabaseInternalSettings.resolve_simple_dynamic_expressions.dynamic()
      )
      cypherConfiguration.resolveSimpleDynamicExpressions
    }

    val enabledSemanticFeatures: Seq[SemanticFeature] = {
      AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
        !GraphDatabaseInternalSettings.cypher_enable_extra_semantic_features.dynamic()
      )
      CompilationPhases.enabledSemanticFeatures(
        cypherConfiguration.enableExtraSemanticFeatures ++ cypherConfiguration.toggledFeatures(
          defaultSemanticFeatures,
          settingToFeatureMapping: _*
        )
      )
    }

    val enabledVirtualGraph: Boolean = cypherConfiguration.useVirtualGraph

    val exposeFullyObfuscatedQueryView: Boolean = {
      AssertMacros3.checkOnlyWhenAssertionsAreEnabled(
        !GraphDatabaseInternalSettings.expose_fully_obfuscated_query_view.dynamic()
      )
      cypherConfiguration.exposeFullyObfuscatedQueryView
    }

    CypherParsingConfig(
      extractLiterals,
      useParameterSizeHint,
      enabledSemanticFeatures,
      () => cypherConfiguration.obfuscateLiterals,
      cypherConfiguration.allowCompositeQueries,
      resolveSimpleDynamicExpressions,
      enabledVirtualGraph,
      exposeFullyObfuscatedQueryView
    )
  }

  def getEnabledFeatures(
    semanticFeatures: Seq[SemanticFeature],
    targetsCompositeInQueryRouter: Boolean,
    queryRouterForCompositeEnabled: Boolean
  ): Seq[SemanticFeature] = {
    if (queryRouterForCompositeEnabled && targetsCompositeInQueryRouter)
      semanticFeatures ++ Seq(SemanticFeature.UseAsMultipleGraphsSelector, SemanticFeature.MultipleGraphs)
    else
      semanticFeatures ++ Seq(SemanticFeature.UseAsSingleGraphSelector)
  }
}
