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
package org.neo4j.fabric.pipeline

import org.neo4j.cypher.internal.CachingPreParser
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.MultipleGraphs
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.UseAsMultipleGraphsSelector
import org.neo4j.cypher.internal.cache.CacheSize
import org.neo4j.cypher.internal.cache.CacheTracer
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CacheStrategy
import org.neo4j.cypher.internal.cache.CypherQueryCaches.PreParserCache
import org.neo4j.cypher.internal.compiler.helpers.ParameterValueTypeHelper
import org.neo4j.cypher.internal.compiler.phases.BaseContextImpl
import org.neo4j.cypher.internal.compiler.phases.CompilationPhases
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.FrontEndCompilationPhases.defaultSemanticFeatures
import org.neo4j.cypher.internal.frontend.phases.FrontEndCompilationPhases.settingToFeatureMapping
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStats
import org.neo4j.cypher.internal.frontend.phases.ScopedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.TryResolveCallables
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.planner.spi.PlannerNameFor
import org.neo4j.cypher.internal.planning.WrappedMonitors
import org.neo4j.cypher.internal.preparser.PreParsedQuery
import org.neo4j.cypher.internal.preparser.QueryOptions
import org.neo4j.cypher.internal.tracing.CompilationTracer
import org.neo4j.cypher.internal.tracing.TimingCompilationTracer
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.rendering.QueryRenderer
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.fabric.planning.FabricPlan
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.monitoring
import org.neo4j.values.virtual.MapValue

case class FabricFrontEnd(
  cypherConfig: CypherConfiguration,
  compositeProfilingEnabled: () => Boolean,
  kernelMonitors: monitoring.Monitors,
  cacheFactory: CaffeineCacheFactory
) {

  val compilationTracer = new TimingCompilationTracer(
    kernelMonitors.newMonitor(classOf[TimingCompilationTracer.EventListener])
  )

  object preParsing {

    private val preParser = new CachingPreParser(
      cypherConfig,
      new PreParserCache.Cache(
        cacheFactory,
        CacheSize.Dynamic(cypherConfig.queryCacheSize),
        new CacheTracer[CypherQueryCaches.PreParserCache.Key] {}
      )
    )

    def executionType(options: QueryOptions, inCompositeContext: Boolean): FabricPlan.ExecutionType =
      options.queryOptions.executionMode match {
        case CypherExecutionMode.default => FabricPlan.Execute
        case CypherExecutionMode.explain => FabricPlan.Explain
        case CypherExecutionMode.profile if inCompositeContext && !compositeProfilingEnabled.apply() =>
          throw InvalidSemanticsException.profileNotSupportedOnComposite()
        case CypherExecutionMode.profile => FabricPlan.PROFILE
      }

    def preParse(
      queryString: String,
      notificationLogger: InternalNotificationLogger,
      defaultLanguage: CypherVersion,
      cacheStrategy: CacheStrategy
    ): PreParsedQuery = {
      preParser.preParseQuery(queryString, notificationLogger, defaultLanguage, cacheStrategy = cacheStrategy)
    }

  }

  case class Pipeline(
    signatures: ScopedProcedureSignatureResolver,
    query: PreParsedQuery,
    params: MapValue,
    cancellationChecker: CancellationChecker,
    notificationLogger: InternalNotificationLogger,
    internalSyntaxUsageStats: InternalUsageStats,
    sessionDatabase: DatabaseReference,
    shadowedFunctions: Set[String]
  ) {

    def traceStart(): CompilationTracer.QueryCompilationEvent =
      compilationTracer.compileQuery(query.description)

    val semanticFeatures: Seq[SemanticFeature] = CompilationPhases.enabledSemanticFeatures(
      cypherConfig.enableExtraSemanticFeatures ++
        cypherConfig.toggledFeatures(defaultSemanticFeatures, settingToFeatureMapping: _*)
    ) ++ Seq(MultipleGraphs, UseAsMultipleGraphsSelector)

    private val parsingConfig = ParsingConfig(
      resolveCallables = TryResolveCallables(signatures),
      extractLiterals = cypherConfig.extractLiterals,
      parameterTypeMapping = ParameterValueTypeHelper.asCypherTypeMap(params, cypherConfig.useParameterSizeHint),
      resolveSimpleDynamicExpressions = cypherConfig.resolveSimpleDynamicExpressions,
      enabledVirtualGraph = cypherConfig.useVirtualGraph
    )

    private val context: BaseContext = BaseContextImpl(
      query.resolvedLanguage,
      CompilationPhaseTracer.NO_TRACING,
      notificationLogger,
      query.rawStatement,
      Some(query.options.offset),
      WrappedMonitors(kernelMonitors),
      cancellationChecker,
      internalSyntaxUsageStats,
      sessionDatabase,
      semanticFeatures,
      query.options.queryOptions.planMode.isScope,
      shadowedFunctions = shadowedFunctions,
      isDebugSession = false
    )

    object parseAndPrepare {

      // We need to make sure that names generated in `parseAndPrepare` cannot ever clash with names
      // generated elsewhere. The reason is that a query fragment that gets sent to a remote only undergoes
      // parseAndPrepare. On the remote, a new anonymousVariableNameGenerator will be used and that must never
      // clash with this one.
      private val anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(negativeNumbers = true)

      private val transformer =
        CompilationPhases
          .fabricParsing(parsingConfig, params)

      def process(): BaseState =
        transformer.transform(
          InitialState(query.statement, null, anonymousVariableNameGenerator),
          context
        )
    }

    object checkAndFinalize {

      private val anonymousVariableNameGenerator = new AnonymousVariableNameGenerator(negativeNumbers = false)

      private val transformer =
        CompilationPhases.fabricFinalize(parsingConfig, signatures)

      def process(statement: Statement, useFullQueryText: Boolean): BaseState = {
        val localQueryString =
          if (useFullQueryText)
            query.rawStatement
          else
            QueryRenderer.render(statement)

        val plannerName = PlannerNameFor(query.options.queryOptions.planner.name)
        val state = InitialState(
          localQueryString,
          plannerName,
          anonymousVariableNameGenerator = anonymousVariableNameGenerator
        )
          .withStatement(statement)
        transformer.transform(state, context)
      }
    }

    def internalNotifications: Set[InternalNotification] =
      context.notificationLogger.notifications
  }
}
