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
package org.neo4j.fabric.planning

import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.ast.CatalogName
import org.neo4j.cypher.internal.cache.CacheSize
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.CypherScope
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStatsNoOp
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.ScopedProcedureSignatureResolver
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.RecordingNotificationLogger
import org.neo4j.cypher.rendering.QueryOptionsRenderer
import org.neo4j.fabric.cache.FabricQueryCache
import org.neo4j.fabric.config.FabricConfig
import org.neo4j.fabric.eval.Catalog
import org.neo4j.fabric.eval.UseEvaluation
import org.neo4j.fabric.pipeline.FabricFrontEnd
import org.neo4j.fabric.planning.FabricPlan.DebugOptions
import org.neo4j.fabric.planning.FabricQuery.LocalQuery
import org.neo4j.fabric.planning.FabricQuery.RemoteQuery
import org.neo4j.monitoring.Monitors
import org.neo4j.values.virtual.MapValue

case class FabricPlanner(
  config: FabricConfig,
  cypherConfig: CypherConfiguration,
  monitors: Monitors,
  cacheFactory: CaffeineCacheFactory
) {

  private[planning] val queryCache = new FabricQueryCache(cacheFactory, CacheSize.Dynamic(cypherConfig.queryCacheSize))

  private val frontend = FabricFrontEnd(cypherConfig, monitors, cacheFactory)

  /**
   * Convenience method without cancellation checker or InternalSyntaxUsageStats. Should be used for tests only.
   */
  def instance(
    signatureResolver: ProcedureSignatureResolver,
    queryString: String,
    queryParams: MapValue,
    defaultGraphName: String,
    catalog: Catalog
  ): PlannerInstance =
    instance(
      signatureResolver,
      queryString,
      queryParams,
      defaultGraphName,
      catalog,
      InternalSyntaxUsageStatsNoOp,
      CancellationChecker.NeverCancelled
    )

  def instance(
    signatureResolver: ProcedureSignatureResolver,
    queryString: String,
    queryParams: MapValue,
    defaultGraphName: String,
    catalog: Catalog,
    internalSyntaxUsageStats: InternalSyntaxUsageStats,
    cancellationChecker: CancellationChecker
  ): PlannerInstance = {
    val notificationLogger = new RecordingNotificationLogger()
    val query = frontend.preParsing.preParse(queryString, notificationLogger)
    PlannerInstance(
      ScopedProcedureSignatureResolver.from(
        signatureResolver,
        CypherScope.from(query.options.queryOptions.cypherVersion.actualVersion)
      ),
      query,
      queryParams,
      defaultGraphName,
      catalog,
      cancellationChecker,
      notificationLogger,
      internalSyntaxUsageStats
    )
  }

  case class PlannerInstance(
    signatureResolver: ScopedProcedureSignatureResolver,
    query: PreParsedQuery,
    queryParams: MapValue,
    defaultContextName: String,
    catalog: Catalog,
    cancellationChecker: CancellationChecker,
    notificationLogger: InternalNotificationLogger,
    internalSyntaxUsageStats: InternalSyntaxUsageStats
  ) {

    private lazy val pipeline =
      frontend.Pipeline(
        signatureResolver,
        query,
        queryParams,
        cancellationChecker,
        notificationLogger,
        internalSyntaxUsageStats
      )

    private val useHelper = new UseHelper(catalog, defaultContextName)

    lazy val plan: FabricPlan = {
      val plan = queryCache.computeIfAbsent(
        query.cacheKey,
        queryParams,
        defaultContextName,
        () => computePlan(),
        shouldCache,
        cypherConfig.useParameterSizeHint
      )
      plan.copy(
        executionType = frontend.preParsing.executionType(query.options, plan.inCompositeContext),
        queryOptionsOffset = query.options.offset
      )
    }

    private def computePlan(): FabricPlan = trace {
      val prepared = pipeline.parseAndPrepare.process()

      val fragmenter =
        new FabricFragmenter(defaultContextName, query.statement, prepared.statement(), prepared.semantics())
      val fragments = fragmenter.fragment

      val compositeContext = useHelper.rootTargetsCompositeContext(fragments)

      val stitcher =
        FabricStitcher(query.statement, compositeContext, pipeline, useHelper)
      val stitchedFragments = stitcher.convert(fragments)

      FabricPlan(
        query = stitchedFragments,
        queryType = QueryType.recursive(stitchedFragments),
        executionType = FabricPlan.Execute,
        queryString = query.statement,
        debugOptions = DebugOptions.noLogging(),
        obfuscationMetadata = prepared.obfuscationMetadata(),
        inCompositeContext = compositeContext,
        internalNotifications = pipeline.internalNotifications,
        queryOptionsOffset = query.options.offset
      )
    }

    private def shouldCache(plan: FabricPlan): Boolean =
      !QueryType.sensitive(plan.query)

    private def optionsFor(fragment: Fragment) =
      if (useHelper.fragmentTargetsCompositeContext(fragment))
        QueryOptions.default.copy(
          queryOptions = QueryOptions.default.queryOptions.copy(
            runtime = CypherRuntimeOption.slotted,
            expressionEngine = CypherExpressionEngineOption.interpreted
          ),
          materializedEntitiesMode = true
        )
      else
        query.options

    private def trace(compute: => FabricPlan): FabricPlan = {
      val event = pipeline.traceStart()
      try compute
      finally event.close()
    }

    def asLocal(fragment: Fragment.Exec): LocalQuery = LocalQuery(
      FullyParsedQuery(fragment.localQuery, optionsFor(fragment)),
      fragment.queryType
    )

    def asRemote(fragment: Fragment.Exec): RemoteQuery = RemoteQuery(
      QueryOptionsRenderer.addOptions(fragment.remoteQuery.query, optionsFor(fragment)),
      fragment.queryType,
      fragment.remoteQuery.extractedLiterals
    )

    def targetsComposite(fragment: Fragment): Boolean =
      useHelper.fragmentTargetsCompositeContext(fragment)
  }
}

class UseHelper(catalog: Catalog, defaultContextName: String) {

  def rootTargetsCompositeContext(fragment: Fragment): Boolean =
    isComposite(CatalogName(defaultContextName)) || fragmentTargetsCompositeContext(fragment)

  def fragmentTargetsCompositeContext(fragment: Fragment): Boolean = {
    def check(frag: Fragment): Boolean = frag match {
      case chain: Fragment.Chain     => useTargetsCompositeContext(chain.use)
      case union: Fragment.Union     => check(union.lhs) && check(union.rhs)
      case command: Fragment.Command => useTargetsCompositeContext(command.use)
    }

    check(fragment)
  }

  def useTargetsCompositeContext(use: Use): Boolean =
    UseEvaluation.evaluateStatic(use.graphSelection).exists(isComposite)

  private def isComposite(name: CatalogName): Boolean =
    catalog.resolveGraphOption(name) match {
      case Some(_: Catalog.Composite) => true
      case _                          => false
    }
}
