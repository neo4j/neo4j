/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.fabric.planning

import org.neo4j.cypher.internal.FullyParsedQuery
import org.neo4j.cypher.internal.PreParsedQuery
import org.neo4j.cypher.internal.QueryOptions
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.options.CypherExpressionEngineOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.planner.spi.ProcedureSignatureResolver
import org.neo4j.cypher.rendering.QueryRenderer
import org.neo4j.fabric.cache.FabricQueryCache
import org.neo4j.fabric.config.FabricConfig
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
  cacheFactory: CaffeineCacheFactory,
  signatures: ProcedureSignatureResolver
) {

  private[planning] val queryCache = new FabricQueryCache(cacheFactory, cypherConfig.queryCacheSize)

  private val frontend = FabricFrontEnd(cypherConfig, monitors, signatures, cacheFactory)

  private def fabricContextName: Option[String] = {
    val name = config.getFabricDatabaseName
    if (name.isPresent) Some(name.get().name()) else None
  }

  def instance(queryString: String, queryParams: MapValue, defaultGraphName: String): PlannerInstance = {
    val query = frontend.preParsing.preParse(queryString)
    PlannerInstance(query, queryParams, defaultGraphName, fabricContextName)
  }

  def isPeriodicCommit(queryString: String): Boolean = {
    frontend.preParsing.isPeriodicCommit(queryString)
  }

  case class PlannerInstance(
    query: PreParsedQuery,
    queryParams: MapValue,
    defaultContextName: String,
    fabricContextName: Option[String]
  ) {

    private lazy val pipeline = frontend.Pipeline(query, queryParams)

    lazy val plan: FabricPlan = {
      val plan = queryCache.computeIfAbsent(
        query.cacheKey,
        queryParams,
        defaultContextName,
        () => computePlan(),
        shouldCache
      )
      plan.copy(
        executionType = frontend.preParsing.executionType(query.options, plan.inFabricContext),
        queryOptionsOffset = query.options.offset
      )
    }

    private def computePlan(): FabricPlan = trace {
      val prepared = pipeline.parseAndPrepare.process()

      val fragmenter =
        new FabricFragmenter(defaultContextName, query.statement, prepared.statement(), prepared.semantics())
      val fragments = fragmenter.fragment
      val periodicCommitHint = fragmenter.periodicCommitHint

      val fabricContext = inFabricContext(fragments)

      val stitcher = FabricStitcher(query.statement, fabricContext, fabricContextName, periodicCommitHint, pipeline)
      val stitchedFragments = stitcher.convert(fragments)

      FabricPlan(
        query = stitchedFragments,
        queryType = QueryType.recursive(stitchedFragments),
        executionType = FabricPlan.Execute,
        queryString = query.statement,
        debugOptions = DebugOptions.from(query.options.queryOptions.debugOptions),
        obfuscationMetadata = prepared.obfuscationMetadata(),
        inFabricContext = fabricContext,
        internalNotifications = pipeline.internalNotifications,
        queryOptionsOffset = query.options.offset
      )
    }

    private def shouldCache(plan: FabricPlan): Boolean =
      !QueryType.sensitive(plan.query)

    private def optionsFor(fragment: Fragment) =
      if (isFabricFragment(fragment))
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
      QueryRenderer.addOptions(fragment.remoteQuery.query, optionsFor(fragment)),
      fragment.queryType,
      fragment.remoteQuery.extractedLiterals
    )

    private def inFabricContext(fragment: Fragment): Boolean = {
      def inFabricDefaultContext =
        fabricContextName.contains(defaultContextName)

      inFabricDefaultContext || isFabricFragment(fragment)
    }

    private def isFabricFragment(fragment: Fragment): Boolean = {
      def isFabricUse(use: Use): Boolean =
        UseEvaluation.evaluateStatic(use.graphSelection)
          .exists(cn => cn.parts == fabricContextName.toList)

      def check(frag: Fragment): Boolean = frag match {
        case chain: Fragment.Chain     => isFabricUse(chain.use)
        case union: Fragment.Union     => check(union.lhs) && check(union.rhs)
        case command: Fragment.Command => isFabricUse(command.use)
      }

      check(fragment)
    }

    private[planning] def withForceFabricContext(force: Boolean) =
      if (force) this.copy(fabricContextName = Some(defaultContextName))
      else this.copy(fabricContextName = None)
  }
}
