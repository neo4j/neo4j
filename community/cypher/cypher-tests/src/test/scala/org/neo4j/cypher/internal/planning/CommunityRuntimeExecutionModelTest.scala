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
package org.neo4j.cypher.internal.planning

import org.neo4j.common
import org.neo4j.configuration.Config
import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.CachingPreParser
import org.neo4j.cypher.internal.CommunityRuntimeFactory
import org.neo4j.cypher.internal.CommunitySchemaCommandRuntime
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.cache.CypherQueryCaches.CacheStrategy
import org.neo4j.cypher.internal.cache.LFUCache
import org.neo4j.cypher.internal.compiler.CypherParsingConfig
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.ExecutionModel.Volcano
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStatsImpl
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.notification.devNullLogger
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.preparser.PreParsedQuery
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.NodesAllCardinality
import org.neo4j.cypher.internal.planner.spi.NotImplementedPlanContext
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.NullLog
import org.neo4j.logging.NullLogProvider
import org.neo4j.monitoring
import org.neo4j.values.virtual.MapValue

import java.time.Clock

import scala.collection.mutable

/**
 * Regression test for https://github.com/neo4j/neo4j/issues/13937.
 *
 * In Community Edition an explicit `runtime=pipelined` (or `runtime=parallel`) has no pipelined
 * executor: the query falls back to the slotted runtime (see [[CommunityRuntimeFactory]]).
 * Logical planning must therefore use Volcano (row-at-a-time) cost assumptions, not batched ones:
 * a Cartesian product costed with `ceil(lhsCardinality / batchSize)` RHS executions but executed
 * with `lhsCardinality` executions can select an orientation that is catastrophically expensive
 * under the actual slotted execution.
 *
 * These tests plan a real query through the real pre-parser and [[TransformingPlanner]] and assert
 * the effective [[org.neo4j.cypher.internal.compiler.ExecutionModel]].
 */
class CommunityRuntimeExecutionModelTest extends CommunityCypherTestSuite {

  private def planWithCommunityRuntime(queryText: String, runtimeOption: CypherRuntimeOption) = {
    val stats = new GraphStatistics {
      override def nodesAllCardinality(): Cardinality = Cardinality.EMPTY
      override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = Cardinality.EMPTY
      override def patternStepCardinality(
        fromLabel: Option[LabelId],
        relTypeId: Option[RelTypeId],
        toLabel: Option[LabelId]
      ): Cardinality = Cardinality.EMPTY
      override def uniqueValueSelectivity(index: IndexDescriptor): Option[Selectivity] = Some(Selectivity.ZERO)
      override def indexPropertyIsNotNullSelectivity(index: IndexDescriptor): Option[Selectivity] =
        Some(Selectivity.ZERO)
    }

    val getTx = () => 1L
    val planContext = new NotImplementedPlanContext {
      override def statistics: InstrumentedGraphStatistics = InstrumentedGraphStatistics(
        stats,
        new MutableGraphStatisticsSnapshot(mutable.Map(NodesAllCardinality -> 1.0))
      )
      override def getPropertiesWithExistenceConstraint: Set[String] = Set.empty
      override def nodeTokenIndex: Option[TokenIndexDescriptor] =
        Some(TokenIndexDescriptor(common.EntityType.NODE, IndexOrderCapability.BOTH))
      override def lastCommittedTxIdProvider: () => Long = getTx
      override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = Iterator.empty

      override def procedureSignatureVersion: Long = -1

      override def databaseMode: DatabaseMode = DatabaseMode.SINGLE

      override def storageHasPropertyColocation: Boolean = true
      override def storageSupportsFastExpandInto: Boolean = true
      override def storageIsMvcc: Boolean = false
      override def txStateHasChanges(): Boolean = false
    }

    TransformingPlanner.customPlanContextCreator = Some((_, _, _, _, _) => planContext)
    try {
      val monitors = new monitoring.Monitors()
      val cypherConfig = CypherConfiguration.fromConfig(Config.defaults())
      val caches = new CypherQueryCaches(
        CypherQueryCaches.Config.fromCypherConfiguration(cypherConfig),
        getTx,
        TestExecutorCaffeineCacheFactory,
        Clock.systemUTC(),
        monitors,
        NullLogProvider.getInstance()
      )

      val planner = DefaultCypherPlanner(
        CypherParsingConfig(),
        CypherPlannerConfiguration.defaults(),
        Clock.systemUTC(),
        monitors,
        NullLog.getInstance(),
        CommunitySecurityLog.NULL_LOG,
        caches,
        CypherPlannerOption.default,
        null,
        CommunitySchemaCommandRuntime,
        null,
        new InternalUsageStatsImpl
      )

      val preParser = new CachingPreParser(
        CypherConfiguration.fromConfig(Config.defaults()),
        new LFUCache[PreParsedQuery.CacheKey, PreParsedQuery](
          TestExecutorCaffeineCacheFactory,
          0
        )
      )
      val preParsed = preParser.preParseQuery(queryText, devNullLogger, CypherVersion.Legacy.legacyVersion())
      val tc = mock[TransactionalContext](org.mockito.Mockito.RETURNS_DEEP_STUBS)
      val runtime = CommunityRuntimeFactory.getRuntime(runtimeOption, disallowFallback = false)

      planner.parseAndPlan(
        preParsed,
        NO_TRACING,
        tc,
        MapValue.EMPTY,
        runtime,
        devNullLogger,
        null,
        cacheStrategy = CacheStrategy.defaultDefault
      )
    } finally {
      TransformingPlanner.customPlanContextCreator = None
    }
  }

  test("community explicit pipelined plans with Volcano since it executes as slotted") {
    val result = planWithCommunityRuntime(
      "CYPHER runtime=pipelined MATCH (a), (b) RETURN a, b",
      CypherRuntimeOption.pipelined
    )
    result.plannerContext.executionModel should equal(Volcano)
  }

  test("community explicit parallel plans with Volcano since it executes as slotted") {
    val result = planWithCommunityRuntime(
      "CYPHER runtime=parallel MATCH (a), (b) RETURN a, b",
      CypherRuntimeOption.parallel
    )
    result.plannerContext.executionModel should equal(Volcano)
  }

  test("community slotted plans with Volcano") {
    val result = planWithCommunityRuntime(
      "CYPHER runtime=slotted MATCH (a), (b) RETURN a, b",
      CypherRuntimeOption.slotted
    )
    result.plannerContext.executionModel should equal(Volcano)
  }

  test("community default plans with Volcano") {
    val result = planWithCommunityRuntime(
      "MATCH (a), (b) RETURN a, b",
      CypherRuntimeOption.default
    )
    result.plannerContext.executionModel should equal(Volcano)
  }
}
