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
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.GraphDatabaseSettings.InferSchemaPartsStrategy
import org.neo4j.cypher.internal.CommunityInterpretedRuntime
import org.neo4j.cypher.internal.CommunitySchemaCommandRuntime
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.CypherVersionTestSupport
import org.neo4j.cypher.internal.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.compiler.CypherParsingConfig
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.compiler.NotImplementedPlanContext
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.NO_TRACING
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStats
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStatsImpl
import org.neo4j.cypher.internal.frontend.phases.SchemaInferenceUsageMetricKey
import org.neo4j.cypher.internal.notification.devNullLogger
import org.neo4j.cypher.internal.options.CypherInferSchemaPartsOption
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.planner.spi.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.DatabaseMode.DatabaseMode
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.NodesAllCardinality
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.preparser.PreParsedQuery
import org.neo4j.cypher.internal.preparser.QueryOptions
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.NullLog
import org.neo4j.logging.NullLogProvider
import org.neo4j.monitoring
import org.neo4j.values.virtual.MapValue

import java.time.Clock

import scala.collection.mutable

class InferSchemaPartsUsageMetricsTest extends CypherFunSuite with CypherVersionTestSupport {

  testVersions("DbOption OFF, CypherOption OFF: should increment use of inferSchemaParts' OFF option") { version =>
    val internalUsageStats = runParseAndPlan(
      dbOptionLabelInference = InferSchemaPartsStrategy.OFF,
      queries = List(
        QueryAndLabelInferenceOption(
          """
            |MATCH (n)-[r]->(m)
            |RETURN *
            |""".stripMargin,
          CypherInferSchemaPartsOption.off
        )
      ),
      version = version
    )
    internalUsageStats.getSchemaInferenceUsageCount(SchemaInferenceUsageMetricKey.OFF) shouldBe 1
    internalUsageStats.getSchemaInferenceUsageCount(SchemaInferenceUsageMetricKey.MOST_SELECTIVE_LABEL) shouldBe 0
  }

  testVersions(
    "DbOption OFF, CypherOption mostSelectiveLabel: should increment use of inferSchemaParts' most_selective_option option"
  ) { version =>
    val internalUsageStats = runParseAndPlan(
      dbOptionLabelInference = InferSchemaPartsStrategy.OFF,
      queries = List(
        QueryAndLabelInferenceOption(
          """
            |MATCH (n)-[r]->(m)
            |RETURN *
            |""".stripMargin,
          CypherInferSchemaPartsOption.mostSelectiveLabel
        )
      ),
      version = version
    )
    internalUsageStats.getSchemaInferenceUsageCount(SchemaInferenceUsageMetricKey.OFF) shouldBe 0
    internalUsageStats.getSchemaInferenceUsageCount(SchemaInferenceUsageMetricKey.MOST_SELECTIVE_LABEL) shouldBe 1
  }

  testVersions(
    "DbOption most_selective_label, CypherOption OFF: should increment use of inferSchemaParts' OFF option"
  ) { version =>
    val internalUsageStats = runParseAndPlan(
      dbOptionLabelInference = InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL,
      queries = List(
        QueryAndLabelInferenceOption(
          """
            |MATCH (n)-[r]->(m)
            |RETURN *
            |""".stripMargin,
          CypherInferSchemaPartsOption.off
        )
      ),
      version = version
    )
    internalUsageStats.getSchemaInferenceUsageCount(SchemaInferenceUsageMetricKey.OFF) shouldBe 1
    internalUsageStats.getSchemaInferenceUsageCount(SchemaInferenceUsageMetricKey.MOST_SELECTIVE_LABEL) shouldBe 0
  }

  testVersions(
    "DbOption most_selective_label, CypherOption most_selective_label: should increment use of inferSchemaParts' most_selective_label option"
  ) { version =>
    val internalUsageStats = runParseAndPlan(
      dbOptionLabelInference = InferSchemaPartsStrategy.MOST_SELECTIVE_LABEL,
      queries = List(
        QueryAndLabelInferenceOption(
          """
            |MATCH (n)-[r]->(m)
            |RETURN *
            |""".stripMargin,
          CypherInferSchemaPartsOption.mostSelectiveLabel
        )
      ),
      version = version
    )
    internalUsageStats.getSchemaInferenceUsageCount(SchemaInferenceUsageMetricKey.OFF) shouldBe 0
    internalUsageStats.getSchemaInferenceUsageCount(SchemaInferenceUsageMetricKey.MOST_SELECTIVE_LABEL) shouldBe 1
  }

  testVersions(
    "DbOption OFF, CypherOption OFF 2x, most_selective_label 1x: should increment use of inferSchemaParts' off option 2x, most_selective_label option 1x"
  ) { version =>
    val internalUsageStats = runParseAndPlan(
      dbOptionLabelInference = InferSchemaPartsStrategy.OFF,
      queries = List(
        QueryAndLabelInferenceOption(
          """
            |MATCH (n)-[r]->(m)
            |RETURN *
            |""".stripMargin,
          CypherInferSchemaPartsOption.off
        ),
        QueryAndLabelInferenceOption(
          """
            |MATCH (n)-[r]->(m)<-[s]-(o)
            |RETURN *
            |""".stripMargin,
          CypherInferSchemaPartsOption.mostSelectiveLabel
        ),
        QueryAndLabelInferenceOption(
          """
            |MATCH (n)-[r]->(m)<-[s]-(o)--()
            |RETURN *
            |""".stripMargin,
          CypherInferSchemaPartsOption.off
        )
      ),
      version = version
    )
    internalUsageStats.getSchemaInferenceUsageCount(SchemaInferenceUsageMetricKey.OFF) shouldBe 2
    internalUsageStats.getSchemaInferenceUsageCount(SchemaInferenceUsageMetricKey.MOST_SELECTIVE_LABEL) shouldBe 1
  }

  private case class QueryAndLabelInferenceOption(
    query: String,
    cypherInferSchemaPartsOption: CypherInferSchemaPartsOption
  )

  /**
   *
   * @param dbOptionLabelInference        The database option for inferSchemaParts
   * @param queries                       A list of queryString-cypherOptionInferSchemaParts pairs
   * @param version                       The cypher version
   * @return                              The internal usage statistics after executing the last query
   */
  private def runParseAndPlan(
    dbOptionLabelInference: InferSchemaPartsStrategy,
    queries: List[QueryAndLabelInferenceOption],
    version: CypherVersion
  ) = {
    val planner = constructPlanner(dbOptionLabelInference)

    for (i <- 0 until queries.size - 1) {
      parseAndPlanHelper(
        planner,
        version,
        queries(i).query,
        queries(i).cypherInferSchemaPartsOption
      )
    }
    val stats = parseAndPlanHelper(
      planner,
      version,
      queries.last.query,
      queries.last.cypherInferSchemaPartsOption
    )
    stats
  }

  private def parseAndPlanHelper(
    planner: CypherPlanner,
    version: CypherVersion,
    query: String,
    inferSchemaPartsOption: CypherInferSchemaPartsOption
  ): InternalUsageStats = {
    val preParserQuery =
      PreParsedQuery(query, query, QueryOptions.default(version).withInferSchemaPartsOption(inferSchemaPartsOption))

    val tc = mock[TransactionalContext](org.mockito.Mockito.RETURNS_DEEP_STUBS)

    val logicalPlanResult = planner
      .parseAndPlan(preParserQuery, NO_TRACING, tc, MapValue.EMPTY, CommunityInterpretedRuntime, devNullLogger, null)

    logicalPlanResult.plannerContext.internalUsageStats
  }

  private def constructPlanner(inferSchemaPartsStrategy: InferSchemaPartsStrategy): CypherPlanner = {
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
    }

    CypherPlanner.customPlanContextCreator = Some((_, _, _, _, _) => planContext)

    val monitors = new monitoring.Monitors()

    val caches = new CypherQueryCaches(
      CypherQueryCaches.Config.fromCypherConfiguration(CypherConfiguration.fromConfig(Config.defaults())),
      getTx,
      TestExecutorCaffeineCacheFactory,
      Clock.systemUTC(),
      monitors,
      NullLogProvider.getInstance()
    )

    CypherPlanner(
      CypherParsingConfig(),
      CypherPlannerConfiguration.withSettings(
        Map(GraphDatabaseSettings.cypher_infer_schema_parts_strategy -> inferSchemaPartsStrategy)
      ),
      Clock.systemUTC(),
      monitors,
      NullLog.getInstance(),
      caches,
      CypherPlannerOption.default,
      null,
      CommunitySchemaCommandRuntime,
      null,
      new InternalUsageStatsImpl
    )
  }
}
