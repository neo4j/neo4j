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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.graphcounts.GraphCountsJson
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexCapabilities
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.getProvidesOrder
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.getWithValues
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlannerWithCaching
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.internal.schema.IndexCapability
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.api.impl.schema.TextIndexProvider
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider
import org.neo4j.kernel.impl.index.schema.PointIndexProvider
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class StatisticsBackedLogicalPlanningConfigurationBuilderTest extends AnyFunSuite
    with StatisticsBackedLogicalPlanningSupport {

  /**
   * These index types are currently handled differently from all the other property indexes.
   */
  val unsupportedIndexTypes: Set[IndexType] = Set(IndexType.LOOKUP, IndexType.FULLTEXT)

  private def indexCapability(indexProviderDescriptor: IndexProviderDescriptor): IndexCapability =
    indexProviderDescriptor match {
      case TextIndexProvider.DESCRIPTOR    => IndexCapabilities.text_1_0
      case TrigramIndexProvider.DESCRIPTOR => IndexCapabilities.text_2_0
      case RangeIndexProvider.DESCRIPTOR   => IndexCapabilities.range
      case PointIndexProvider.DESCRIPTOR   => IndexCapabilities.point
    }

  private def indexProviders(indexType: IndexType): Seq[IndexProviderDescriptor] = indexType match {
    case IndexType.TEXT  => Seq(TextIndexProvider.DESCRIPTOR, TrigramIndexProvider.DESCRIPTOR)
    case IndexType.RANGE => Seq(RangeIndexProvider.DESCRIPTOR)
    case IndexType.POINT => Seq(PointIndexProvider.DESCRIPTOR)
  }

  test("processGraphCount for node indexes") {
    for {
      indexType <- IndexType.values()
      if !unsupportedIndexTypes.contains(indexType)
      indexProvider <- indexProviders(indexType)
    } {
      withClue(s"with ${indexType.name.toLowerCase} index and ${indexProvider.name()} provider:") {
        val personCount = 20
        val json =
          s"""
             |{
             |  "relationships":[],
             |  "nodes":[
             |    {"count":150},
             |    {"count":$personCount,"label":"Person"}
             |  ],
             |  "indexes":[
             |    {
             |      "updatesSinceEstimation":0,
             |      "totalSize":1,
             |      "properties":["name"],
             |      "labels":["Person"],
             |      "indexType":"${indexType.name}",
             |      "indexProvider":"${indexProvider.name()}",
             |      "estimatedUniqueSize": 1
             |    }
             |  ],
             |  "constraints":[]
             |}
             |""".stripMargin

        val graphCountData = GraphCountsJson.parseAsGraphCountDataFromString(json)
        val planner = plannerBuilder().processGraphCounts(graphCountData)
        planner.indexes.propertyIndexes should contain only IndexDefinition(
          entityType = IndexDefinition.EntityType.Node("Person"),
          indexType = indexType.toPublicApi,
          propertyKeys = Seq("name"),
          uniqueValueSelectivity = 1,
          propExistsSelectivity = 1.0 / personCount,
          withValues = getWithValues(indexType),
          withOrdering = getProvidesOrder(indexType),
          indexCapability = indexCapability(indexProvider)
        )
      }
    }
  }

  test("processGraphCount for relationship indexes") {
    for {
      indexType <- IndexType.values()
      if !unsupportedIndexTypes.contains(indexType)
      indexProvider <- indexProviders(indexType)
    } {
      withClue(s"with ${indexType.name.toLowerCase} index and ${indexProvider.name()} provider:") {
        val friendCount = 20
        val json =
          s"""
             |{
             |  "relationships":[
             |    {"count":500},
             |    {"count":$friendCount,"relationshipType":"FRIEND"}
             |  ],
             |  "nodes":[
             |    {"count":150},
             |    {"count":80,"label":"Person"}
             |  ],
             |  "indexes":[
             |    {
             |      "updatesSinceEstimation":0,
             |      "totalSize":1,
             |      "properties":["name"],
             |      "relationshipTypes":["FRIEND"],
             |      "indexType":"${indexType.name}",
             |      "indexProvider":"${indexProvider.name()}",
             |      "estimatedUniqueSize": 1
             |    }
             |  ],
             |  "constraints":[]
             |}
             |""".stripMargin

        val graphCountData = GraphCountsJson.parseAsGraphCountDataFromString(json)
        val planner = plannerBuilder().processGraphCounts(graphCountData)
        planner.indexes.propertyIndexes should contain only IndexDefinition(
          entityType = IndexDefinition.EntityType.Relationship("FRIEND"),
          indexType = indexType.toPublicApi,
          propertyKeys = Seq("name"),
          uniqueValueSelectivity = 1,
          propExistsSelectivity = 1.0 / friendCount,
          withValues = getWithValues(indexType),
          withOrdering = getProvidesOrder(indexType),
          indexCapability = indexCapability(indexProvider)
        )
      }
    }
  }

  test("should be able to control EXISTS subquery caching from tests") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .build()
    val plannerWithDebugFlag = plannerBuilder()
      .setAllNodesCardinality(100)
      .enableDebugOption(CypherDebugOption.disableExistsSubqueryCaching)
      .build()

    planner.queryGraphSolver() should beLike {
      case IDPQueryGraphSolver(_, _, ExistsSubqueryPlannerWithCaching()) => ()
    }
    plannerWithDebugFlag.queryGraphSolver() should beLike {
      case IDPQueryGraphSolver(_, _, ExistsSubqueryPlanner) => ()
    }
  }
}
