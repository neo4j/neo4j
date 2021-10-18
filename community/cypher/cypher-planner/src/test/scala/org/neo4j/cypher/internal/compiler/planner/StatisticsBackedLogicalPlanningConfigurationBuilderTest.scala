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
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.graphdb.schema.IndexType
import org.scalatest.FunSuite
import org.scalatest.Matchers.contain
import org.scalatest.Matchers.convertToAnyShouldWrapper

class StatisticsBackedLogicalPlanningConfigurationBuilderTest extends FunSuite with StatisticsBackedLogicalPlanningSupport {

  /**
   * These index types are currently handled differently from all the other property indexes.
   */
  val unsupportedIndexTypes = Set(IndexType.LOOKUP, IndexType.FULLTEXT)

  test("processGraphCount for node indexes") {
    IndexType.values()
      .filter(!unsupportedIndexTypes.contains(_))
      .foreach { indexType =>
        withClue(s"with ${indexType.name.toLowerCase} index") {

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
            indexType = indexType,
            propertyKeys = Seq("name"),
            uniqueValueSelectivity = 1,
            propExistsSelectivity = 1.0 / personCount,
            withValues = true,
            withOrdering = IndexOrderCapability.BOTH)
        }
      }
  }
  test("processGraphCount for relationship indexes") {
    IndexType.values()
      .filter(!unsupportedIndexTypes.contains(_))
      .foreach { indexType =>
        withClue(s"with ${indexType.name.toLowerCase} index") {

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
            indexType = indexType,
            propertyKeys = Seq("name"),
            uniqueValueSelectivity = 1,
            propExistsSelectivity = 1.0 / friendCount,
            withValues = true,
            withOrdering = IndexOrderCapability.BOTH)
        }
      }
  }
}
