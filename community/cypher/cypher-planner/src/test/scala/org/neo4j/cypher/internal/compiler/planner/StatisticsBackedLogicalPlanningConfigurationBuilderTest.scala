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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.graphcounts.GraphCountsJson
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.ExistenceConstraintDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexCapabilities
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.getProvidesOrder
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.getWithValues
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlannerWithCaching
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.schema.IndexCapability
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.neo4j.kernel.api.impl.schema.TextIndexProvider
import org.neo4j.kernel.api.impl.schema.trigram.TrigramIndexProvider
import org.neo4j.kernel.impl.index.schema.PointIndexProvider
import org.neo4j.kernel.impl.index.schema.RangeIndexProvider
import org.scalatest.LoneElement

import java.util.Locale

class StatisticsBackedLogicalPlanningConfigurationBuilderTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport with LoneElement {

  /**
   * These index types are currently handled differently from all the other property indexes.
   */
  val unsupportedIndexTypes: Set[IndexType] = Set(IndexType.LOOKUP, IndexType.FULLTEXT, IndexType.VECTOR)

  private def indexCapability(indexProviderDescriptor: IndexProviderDescriptor): IndexCapability =
    indexProviderDescriptor match {
      case TextIndexProvider.DESCRIPTOR    => IndexCapabilities.text_1_0
      case TrigramIndexProvider.DESCRIPTOR => IndexCapabilities.text_2_0
      case RangeIndexProvider.DESCRIPTOR   => IndexCapabilities.range
      case PointIndexProvider.DESCRIPTOR   => IndexCapabilities.point
      case _ => throw new IllegalArgumentException(s"Unexpected descriptor: $indexProviderDescriptor")
    }

  private def indexProviders(indexType: IndexType): Seq[IndexProviderDescriptor] = indexType match {
    case IndexType.TEXT  => Seq(TextIndexProvider.DESCRIPTOR, TrigramIndexProvider.DESCRIPTOR)
    case IndexType.RANGE => Seq(RangeIndexProvider.DESCRIPTOR)
    case IndexType.POINT => Seq(PointIndexProvider.DESCRIPTOR)
    case _               => throw new IllegalArgumentException(s"Unexpected index type: $indexType")
  }

  test("processGraphCount for node indexes") {
    for {
      indexType <- IndexType.values()
      if !unsupportedIndexTypes.contains(indexType)
      indexProvider <- indexProviders(indexType)
    } {
      withClue(s"with ${indexType.name.toLowerCase(Locale.ROOT)} index and ${indexProvider.name()} provider:") {
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
        planner.indexes.propertyIndexes.loneElement should be(IndexDefinition(
          entityType = IndexDefinition.EntityType.Node("Person"),
          indexType = indexType.toPublicApi,
          propertyKeys = Seq("name"),
          uniqueValueSelectivity = 1,
          propExistsSelectivity = 1.0 / personCount,
          withValues = getWithValues(indexType),
          withOrdering = getProvidesOrder(indexType),
          indexCapability = indexCapability(indexProvider)
        ))
      }
    }
  }

  test("processGraphCount for node key constraints") {
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
         |      "estimatedUniqueSize": 0,
         |      "labels": [],
         |      "totalSize": 0,
         |      "updatesSinceEstimation": 0,
         |      "indexType": "LOOKUP",
         |      "indexProvider": "token-lookup-1.0"
         |    },
         |    {
         |      "updatesSinceEstimation":0,
         |      "totalSize":1,
         |      "properties":["name"],
         |      "labels":["Person"],
         |      "indexType":"${IndexType.RANGE.name}",
         |      "indexProvider":"${RangeIndexProvider.DESCRIPTOR.name()}",
         |      "estimatedUniqueSize": 1
         |    }
         |  ],
         |  "constraints":[
         |  {
         |    "label": "Person",
         |    "properties": ["name"],
         |    "type": "Node Key"
         |   }
         |  ]
         |}
         |""".stripMargin

    val graphCountData = GraphCountsJson.parseAsGraphCountDataFromString(json)
    val builder = plannerBuilder().processGraphCounts(graphCountData)
    builder.existenceConstraints.loneElement should be(ExistenceConstraintDefinition(
      entityType = IndexDefinition.EntityType.Node("Person"),
      propertyKey = "name"
    ))
    val planner = builder.build()
    val plan = planner
      .plan("MATCH (p:Person) RETURN p.name AS name")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[p.name] AS name")
      .nodeIndexOperator("p:Person(name)", getValue = _ => GetValue)
      .build())
  }

  test("processGraphCount for composite node key constraints") {
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
         |      "estimatedUniqueSize": 0,
         |      "labels": [],
         |      "totalSize": 0,
         |      "updatesSinceEstimation": 0,
         |      "indexType": "LOOKUP",
         |      "indexProvider": "token-lookup-1.0"
         |    },
         |    {
         |      "updatesSinceEstimation":0,
         |      "totalSize":1,
         |      "properties":["name", "surname"],
         |      "labels":["Person"],
         |      "indexType":"${IndexType.RANGE.name}",
         |      "indexProvider":"${RangeIndexProvider.DESCRIPTOR.name()}",
         |      "estimatedUniqueSize": 1
         |    }
         |  ],
         |  "constraints":[
         |  {
         |    "label": "Person",
         |    "properties": ["name", "surname"],
         |    "type": "Node Key"
         |   }
         |  ]
         |}
         |""".stripMargin

    val graphCountData = GraphCountsJson.parseAsGraphCountDataFromString(json)
    val builder = plannerBuilder().processGraphCounts(graphCountData)
    builder.existenceConstraints should contain.only(
      ExistenceConstraintDefinition(
        entityType = IndexDefinition.EntityType.Node("Person"),
        propertyKey = "name"
      ),
      ExistenceConstraintDefinition(
        entityType = IndexDefinition.EntityType.Node("Person"),
        propertyKey = "surname"
      )
    )
    val planner = builder.build()
    val plan = planner
      .plan("MATCH (p:Person) RETURN p.name AS name")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[p.name] AS name")
      .nodeIndexOperator("p:Person(name, surname)", getValue = Map("name" -> GetValue, "surname" -> DoNotGetValue))
      .build())
  }

  test("processGraphCount for relationship key constraints") {
    // Relationship key constraints are not yet supported in the kernel
    val friendCount = 20
    val json =
      s"""
         |{
         |  "relationships":[
         |    {"count":500},
         |    {"count":$friendCount,"relationshipType":"KNOWS"}
         |  ],
         |  "nodes":[
         |    {"count":150},
         |    {"count":80,"label":"Person"}
         |  ],
         |  "indexes":[
         |    {
         |      "estimatedUniqueSize": 0,
         |      "relationshipTypes": [],
         |      "totalSize": 0,
         |      "updatesSinceEstimation": 0,
         |      "indexType": "LOOKUP",
         |      "indexProvider": "token-lookup-1.0"
         |    },
         |    {
         |      "updatesSinceEstimation":0,
         |      "totalSize":1,
         |      "properties":["since"],
         |      "relationshipTypes":["KNOWS"],
         |      "indexType":"${IndexType.RANGE.name}",
         |      "indexProvider":"${RangeIndexProvider.DESCRIPTOR.name()}",
         |      "estimatedUniqueSize": 1
         |    }
         |  ],
         |  "constraints":[
         |  {
         |    "relationshipType": "KNOWS",
         |    "properties": [
         |      "since"
         |    ],
         |    "type": "Node Key" 
         |   }
         |  ]
         |}
         |""".stripMargin

    val graphCountData = GraphCountsJson.parseAsGraphCountDataFromString(json)
    val builder = plannerBuilder().processGraphCounts(graphCountData)
    builder.existenceConstraints should contain only ExistenceConstraintDefinition(
      entityType = IndexDefinition.EntityType.Relationship("KNOWS"),
      propertyKey = "since"
    )
    val planner = builder.build()
    val plan = planner
      .plan("MATCH ()-[r:KNOWS]->() RETURN r.since AS since")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheR[r.since] AS since")
      .relationshipIndexOperator("(anon_0)-[r:KNOWS(since)]->(anon_1)", getValue = _ => GetValue)
      .build())
  }

  test("processGraphCount for relationship indexes") {
    for {
      indexType <- IndexType.values()
      if !unsupportedIndexTypes.contains(indexType)
      indexProvider <- indexProviders(indexType)
    } {
      withClue(s"with ${indexType.name.toLowerCase(Locale.ROOT)} index and ${indexProvider.name()} provider:") {
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
        planner.indexes.propertyIndexes.loneElement should be(IndexDefinition(
          entityType = IndexDefinition.EntityType.Relationship("FRIEND"),
          indexType = indexType.toPublicApi,
          propertyKeys = Seq("name"),
          uniqueValueSelectivity = 1,
          propExistsSelectivity = 1.0 / friendCount,
          withValues = getWithValues(indexType),
          withOrdering = getProvidesOrder(indexType),
          indexCapability = indexCapability(indexProvider)
        ))
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
