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
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.DatabaseFormat
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.ExistenceConstraintDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexCapabilities
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.RelDef
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.RelationshipEndpointLabelConstraintDefinition
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.getProvidesOrder
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.getWithValues
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlannerWithCaching
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.internal.schema.AllIndexProviderDescriptors
import org.neo4j.internal.schema.EndpointType
import org.neo4j.internal.schema.IndexCapability
import org.neo4j.internal.schema.IndexProviderDescriptor
import org.neo4j.internal.schema.IndexType
import org.scalatest.LoneElement

import java.util.Locale

class StatisticsBackedLogicalPlanningConfigurationBuilderTest extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport with LoneElement {

  /**
   * These index types are currently handled differently from all the other property indexes.
   */
  val unsupportedIndexTypes: Set[IndexType] = Set(IndexType.LOOKUP, IndexType.FULLTEXT, IndexType.VECTOR)

  private def indexCapability(indexProviderDescriptor: IndexProviderDescriptor): IndexCapability =
    indexProviderDescriptor match {
      case AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR => IndexCapabilities.text_1_0
      case AllIndexProviderDescriptors.TEXT_V2_DESCRIPTOR => IndexCapabilities.text_2_0
      case AllIndexProviderDescriptors.TEXT_V3_DESCRIPTOR => IndexCapabilities.text_3_0
      case AllIndexProviderDescriptors.RANGE_DESCRIPTOR   => IndexCapabilities.range
      case AllIndexProviderDescriptors.POINT_DESCRIPTOR   => IndexCapabilities.point
      case _ => throw new IllegalArgumentException(s"Unexpected descriptor: $indexProviderDescriptor")
    }

  private def indexProviders(indexType: IndexType): Seq[IndexProviderDescriptor] = indexType match {
    case IndexType.TEXT =>
      Seq(
        AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR,
        AllIndexProviderDescriptors.TEXT_V2_DESCRIPTOR,
        AllIndexProviderDescriptors.TEXT_V3_DESCRIPTOR
      )
    case IndexType.RANGE => Seq(AllIndexProviderDescriptors.RANGE_DESCRIPTOR)
    case IndexType.POINT => Seq(AllIndexProviderDescriptors.POINT_DESCRIPTOR)
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
         |      "indexProvider":"${AllIndexProviderDescriptors.RANGE_DESCRIPTOR.name()}",
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
         |      "indexProvider":"${AllIndexProviderDescriptors.RANGE_DESCRIPTOR.name()}",
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
         |      "indexProvider":"${AllIndexProviderDescriptors.RANGE_DESCRIPTOR.name()}",
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
      .relationshipIndexOperator("()-[r:KNOWS(since)]->()", getValue = _ => GetValue)
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

  test("should accumulate graph counts for all relationships if relType is empty") {
    val knowsCount = 5
    val worksWithCount = 5
    val worksWithCountAdditionalValue = 7
    val ownsCount = 5
    val json =
      s"""
         |{
         |  "indexes": [],
         |  "nodes": [
         |    {
         |      "count": 50
         |    },
         |    {
         |      "count": 40,
         |      "label": "Person"
         |    },
         |    {
         |      "count": 10,
         |      "label": "Pet"
         |    }
         |  ],
         |  "relationships": [
         |    {
         |      "count": 25
         |    },
         |    {
         |      "count": $knowsCount,
         |      "relationshipType": "knows"
         |    },
         |    {
         |      "count": $knowsCount,
         |      "startLabel": "Person",
         |      "relationshipType": "knows"
         |    },
         |    {
         |      "count": $knowsCount,
         |      "relationshipType": "knows",
         |      "startLabel": "Person",
         |      "endLabel": "Person"
         |    },
         |    {
         |      "count": $knowsCount,
         |      "relationshipType": "knows",
         |      "endLabel": "Person"
         |    },
         |    {
         |      "count": $worksWithCountAdditionalValue,
         |      "relationshipType": "works_with"
         |    },
         |    {
         |      "count": $worksWithCount,
         |      "startLabel": "Person",
         |      "relationshipType": "works_with"
         |    },
         |    {
         |      "count": $worksWithCount,
         |      "relationshipType": "works_with",
         |      "endLabel": "Person"
         |    },
         |    {
         |      "count": $worksWithCount,
         |      "relationshipType": "works_with",
         |      "startLabel": "Person",
         |      "endLabel": "Person"
         |    },
         |    {
         |      "count": $ownsCount,
         |      "relationshipType": "owns"
         |    },
         |    {
         |      "count": $ownsCount,
         |      "startLabel": "Person",
         |      "relationshipType": "owns"
         |    },
         |    {
         |      "count": $ownsCount,
         |      "relationshipType": "owns",
         |      "endLabel": "Pet"
         |    },
         |    {
         |      "count": $ownsCount,
         |      "startLabel": "Person",
         |      "relationshipType": "owns",
         |      "endLabel": "Pet"
         |    }
         |  ]
         |}
         |""".stripMargin

    val graphCountData = GraphCountsJson.parseAsGraphCountDataFromString(json)
    val planner = plannerBuilder().processGraphCounts(graphCountData)

    planner.cardinalities.getRelCount(
      RelDef(Some("Person"), None, Some("Person"))
    ) shouldEqual (knowsCount + worksWithCount)
    planner.cardinalities.getRelCount(
      RelDef(Some("Person"), Some("works_with"), Some("Person"))
    ) shouldEqual (worksWithCount)
    planner.cardinalities.getRelCount(RelDef(None, None, Some("Person"))) shouldEqual (knowsCount + worksWithCount)
    planner.cardinalities.getRelCount(
      RelDef(Some("Person"), None, None)
    ) shouldEqual (knowsCount + worksWithCount + ownsCount)
    planner.cardinalities.getRelCount(
      RelDef(None, Some("works_with"), None)
    ) shouldEqual (worksWithCountAdditionalValue)
  }

  test("should consider the second time a cardinality is set") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllNodesCardinality(200)
      .build()

    planner.planContext.statistics.nodesAllCardinality().amount shouldEqual 200
  }

  test("should allow relationship endpoint label constraints to be overridden") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .addRelationshipEndpointLabelConstraint("()-[:REL]-(:StartAndEnd)")
      .addRelationshipEndpointLabelConstraint("()<-[:REL]-(:Start)")
      .build()

    planner.planContext.getRelationshipEndpointLabelConstraints("REL") shouldEqual
      Map(
        // second constraint overrides the first one
        EndpointType.START -> "Start",
        EndpointType.END -> "StartAndEnd"
      )
  }

  test("should allow start and end relationship endpoint label constraints to be specified at once") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .addRelationshipEndpointLabelConstraint("(:End)<-[:REL]-(:Start)")
      .build()

    planner.planContext.getRelationshipEndpointLabelConstraints("REL") shouldEqual
      Map(
        EndpointType.START -> "Start",
        EndpointType.END -> "End"
      )
  }

  test("should fail on invalid relationship endpoint label constraints") {
    (the[IllegalArgumentException] thrownBy {
      plannerBuilder()
        .addRelationshipEndpointLabelConstraint("()<-[:REL]-()")
    }).getMessage should equal(
      "Invalid relationship pattern `()-[:REL]->()` for relationship endpoint constraint. Expected relationship type and at least one of the labels to be set."
    )

    (the[IllegalArgumentException] thrownBy {
      plannerBuilder()
        .addRelationshipEndpointLabelConstraint("(:End)<-[]-()")
    }).getMessage should equal(
      "Invalid relationship pattern `()-[]->(:End)` for relationship endpoint constraint. Expected relationship type and at least one of the labels to be set."
    )
  }

  test("should provide node label constraints if specified in the configuration") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .addNodeLabelConstraint("Constrained", "Implied")
      .addNodeLabelConstraint("Constrained", "Implied2")
      .build()

    planner.planContext.getNodeLabelConstraints("Constrained") shouldEqual Set("Implied", "Implied2")
    planner.planContext.hasNodeLabelConstraint("Constrained", "Implied") shouldEqual true
    planner.planContext.hasNodeLabelConstraint("Constrained", "Implied2") shouldEqual true
  }

  test("processGraphCount for relationship endpoint constraints") {
    val json =
      s"""
         |{
         |  "relationships":[
         |    {"count":500},
         |    {"count":20,"relationshipType":"ACTED_IN"}
         |  ],
         |  "nodes":[
         |    {"count":150},
         |    {"count":80,"label":"Actor"},
         |    {"count":20, "label": "Movie"}
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
         |      "relationshipTypes":["ACTED_IN"],
         |      "indexType":"${IndexType.RANGE.name}",
         |      "indexProvider":"${AllIndexProviderDescriptors.RANGE_DESCRIPTOR.name()}",
         |      "estimatedUniqueSize": 1
         |    }
         |  ],
         |  "constraints":[
         |  {
         |    "relationshipType": "ACTED_IN",
         |    "type": "Relationship endpoint label constraint",
         |    "endpointType": "START",
         |    "enforcedLabel": "Actor"
         |   },
         |   {
         |    "relationshipType": "ACTED_IN",
         |    "type": "Relationship endpoint label constraint",
         |    "endpointType": "END",
         |    "enforcedLabel": "Movie"
         |   }
         |  ]
         |}
         |""".stripMargin

    val graphCountData = GraphCountsJson.parseAsGraphCountDataFromString(json)
    val builder = plannerBuilder().processGraphCounts(graphCountData)
    builder.relationshipEndpointLabelConstraints should contain(
      RelationshipEndpointLabelConstraintDefinition(
        relType = "ACTED_IN",
        endPoint = EndpointType.START,
        label = "Actor"
      )
    )
    builder.relationshipEndpointLabelConstraints should contain(
      RelationshipEndpointLabelConstraintDefinition(relType = "ACTED_IN", endPoint = EndpointType.END, label = "Movie")
    )
  }

  test("processGraphCount for node label existence constraints") {
    val json =
      s"""
         |{
         |  "nodes":[
         |    {"count":150},
         |    {"count":10,"label":"Actor"},
         |    {"count":120, "label": "Person"}
         |  ],
         |  "indexes":[],
         |  "constraints":[
         |  {
         |    "type": "Node label existence constraint",
         |    "label": "Actor",
         |    "enforcedLabel": "Person"
         |   }
         |  ]
         |}
         |""".stripMargin

    val graphCountData = GraphCountsJson.parseAsGraphCountDataFromString(json)
    val builder = plannerBuilder().processGraphCounts(graphCountData)
    builder.nodeLabelConstraints should contain(
      "Actor" -> Set("Person")
    )
  }

  test(
    "should throw if cardinality is not set for a relationship without type, nor for a matching typed relationships"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 100)
      .build()

    val q = "MATCH (a:A)-->(b) USING SCAN a:A RETURN count(*) AS res"
    val ex = intercept[IllegalStateException] {
      planner.plan(q).stripProduceResults
    }
    ex.getMessage should include("No cardinality set for relationship")
  }

  test(
    "should not throw if cardinality is not set for a relationship without type, but default-cardinality-to-0 is set"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 100)
      .defaultRelationshipCardinalityTo0()
      .build()

    val q = "MATCH (a:A)-->(b) USING SCAN a:A RETURN count(*) AS res"
    noException should be thrownBy {
      planner.plan(q).stripProduceResults
    }
  }

  test(
    "should not throw if cardinality is not set for a relationship without type, but is set for a matching typed relationship"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .setRelationshipCardinality("(:A)-[:REL]->()", 100)
      .build()

    val q = "MATCH (a:A)-->(b) USING SCAN a:A RETURN count(*) AS res"
    noException should be thrownBy {
      planner.plan(q).stripProduceResults
    }
  }

  test("storageIsMvcc should be true only for the Mvcc database format") {
    def planContextFromDatabaseFormat(format: DatabaseFormat): PlanContext =
      plannerBuilder()
        .setAllNodesCardinality(100)
        .setDatabaseFormat(format)
        .build()
        .planContext

    planContextFromDatabaseFormat(DatabaseFormat.Mvcc)
      .storageIsMvcc shouldEqual true
    planContextFromDatabaseFormat(DatabaseFormat.Block)
      .storageIsMvcc shouldEqual false
    planContextFromDatabaseFormat(DatabaseFormat.Aligned)
      .storageIsMvcc shouldEqual false
  }
}
