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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeFull
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationshipWithDynamicType
import org.neo4j.cypher.internal.logical.plans.DynamicElement
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone

class DynamicRelationshipTypeLookupPlanningIntegrationTest
    extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  final private val planner =
    plannerBuilder()
      .setAllNodesCardinality(120)
      .setLabelCardinality("A", 1)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("(:A)-[]->()", 10)
      .build()

  test("should plan dynamic relationship type scan with single type") {
    val query =
      """MATCH (a)-[r:$('R')]->(b)
        |RETURN a, r, b""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a", "r", "b")
        .dynamicRelationshipTypeLookup(
          leftNode = Some("a"),
          relName = Some("r"),
          relTypeExpr = literalString("R"),
          rightNode = Some("b"),
          direction = SemanticDirection.OUTGOING,
          operator = DynamicElement.All,
          indexOrder = IndexOrderNone,
          propertyPredicates = Map.empty,
          argumentIds = Set.empty
        )
        .build()
  }

  test("should plan dynamic relationship type scan with single type in a WHERE clause") {
    val query =
      """MATCH (a)-[r]->(b)
        |WHERE r:$('R')
        |RETURN a, r, b""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a", "r", "b")
        .dynamicRelationshipTypeLookup(
          leftNode = Some("a"),
          relName = Some("r"),
          relTypeExpr = literalString("R"),
          rightNode = Some("b"),
          direction = SemanticDirection.OUTGOING,
          operator = DynamicElement.All,
          indexOrder = IndexOrderNone,
          propertyPredicates = Map.empty,
          argumentIds = Set.empty
        )
        .build()
  }

  test("should plan dynamic relationship type scan for the disjunction of multiple types") {
    val query =
      """MATCH (a)-[r:$any(['R', 'S', 'T'])]-(b)
        |RETURN a, r, b""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a", "r", "b")
        .dynamicRelationshipTypeLookup(
          leftNode = Some("a"),
          relName = Some("r"),
          relTypeExpr = listOfString("R", "S", "T"),
          rightNode = Some("b"),
          direction = SemanticDirection.BOTH,
          operator = DynamicElement.Any,
          indexOrder = IndexOrderNone,
          propertyPredicates = Map.empty,
          argumentIds = Set.empty
        )
        .build()
  }

  test("should plan dynamic relationship type scan for types passed as parameters") {
    val query =
      """WITH ['R', 'S', 'T'] AS types
        |MATCH (a)<-[r:$any(types)]-(b)
        |RETURN a, r, b""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a", "r", "b")
        .apply()
        .|.dynamicRelationshipTypeLookup(
          leftNode = Some("a"),
          relName = Some("r"),
          relTypeExpr = varFor("types"),
          rightNode = Some("b"),
          direction = SemanticDirection.INCOMING,
          operator = DynamicElement.Any,
          indexOrder = IndexOrderNone,
          propertyPredicates = Map.empty,
          argumentIds = Set("types")
        )
        .projection("['R', 'S', 'T'] AS types")
        .argument()
        .build()
  }

  test("should plan dynamic relationship type scan with complex expression in predicate") {
    val query =
      """MATCH (a:A)
        |WITH labels(a) AS labels
        |MATCH (x)-[r:$any(labels)]-(y)
        |RETURN x, r, y""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("x", "r", "y")
        .apply()
        .|.dynamicRelationshipTypeLookup(
          leftNode = Some("x"),
          relName = Some("r"),
          relTypeExpr = varFor("labels"),
          rightNode = Some("y"),
          direction = SemanticDirection.BOTH,
          operator = DynamicElement.Any,
          indexOrder = IndexOrderNone,
          propertyPredicates = Map.empty,
          argumentIds = Set("labels")
        )
        .projection("labels(a) AS labels")
        .nodeByLabelScan("a", "A", IndexOrderNone)
        .build()
  }

  test("should plan dynamic relationship type scan with dynamic types in MERGE") {
    val query =
      """MERGE ()-[r:$('R')]->()
        |RETURN r""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("r")
        .merge(
          nodes = Seq(createNodeFull("anon_0"), createNodeFull("anon_1")),
          relationships = Seq(
            createRelationshipWithDynamicType("r", "anon_0", "'R'", "anon_1", SemanticDirection.OUTGOING)
          )
        )
        .dynamicRelationshipTypeLookup(
          leftNode = Some("anon_0"),
          relName = Some("r"),
          relTypeExpr = literalString("R"),
          rightNode = Some("anon_1"),
          direction = SemanticDirection.OUTGOING,
          operator = DynamicElement.All,
          indexOrder = IndexOrderNone,
          propertyPredicates = Map.empty,
          argumentIds = Set.empty
        )
        .build()
  }

  test("should not plan dynamic relationship type scan if cypher_enable_dynamic_label_scan is false") {
    val query =
      """MATCH (a)-[r:$('R')]->(b)
        |RETURN a, r, b""".stripMargin

    val plan = plannerBuilder()
      .enablePlanningDynamicLabelScans(false)
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(20)
      .build()
      .plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a", "r", "b")
        .filter(hasDynamicType(varFor("r"), literalString("R")))
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
  }

  test("should not plan dynamic relationship type scan if relationship lookup index is not available") {
    val query =
      """MATCH (a)-[r:$('R')]->(b)
        |RETURN a, r, b""".stripMargin

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(20)
      .removeRelationshipLookupIndex()
      .build()
      .plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a", "r", "b")
        .filter(hasDynamicType(varFor("r"), literalString("R")))
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
  }

  test("should plan dynamic relationship type scan ordered by the variable name") {
    val query =
      """MATCH (a)-[r:$('R')]->(b)
        |RETURN a, r, b ORDER BY r""".stripMargin

    val plan = planner
      .plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a", "r", "b")
        .dynamicRelationshipTypeLookup(
          leftNode = Some("a"),
          relName = Some("r"),
          relTypeExpr = literalString("R"),
          rightNode = Some("b"),
          direction = SemanticDirection.OUTGOING,
          operator = DynamicElement.All,
          indexOrder = IndexOrderAscending,
          propertyPredicates = Map.empty,
          argumentIds = Set.empty
        )
        .build()
  }

  test("should plan dynamic relationship type scan ordered by the variable name renamed") {
    val query =
      """MATCH (a)-[r:$('R')]-(b)
        |RETURN a, r AS s, b ORDER BY s DESC""".stripMargin

    val plan = planner
      .plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a", "s", "b")
        .projection("r AS s")
        .dynamicRelationshipTypeLookup(
          leftNode = Some("a"),
          relName = Some("r"),
          relTypeExpr = literalString("R"),
          rightNode = Some("b"),
          direction = SemanticDirection.BOTH,
          operator = DynamicElement.All,
          indexOrder = IndexOrderDescending,
          propertyPredicates = Map.empty,
          argumentIds = Set.empty
        )
        .build()
  }

  test("should not plan index usage when setting is disabled") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(100)
      .enablePlanningDynamicLabelIndexUse(false)
      .build()

    val query = "MATCH (a)-[r:$('R')]-(b) WHERE r.prop = 123 RETURN r"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("r.prop = 123")
      .dynamicRelationshipTypeLookup("()-[r]-()", "$all('R')")
      .build()
  }

  test("should plan index usage for equality predicate") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query = "MATCH (a)-[r:$('R')]-(b) WHERE r.prop = 123 RETURN r"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .dynamicRelationshipTypeLookup("()-[r]-()", "$all('R')", propertyPredicates = Map("prop" -> "123"))
      .build()
  }

  test("should plan index for multiple equality predicates on different properties") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query =
      """MATCH (a)-[r:$('R') {name: 'hello', version: 123}]->(b)
        |WHERE r.location = point({x:22.0, y:44.0})
        |RETURN r
        |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .dynamicRelationshipTypeLookup(
        "()-[r]->()",
        "$all('R')",
        propertyPredicates = Map("name" -> "'hello'", "version" -> "123", "location" -> "point({x:22.0, y:44.0})")
      )
      .build()
  }

  test("should only plan index usage for known properties") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(100)
      .addProperty("name")
      .addProperty("version")
      .addProperty("location")
      .setAutoResolvePropertiesDuringPlanning(false)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query =
      """MATCH (a)-[r:$('R') {name: 'hello', version: 123}]->(b)
        |WHERE
        |  r.location = point({x:22.0, y:44.0}) AND
        |  r.unknown = 321
        |RETURN r
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("r.unknown = 321")
      .dynamicRelationshipTypeLookup(
        "()-[r]->()",
        "$all('R')",
        propertyPredicates = Map("name" -> "'hello'", "version" -> "123", "location" -> "point({x:22.0, y:44.0})")
      )
      .build()
  }

  test("should plan index usage for equality predicate with argument dependencies") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query =
      """MATCH (x)
        |CALL (x) {
        |  MATCH (a)-[r:$('R')]-(b)
        |  WHERE r.prop = x.name
        |  RETURN r
        |}
        |RETURN x, r
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .apply()
      .|.dynamicRelationshipTypeLookup(
        "()-[r]-()",
        "$all('R')",
        propertyPredicates = Map("prop" -> "x.name"),
        argumentIds = Set("x")
      )
      .allNodeScan("x")
      .build()
  }

  test("should not plan index usage for predicate on unrelated variable") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query =
      """MATCH (x)
        |CALL (x) {
        |  MATCH (a)-[r:$('R')]->(b)
        |  WHERE r.prop = x.name AND x.other = 123
        |  RETURN r
        |}
        |RETURN x, r
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("cacheN[x.other] = 123")
      .apply()
      .|.dynamicRelationshipTypeLookup(
        "()-[r]->()",
        "$all('R')",
        propertyPredicates = Map("prop" -> "x.name"),
        argumentIds = Set("x")
      )
      .cacheProperties("cacheNFromStore[x.other]")
      .allNodeScan("x")
      .build()
  }

  test("should not plan index usage for non-equality predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val unsupportedPredicates = Seq(
      "r.prop IS NOT NULL",
      "r.prop STARTS WITH 'hello'",
      "r.prop ENDS WITH 'hello'",
      "r.prop CONTAINS 'hello'",
      "r.prop > 123"
    )

    for (pred <- unsupportedPredicates) {

      val query = s"MATCH (a)-[r:$$('R')]->(b) WHERE r.x = 123 AND $pred RETURN r"
      val plan = planner.plan(query).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(pred)
        .dynamicRelationshipTypeLookup("()-[r]->()", "$all('R')", propertyPredicates = Map("x" -> "123"))
        .build()
    }
  }

  test("should plan index usage for one equality predicate if there are multiple predicates on the same property") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query = "MATCH (a)-[r:$('R')]-(b) WHERE r.x = 123 AND r.x = 321 RETURN r"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("r.x = 321")
      .dynamicRelationshipTypeLookup("()-[r]-()", "$all('R')", propertyPredicates = Map("x" -> "123"))
      .build()
  }
}
