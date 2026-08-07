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
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeFull
import org.neo4j.cypher.internal.logical.plans.DynamicElement
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone

class DynamicLabelNodeLookupPlanningIntegrationTest
    extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  final private val planner =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .build()

  test("should plan dynamic label scan with single label") {
    val query =
      """MATCH (a:$('A'))
        |RETURN a""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .dynamicLabelNodeLookup("a", literalString("A"), DynamicElement.All)
        .build()
  }

  test("should plan dynamic label scan with single label in a WHERE clause") {
    val query =
      """MATCH (a)
        |WHERE a:$('A')
        |RETURN a""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .dynamicLabelNodeLookup("a", literalString("A"), DynamicElement.All)
        .build()
  }

  test("probably could but currently does not combine dynamic labels in node pattern and in a WHERE clause") {
    val query =
      """MATCH (a:$('A'))
        |WHERE a:$('B')
        |RETURN a""".stripMargin

    val plan = planner.plan(CypherVersion.Cypher25, query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .filter(hasDynamicLabels(varFor("a"), literalString("B")))
        .dynamicLabelNodeLookup("a", literalString("A"), DynamicElement.All)
        .build()
  }

  test("should plan dynamic label scan with multiple labels – conjunction") {
    val query =
      """MATCH (a:$(['A', 'B']))
        |RETURN a""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .dynamicLabelNodeLookup("a", listOfString("A", "B"), DynamicElement.All)
        .build()
  }

  test("should plan dynamic label scan with multiple labels – disjunction") {
    val query =
      """MATCH (a:$any(['A', 'B']))
        |RETURN a""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .dynamicLabelNodeLookup("a", listOfString("A", "B"), DynamicElement.Any)
        .build()
  }

  test("should plan dynamic label scan with multiple dynamic labels predicates") {
    val query =
      """MATCH (a:$(['A', 'B']) & $('C'))
        |RETURN a""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .filter(hasDynamicLabels(varFor("a"), literalString("C")))
        .dynamicLabelNodeLookup("a", listOfString("A", "B"), DynamicElement.All)
        .build()
  }

  test("should plan dynamic label scan given a combination of negated conjunction and disjunction") {
    val query =
      """WITH ['A', 'B'] AS labels
        |MATCH (a:!$all(labels) & $any(labels))
        |RETURN a""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .filter(not(hasDynamicLabels(varFor("a"), varFor("labels"))))
        .apply()
        .|.dynamicLabelNodeLookup("a", varFor("labels"), DynamicElement.Any, "labels")
        .projection("['A', 'B'] AS labels")
        .argument()
        .build()
  }

  test("should plan dynamic label scan with complex expression in predicate") {
    val query =
      """MATCH (a:A)
        |WITH labels(a) AS labels
        |MATCH (b:$(labels))
        |RETURN b""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("b")
        .apply()
        .|.dynamicLabelNodeLookup("b", varFor("labels"), DynamicElement.All, "labels")
        .projection("labels(a) AS labels")
        .nodeByLabelScan("a", "A", IndexOrderNone)
        .build()
  }

  test("should plan a union of static and dynamic label scans") {
    val query =
      """MATCH (a:A | $(['B', 'C']))
        |RETURN a""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .distinct("a AS a")
        .union()
        .|.dynamicLabelNodeLookup("a", listOfString("B", "C"), DynamicElement.All)
        .nodeByLabelScan("a", "A", IndexOrderNone)
        .build()
  }

  test("should not plan dynamic label scan for an argument") {
    val query =
      """MATCH (a)
        |WITH a SKIP 0
        |MATCH (a:$('A'))
        |RETURN a""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .filter(hasDynamicLabels(varFor("a"), literalString("A")), assertIsNode("a"))
        .skip(0)
        .allNodeScan("a")
        .build()
  }

  test("should not plan dynamic label scan if cypher_enable_dynamic_label_scan is false") {
    val query =
      """MATCH (a:$('A'))
        |RETURN a""".stripMargin

    val plan = plannerBuilder()
      .enablePlanningDynamicLabelScans(false)
      .setAllNodesCardinality(100)
      .build()
      .plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .filter(hasDynamicLabels(varFor("a"), literalString("A")))
        .allNodeScan("a")
        .build()
  }

  test("should not plan dynamic label scan if node lookup index is not available") {
    val query =
      """MATCH (a:$('A'))
        |RETURN a""".stripMargin

    val plan = plannerBuilder()
      .setAllNodesCardinality(100)
      .removeNodeLookupIndex()
      .build()
      .plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .filter(hasDynamicLabels(varFor("a"), literalString("A")))
        .allNodeScan("a")
        .build()
  }

  test("should plan dynamic label scan ordered by the variable name by sorting afterwards") {
    val query =
      """MATCH (a:$('A'))
        |RETURN a ORDER BY a""".stripMargin

    val plan = planner
      .plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .sort("a ASC")
        .dynamicLabelNodeLookup("a", literalString("A"), DynamicElement.All)
        .build()
  }

  test("should plan dynamic label scan with dynamic labels in MERGE") {
    val query =
      """MERGE (a:$('A'))
        |RETURN a""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("a")
        .merge(Seq(createNodeFull("a", dynamicLabels = Seq("'A'"))))
        .dynamicLabelNodeLookup("a", literalString("A"), DynamicElement.All)
        .build()
  }

  test("should plan dynamic label scan with sub-query label expression") {
    val query =
      """MATCH (n:$(COLLECT { UNWIND ['A', 'B'] AS label RETURN label }))
        |RETURN n""".stripMargin

    val plan = planner.plan(query)

    plan shouldEqual
      planner.planBuilder()
        .produceResults("n")
        .apply()
        .|.dynamicLabelNodeLookup("n", varFor("anon_0"), DynamicElement.All, "anon_0")
        .rollUpApply("anon_0", "label")
        .|.unwind("['A', 'B'] AS label")
        .|.argument()
        .argument()
        .build()
  }

  test("should not plan index usage when setting is disabled") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .enablePlanningDynamicLabelIndexUse(false)
      .build()

    val query = "MATCH (n:$('A')) WHERE n.prop = 123 RETURN n"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("n.prop = 123")
      .dynamicLabelNodeLookup("n", literalString("A"), DynamicElement.All)
      .build()
  }

  test("should plan index usage for equality predicate") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query = "MATCH (n:$('A')) WHERE n.prop = 123 RETURN n"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .dynamicLabelNodeLookup("n", "'A'", DynamicElement.All, Map("prop" -> "123"))
      .build()
  }

  test("should plan index for multiple equality predicates on different properties") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query =
      """MATCH (n:$('A') {name: 'hello', version: 123})
        |WHERE n.location = point({x:22.0, y:44.0})
        |RETURN n
        |""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .dynamicLabelNodeLookup(
        "n",
        "'A'",
        DynamicElement.All,
        Map("name" -> "'hello'", "version" -> "123", "location" -> "point({x:22.0, y:44.0})")
      )
      .build()
  }

  test("should only plan index usage for known properties") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .addProperty("name")
      .addProperty("version")
      .addProperty("location")
      .setAutoResolvePropertiesDuringPlanning(false)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query =
      """MATCH (n:$('A') {name: 'hello', version: 123})
        |WHERE
        |  n.location = point({x:22.0, y:44.0}) AND
        |  n.unknown = 321
        |RETURN n
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("n.unknown = 321")
      .dynamicLabelNodeLookup(
        "n",
        "'A'",
        DynamicElement.All,
        Map("name" -> "'hello'", "version" -> "123", "location" -> "point({x:22.0, y:44.0})")
      )
      .build()
  }

  test("should plan index usage for equality predicate with argument dependencies") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query =
      """MATCH (x)
        |CALL (x) {
        |  MATCH (n:$('A'))
        |  WHERE n.prop = x.name
        |  RETURN n
        |}
        |RETURN x, n
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .apply()
      .|.dynamicLabelNodeLookup("n", "'A'", DynamicElement.All, Map("prop" -> "x.name"), "x")
      .allNodeScan("x")
      .build()
  }

  test("should not plan index usage for predicate on unrelated variable") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query =
      """MATCH (x)
        |CALL (x) {
        |  MATCH (n:$('A'))
        |  WHERE n.prop = x.name AND x.other = 123
        |  RETURN n
        |}
        |RETURN x, n
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("cacheN[x.other] = 123")
      .apply()
      .|.dynamicLabelNodeLookup("n", "'A'", DynamicElement.All, Map("prop" -> "x.name"), "x")
      .cacheProperties("cacheNFromStore[x.other]")
      .allNodeScan("x")
      .build()
  }

  test("should not plan index usage for non-equality predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val unsupportedPredicates = Seq(
      "n.prop IS NOT NULL",
      "n.prop STARTS WITH 'hello'",
      "n.prop ENDS WITH 'hello'",
      "n.prop CONTAINS 'hello'",
      "n.prop > 123"
    )

    for (pred <- unsupportedPredicates) {

      val query = s"MATCH (n:$$('A')) WHERE n.x = 123 AND $pred RETURN n"
      val plan = planner.plan(query).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(pred)
        .dynamicLabelNodeLookup("n", "'A'", DynamicElement.All, Map("x" -> "123"))
        .build()
    }
  }

  test("should plan index usage for one equality predicate if there are multiple predicates on the same property") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .enablePlanningDynamicLabelIndexUse()
      .build()

    val query = "MATCH (n:$('A')) WHERE n.x = 123 AND n.x = 321 RETURN n"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("n.x = 321")
      .dynamicLabelNodeLookup("n", "'A'", DynamicElement.All, Map("x" -> "123"))
      .build()
  }
}
