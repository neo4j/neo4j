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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CachedPropertiesPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  test("should cache node property on multiple usages") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (n) WHERE n.prop1 > 42 RETURN n.prop1").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[n.prop1] AS `n.prop1`")
      .filter("cacheNFromStore[n.prop1] > 42")
      .allNodeScan("n")
      .build()
  }

  test("should cache node property on multiple usages without return") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (n) WHERE n.prop1 > 42 SET n.prop2 = n.prop1").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .setNodeProperty("n", "prop2", "cacheN[n.prop1]")
      .filter("cacheNFromStore[n.prop1] > 42")
      .allNodeScan("n")
      .build()
  }

  test("should not rewrite node property if there is only one usage") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (n) RETURN n.prop1").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("n.prop1 AS `n.prop1`")
      .allNodeScan("n")
      .build()
  }

  test("should not rewrite node property if there is only one usage in selection") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (n) WHERE n.prop1 > 42 RETURN n").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filter("n.prop1 > 42")
      .allNodeScan("n")
      .build()
  }

  test("should cache relationship property on multiple usages") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 50)
      .build()

    val plan = cfg.plan("MATCH (a)-[r]-(b) WHERE r.prop1 > 42 RETURN r.prop1").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop1] AS `r.prop1`")
      .filter("cacheRFromStore[r.prop1] > 42")
      .allRelationshipsScan("(a)-[r]-(b)")
      .build()
  }

  test("should not rewrite relationship property if there is only one usage") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 50)
      .build()

    val plan = cfg.plan("MATCH (a)-[r]-(b) RETURN r.prop1").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("r.prop1 AS `r.prop1`")
      .allRelationshipsScan("(a)-[r]-(b)")
      .build()
  }

  test("should cache renamed variable: n AS x") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (n) WHERE n.prop1 > 42 WITH n AS x RETURN x.prop1").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection(Map("x.prop1" -> cachedNodeProp("n", "prop1", "x")))
      .projection("n AS x")
      .filter("cacheNFromStore[n.prop1] > 42")
      .allNodeScan("n")
      .build()
  }

  test("should cache renamed variable: n AS x with predicate in between") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (n) WHERE n.prop1 > 42 WITH n AS x WHERE x.prop1 > 42 RETURN x").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filterExpression(greaterThan(cachedNodeProp("n", "prop1", "x"), literalInt(42)))
      .projection("n AS x")
      .filter("cacheNFromStore[n.prop1] > 42")
      .allNodeScan("n")
      .build()
  }

  test("should cache with byzantine renaming: n AS m, m AS x") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).enableDeduplicateNames(false).build()
    val plan = cfg.plan(
      "MATCH (n), (m) WHERE n.prop1 > 42 AND m.prop1 > 42 WITH n AS m, m AS x RETURN m.prop1, x.prop1"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection(Map(
        "m.prop1" -> cachedNodeProp("n", "prop1", "  m@1"),
        "x.prop1" -> cachedNodeProp("  m@0", "prop1", "x")
      ))
      .projection("n AS `  m@1`", "`  m@0` AS x")
      .cartesianProduct()
      .|.filter("cacheNFromStore[`  m@0`.prop1] > 42")
      .|.allNodeScan("`  m@0`")
      .filter("cacheNFromStore[n.prop1] > 42")
      .allNodeScan("n")
      .build()
  }

  test("should not push down property reads into RHS of apply unnecessarily") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1023)
      .setLabelCardinality("N", 12)
      .setLabelCardinality("M", 11)
      .setRelationshipCardinality("()-[]->()", 2000)
      .setRelationshipCardinality("(:N)-[]->()", 1000)
      .setRelationshipCardinality("(:N)-[]->(:M)", 2)
      .setRelationshipCardinality("()-[]->(:M)", 2)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N)
        |CALL {
        |  WITH n
        |  MATCH (n)-->(m:M)
        |  CALL {
        |    WITH n
        |    MATCH (n)-->(o:M)
        |    RETURN o
        |  }
        |  RETURN m, o
        |}
        |RETURN m.prop
        |""".stripMargin
    )

    val cachePropertyPlans = plan.folder.treeCount {
      case _: CacheProperties => ()
    }

    withClue(plan) {
      cachePropertyPlans should be(0)
    }
  }

  test("should push down property reads past a LIMIT if work is reduced by the LIMIT") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(500)
      .setLabelCardinality("N", 500)
      .setRelationshipCardinality("()-[]->()", 1000)
      .setRelationshipCardinality("(:N)-[]->()", 1000)
      .build()

    val plan = cfg.plan(
      """MATCH (n:N)-[rel]->(m)
        |WITH * LIMIT 10
        |RETURN n.prop AS foo
        |""".stripMargin
    )

    plan shouldEqual cfg.planBuilder()
      .produceResults("foo")
      .projection("cacheN[n.prop] AS foo") // 10 rows
      .limit(10) // 10 rows
      .expandAll("(n)-[rel]->(m)") // 1000 rows, effective 10
      .cacheProperties("cacheNFromStore[n.prop]")
      .nodeByLabelScan("n", "N") // 500 rows, effective 5
      .build()
  }

  test("should not get value for multiple IS NOT NULL checks on same node property") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (n) WHERE n.prop1 IS NOT NULL RETURN n.prop1 IS NOT NULL AS foo").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheNHasProperty[n.prop1] IS NOT NULL AS foo")
      .filter("cacheNHasPropertyFromStore[n.prop1] IS NOT NULL")
      .allNodeScan("n")
      .build()
  }

  test("should get value when having one IS NOT NULL check and one access on same node property") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (n) WHERE n.prop1 IS NOT NULL RETURN n.prop1 AS foo").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[n.prop1] AS foo")
      .filter("cacheNFromStore[n.prop1] IS NOT NULL")
      .allNodeScan("n")
      .build()
  }

  test("should not get value for multiple IS NOT NULL checks on same relationship property") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 50)
      .build()

    val plan =
      cfg.plan("MATCH (a)-[r]-(b) WHERE r.prop1 IS NOT NULL RETURN r.prop1 IS NOT NULL AS foo").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheRHasProperty[r.prop1] IS NOT NULL AS foo")
      .filter("cacheRHasPropertyFromStore[r.prop1] IS NOT NULL")
      .allRelationshipsScan("(a)-[r]-(b)")
      .build()
  }

  test("should get value when having one IS NOT NULL check and one access on same relationship property") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[]-()", 50)
      .build()

    val plan = cfg.plan("MATCH (a)-[r]-(b) WHERE r.prop1 IS NOT NULL RETURN r.prop1 AS foo").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop1] AS foo")
      .filter("cacheRFromStore[r.prop1] IS NOT NULL")
      .allRelationshipsScan("(a)-[r]-(b)")
      .build()
  }

  test("should plan caching of properties with the right names after projection") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:Type]->()", 20)
      .build()

    val query =
      """MATCH (n), (m)
        |WITH n AS a, m AS b
        |MERGE (a)-[r:Type]->(b)
        |RETURN a.id AS a, b.id AS b""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("a", "b")
        .projection(Map(
          "a" -> cachedNodeProp("n", "id", "a"),
          "b" -> cachedNodeProp("m", "id", "b")
        ))
        .apply()
        .|.merge(Seq(), Seq(createRelationship("r", "a", "Type", "b")), Seq(), Seq(), Set("a", "b"))
        .|.cacheProperties(Set[LogicalProperty](
          cachedNodeProp("n", "id", "a", knownToAccessStore = true),
          cachedNodeProp("m", "id", "b", knownToAccessStore = true)
        ))
        .|.expandInto("(a)-[r:Type]->(b)")
        .|.argument("a", "b")
        .projection("n AS a", "m AS b")
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )
  }

  test("NameDeduplication should not trigger OrderedIndexPlansUseCachedProperties to complain") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 10)
      .addNodeIndex("N", Seq("prop"), 1, 0.1)
      .build()

    val query =
      """MATCH (n:N) WHERE n.prop IS NOT NULL
        |WITH n.prop AS foo ORDER BY n.prop
        |MATCH (n) WHERE n.prop = 0 // This is a different n!
        |RETURN n
        |""".stripMargin

    // If we check OrderedIndexPlansUseCachedProperties after NameDeduplication,
    // it will complain that n.prop appears non-cached. But that is actually a different
    // `n`-Variable, so a false positive. We should therefore not check
    // OrderedIndexPlansUseCachedProperties after NameDeduplication.
    noException should be thrownBy planner.plan(query)
  }
}
