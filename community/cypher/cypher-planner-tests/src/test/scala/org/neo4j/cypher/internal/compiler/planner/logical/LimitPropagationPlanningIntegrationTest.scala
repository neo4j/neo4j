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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.column
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.graphdb.schema.IndexType
import org.scalatest.Assertion

class LimitPropagationPlanningIntegrationTest
    extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  private def statisticsForLimitPropagationTests(plannerBuilder: StatisticsBackedLogicalPlanningConfigurationBuilder) =
    plannerBuilder
      .setAllNodesCardinality(3333)
      .setLabelCardinality("A", 111)
      .setLabelCardinality("B", 7)
      .setLabelCardinality("C", 2222)
      .setRelationshipCardinality("(:A)-[:REL_AB]->()", 123)
      .setRelationshipCardinality("(:A)-[:REL_AB]->(:B)", 123)
      .setRelationshipCardinality("()-[:REL_AB]->(:B)", 555)
      .setRelationshipCardinality("(:B)-[:REL_AB]->()", 0)
      .setRelationshipCardinality("()-[:REL_AB]->()", 555)
      .setRelationshipCardinality("(:C)-[:REL_CB]->()", 4444)
      .setRelationshipCardinality("(:C)-[:REL_CB]->(:B)", 4444)
      .setRelationshipCardinality("(:B)-[:REL_CB]->()", 0)
      .setRelationshipCardinality("()-[:REL_CB]->(:B)", 10000)
      .setRelationshipCardinality("()-[:REL_CB]->()", 10000)
      .setRelationshipCardinality("(:B)-[:REL_AB]->()", 0)
      .setRelationshipCardinality("(:B)-[:REL_CB]->()", 0)
      .setRelationshipCardinality("()-[:REL_AB]->(:A)", 0)
      .setRelationshipCardinality("()-[:REL_CB]->(:C)", 0)
      .addNodeIndex("A", Seq("id"), 0.5, 1.0 / 111.0)
      .addNodeIndex("C", Seq("id"), 0.5, 1.0 / 2222.0)
      .addRelationshipIndex(
        "REL_CB",
        Seq("id"),
        0.5,
        1.0 / 10000,
        indexType = IndexType.RANGE
      )
      .addRelationshipIndex(
        "REL_CB",
        Seq("id"),
        0.5,
        1.0 / 10000,
        indexType = IndexType.TEXT
      )
      .build()

  private def assertExpectedPlanForQueryGivenStatistics(
    queryString: String,
    buildStats: StatisticsBackedLogicalPlanningConfigurationBuilder => StatisticsBackedLogicalPlanningConfiguration
  )(buildExpectedPlan: LogicalPlanBuilder => LogicalPlan): Assertion = {
    val cfg = buildStats(plannerBuilder())
    val plan = cfg.plan(queryString)
    plan shouldEqual buildExpectedPlan(cfg.planBuilder())
  }

  test("should plan lazy index seek instead of sort when under limit") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |RETURN a, c ORDER BY c.id LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .limit(10)
        .nodeHashJoin("b")
        .|.expandAll("(c)-[:REL_CB]->(b)")
        .|.nodeIndexOperator(
          "c:C(id STARTS WITH '')",
          _ => GetValue,
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan lazy relationship index scan instead of sort when under limit") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE cb.id IS NOT NULL
         |RETURN a, c ORDER BY cb.id LIMIT 1
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c"))
        .limit(1)
        .nodeHashJoin("b")
        .|.filter(hasLabels("c", "C"))
        .|.relationshipIndexOperator(
          "(c)-[:REL_CB(id)]->(b)",
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .filter(hasLabels("b", "B"))
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan lazy relationship index seek instead of sort when under limit") {
    val query =
      """
        |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
        |MATCH (c:C)-[cb:REL_CB]->(b) WHERE cb.id > 123
        |RETURN a, c ORDER BY cb.id LIMIT 1
        |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c"))
        .limit(1)
        .nodeHashJoin("b")
        .|.filter(hasLabels("c", "C"))
        .|.relationshipIndexOperator(
          "(c)-[:REL_CB(id > 123)]->(b)",
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .filter(hasLabels("b", "B"))
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan lazy index scan instead of sort when under limit") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id IS NOT NULL
         |RETURN a, c ORDER BY c.id LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .limit(10)
        .nodeHashJoin("b")
        .|.expandAll("(c)-[:REL_CB]->(b)")
        .|.nodeIndexOperator("c:C(id)", _ => GetValue, indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan lazy index seek instead of sort when limit is in a different query part") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |WITH a, c ORDER BY c.id
         |RETURN a, c LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .limit(10)
        .nodeHashJoin("b")
        .|.expandAll("(c)-[:REL_CB]->(b)")
        .|.nodeIndexOperator(
          "c:C(id STARTS WITH '')",
          _ => GetValue,
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan lazy index seek instead of sort when sort and limit are in a different query part") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |WITH DISTINCT a, c
         |RETURN a, c ORDER BY c.id LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .limit(10)
        .distinct("a AS a", "c AS c")
        .nodeHashJoin("b")
        .|.expandAll("(c)-[:REL_CB]->(b)")
        .|.nodeIndexOperator(
          "c:C(id STARTS WITH '')",
          _ => GetValue,
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test(
    "should plan lazy index seek instead of sort when sort and limit are in a different query part with many horizons inbetween"
  ) {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |WITH DISTINCT a, c
         |WITH *, 1 AS foo
         |CALL {
         |  WITH a
         |  RETURN a AS aaa
         |}
         |RETURN a, c ORDER BY c.id LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .limit(10)
        .projection("a AS aaa")
        .projection("1 AS foo")
        .distinct("a AS a", "c AS c")
        .nodeHashJoin("b")
        .|.expandAll("(c)-[:REL_CB]->(b)")
        .|.nodeIndexOperator(
          "c:C(id STARTS WITH '')",
          _ => GetValue,
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan lazy index seek instead of sort when under limit and aggregation in the next query part") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |WITH a, c ORDER BY c.id LIMIT 10
         |RETURN count(*) AS count
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .limit(10)
        .nodeHashJoin("b")
        .|.expandAll("(c)-[:REL_CB]->(b)")
        .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan lazy index seek instead of sort when under limit and small skip") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |RETURN a, c ORDER BY c.id
         |SKIP 7 LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .skip(7)
        .limit(add(literalInt(10), literalInt(7)))
        .nodeHashJoin("b")
        .|.expandAll("(c)-[:REL_CB]->(b)")
        .|.nodeIndexOperator(
          "c:C(id STARTS WITH '')",
          _ => GetValue,
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should not plan lazy index seek instead of sort when under limit and large skip") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |RETURN a, c ORDER BY c.id
         |SKIP 100000 LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .skip(100000)
        .top(Seq(Ascending(v"c.id")), add(literalInt(10), literalInt(100000)))
        .projection("cache[c.id] AS `c.id`")
        .filter("c:C", "cacheNFromStore[c.id] STARTS WITH ''")
        .expandAll("(b)<-[:REL_CB]-(c)")
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should not plan lazy relationship index scan instead of sort when under limit and large skip") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE cb.id IS NOT NULL
         |RETURN a, c ORDER BY cb.id
         |SKIP 100000 LIMIT 1
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c"))
        .skip(100000)
        .top(Seq(Ascending(v"cb.id")), add(literalInt(1), literalInt(100000)))
        .projection("cacheR[cb.id] AS `cb.id`")
        .filter(
          hasLabels("c", "C"),
          isNotNull(cachedRelPropFromStore("cb", "id"))
        )
        .expandAll("(b)<-[cb:REL_CB]-(c)")
        .filter(hasLabels("b", "B"))
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should not plan lazy index seek when updates before the limit") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |SET b.prop = 5
         |RETURN a, c ORDER BY c.id
         |LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .top(10, "`c.id` ASC")
        .projection("cache[c.id] AS `c.id`")
        .setNodeProperty("b", "prop", "5")
        .filter("c:C", "cacheNFromStore[c.id] STARTS WITH ''")
        .expandAll("(b)<-[:REL_CB]-(c)")
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan lazy index seek instead of sort when under limit and small skip in the next query part") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |WITH DISTINCT a, c
         |RETURN a, c ORDER BY c.id
         |SKIP 7 LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .skip(7)
        .limit(add(literalInt(10), literalInt(7)))
        .distinct("a AS a", "c AS c")
        .nodeHashJoin("b")
        .|.expandAll("(c)-[:REL_CB]->(b)")
        .|.nodeIndexOperator(
          "c:C(id STARTS WITH '')",
          _ => GetValue,
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should not plan lazy index seek instead of sort when under limit and large skip in the next query part") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |WITH DISTINCT a, c
         |RETURN a, c ORDER BY c.id
         |SKIP 100000 LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .skip(100000)
        .top(Seq(Ascending(v"c.id")), add(literalInt(10), literalInt(100000)))
        .projection("cache[c.id] AS `c.id`")
        .distinct("a AS a", "c AS c")
        .filter("c:C", "cacheNFromStore[c.id] STARTS WITH ''")
        .expandAll("(b)<-[:REL_CB]-(c)")
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test(
    "should plan lazy index seek instead of sort when under small skip in same query part and limit and in the next query part"
  ) {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |WITH DISTINCT a, c
         |SKIP 7
         |RETURN a, c ORDER BY c.id
         |LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .limit(10)
        .skip(7)
        .distinct("a AS a", "c AS c")
        .nodeHashJoin("b")
        .|.expandAll("(c)-[:REL_CB]->(b)")
        .|.nodeIndexOperator(
          "c:C(id STARTS WITH '')",
          _ => GetValue,
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test(
    "should not plan lazy index seek instead of sort when under large skip in same query part and limit in the next query part"
  ) {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE c.id STARTS WITH ''
         |WITH DISTINCT a, c
         |SKIP 100000
         |RETURN a, c ORDER BY c.id
         |LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults(column("a", "cacheN[a.id]"), column("c", "cacheN[c.id]"))
        .top(10, "`c.id` ASC")
        .projection("cache[c.id] AS `c.id`")
        .skip(100000)
        .distinct("a AS a", "c AS c")
        .filter("c:C", "cacheNFromStore[c.id] STARTS WITH ''")
        .expandAll("(b)<-[:REL_CB]-(c)")
        .filter("b:B")
        .expandAll("(a)-[:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should not plan node hash join under EXISTS when expand is cheaper with implicit LIMIT 1") {
    val relCount = 50000
    val fromStartRelCount = relCount * 0.1
    val fromEndRelCount = relCount * 0.2

    val planner = plannerBuilder()
      .setAllNodesCardinality(relCount)
      .setAllRelationshipsCardinality(relCount)
      .setLabelCardinality("Middle", 1)
      .setLabelCardinality("Start", fromStartRelCount)
      .setLabelCardinality("End", fromEndRelCount)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("()-[:REL]->(:Middle)", relCount)
      .setRelationshipCardinality("(:End)-[:REL]->(:Middle)", fromEndRelCount)
      .setRelationshipCardinality("(:End)-[:REL]->()", fromEndRelCount)
      .setRelationshipCardinality("(:Start)-[:REL]->(:Middle)", fromStartRelCount)
      .setRelationshipCardinality("(:Start)-[:REL]->()", fromStartRelCount)
      .build()

    val subQuery =
      """
        |MATCH (a:Start)-[r:REL]->(x:Middle)<-[p:REL]-(b:End)
        |RETURN a, x, b
        |""".stripMargin

    val existsQuery = s"RETURN EXISTS { $subQuery } AS result"
    val countQuery = s"RETURN COUNT { $subQuery } AS result"

    planner.plan(existsQuery) shouldEqual planner.planBuilder()
      .produceResults("result")
      .letSemiApply("result")
      .|.filter("b:End", "NOT p = r")
      .|.expandAll("(x)<-[p:REL]-(b)")
      .|.filter("x:Middle")
      .|.expandAll("(a)-[r:REL]->(x)")
      .|.nodeByLabelScan("a", "Start")
      .argument()
      .build()

    planner.plan(countQuery) shouldEqual planner.planBuilder()
      .produceResults("result")
      .aggregation(Seq.empty, Seq("count(*) AS result"))
      .filter("NOT p = r")
      .nodeHashJoin("x")
      .|.expandAll("(b)-[p:REL]->(x)")
      .|.nodeByLabelScan("b", "End")
      .filter("x:Middle")
      .expandAll("(a)-[r:REL]->(x)")
      .nodeByLabelScan("a", "Start")
      .build()

    // standalone query
    planner.plan(subQuery) shouldEqual planner.planBuilder()
      .produceResults("a", "x", "b")
      .filter("NOT p = r")
      .nodeHashJoin("x")
      .|.expandAll("(b)-[p:REL]->(x)")
      .|.nodeByLabelScan("b", "End")
      .filter("x:Middle")
      .expandAll("(a)-[r:REL]->(x)")
      .nodeByLabelScan("a", "Start")
      .build()
  }

  test("should plan node hash join under EXISTS when it's still cheaper with implicit LIMIT 1") {
    val relCount = 50000
    val fromStartRelCount = 10
    val fromEndRelCount = 20

    val planner = plannerBuilder()
      .setAllNodesCardinality(relCount)
      .setAllRelationshipsCardinality(relCount)
      .setLabelCardinality("Middle", 1)
      .setLabelCardinality("Start", fromStartRelCount)
      .setLabelCardinality("End", fromEndRelCount)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("()-[:REL]->(:Middle)", relCount)
      .setRelationshipCardinality("(:End)-[:REL]->(:Middle)", fromEndRelCount)
      .setRelationshipCardinality("(:End)-[:REL]->()", fromEndRelCount)
      .setRelationshipCardinality("(:Start)-[:REL]->(:Middle)", fromStartRelCount)
      .setRelationshipCardinality("(:Start)-[:REL]->()", fromStartRelCount)
      .build()

    val query =
      """RETURN EXISTS {
        |  MATCH (a:Start)-[r:REL]->(x:Middle)<-[p:REL]-(b:End)
        |  RETURN a, x, b
        |} AS result
        |""".stripMargin

    planner.plan(query) shouldEqual planner.planBuilder()
      .produceResults("result")
      .letSemiApply("result")
      .|.filter("NOT p = r")
      .|.nodeHashJoin("x")
      .|.|.expandAll("(b)-[p:REL]->(x)")
      .|.|.nodeByLabelScan("b", "End")
      .|.filter("x:Middle")
      .|.expandAll("(a)-[r:REL]->(x)")
      .|.nodeByLabelScan("a", "Start")
      .argument()
      .build()
  }

  test("should not plan node hash join under EXISTS when expand is cheaper with implicit LIMIT, predicate in WHERE") {
    val relCount = 50000
    val fromStartRelCount = relCount * 0.1
    val fromEndRelCount = relCount * 0.2
    val nodeCount = 5
    val hasStartCount = 5 * fromStartRelCount

    val planner = plannerBuilder()
      .setAllNodesCardinality(relCount)
      .setAllRelationshipsCardinality(relCount + hasStartCount)
      .setLabelCardinality("Middle", 1)
      .setLabelCardinality("Start", fromStartRelCount)
      .setLabelCardinality("End", fromEndRelCount)
      .setLabelCardinality("Node", nodeCount)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("()-[:REL]->(:Middle)", relCount)
      .setRelationshipCardinality("(:End)-[:REL]->(:Middle)", fromEndRelCount)
      .setRelationshipCardinality("(:End)-[:REL]->()", fromEndRelCount)
      .setRelationshipCardinality("(:Start)-[:REL]->(:Middle)", fromStartRelCount)
      .setRelationshipCardinality("(:Start)-[:REL]->()", fromStartRelCount)
      .setRelationshipCardinality("()-[:HAS_START]->()", hasStartCount)
      .setRelationshipCardinality("(:Node)-[:HAS_START]->()", hasStartCount)
      .setRelationshipCardinality("(:Node)-[:HAS_START]->(:Start)", hasStartCount)
      .setRelationshipCardinality("()-[:HAS_START]->(:Start)", hasStartCount)
      .build()

    val subQuery = "MATCH (n)-[z:HAS_START]->(a:Start)-[r:REL]->(x:Middle)<-[p:REL]-(b:End)"

    val existsQuery =
      s"""MATCH (n:Node) WHERE EXISTS {
         |  $subQuery
         |}
         |RETURN n
         |""".stripMargin

    val countQuery =
      s"""MATCH (n:Node) WHERE COUNT {
         |  $subQuery
         |} > n.prop
         |RETURN n
         |""".stripMargin

    val skipQuery = s"MATCH (n:Node) SKIP 0 $subQuery RETURN n"

    planner.plan(existsQuery) shouldEqual planner.planBuilder()
      .produceResults("n")
      .semiApply()
      .|.filter("NOT p = r", "b:End")
      .|.expandAll("(x)<-[p:REL]-(b)")
      .|.filter("x:Middle")
      .|.expandAll("(a)-[r:REL]->(x)")
      .|.filter("a:Start")
      .|.expandAll("(n)-[:HAS_START]->(a)")
      .|.argument("n")
      .nodeByLabelScan("n", "Node")
      .build()

    planner.plan(countQuery) shouldEqual planner.planBuilder()
      .produceResults("n")
      .filter("n.prop < anon_0")
      .apply()
      .|.aggregation(Seq.empty, Seq("count(*) AS anon_0"))
      .|.filter("NOT p = r")
      .|.nodeHashJoin("x")
      .|.|.expandAll("(b)-[p:REL]->(x)")
      .|.|.nodeByLabelScan("b", "End", "n")
      .|.filter("x:Middle")
      .|.expandAll("(a)-[r:REL]->(x)")
      .|.filter("a:Start")
      .|.expandAll("(n)-[:HAS_START]->(a)")
      .|.argument("n")
      .nodeByLabelScan("n", "Node")
      .build()

    planner.plan(skipQuery) shouldEqual planner.planBuilder()
      .produceResults("n")
      .filter("NOT p = r")
      .apply()
      .|.nodeHashJoin("x")
      .|.|.expandAll("(b)-[p:REL]->(x)")
      .|.|.nodeByLabelScan("b", "End", "n")
      .|.filter("x:Middle")
      .|.expandAll("(a)-[r:REL]->(x)")
      .|.filter("a:Start")
      .|.expandAll("(n)-[:HAS_START]->(a)")
      .|.argument("n")
      .skip(0)
      .nodeByLabelScan("n", "Node")
      .build()
  }

  test("should prefer intersection scan under LIMIT") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 700)
      .setLabelCardinality("B", 200)
      .build()

    val plan = planner.plan("MATCH (a:A&B) RETURN a LIMIT 20").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .limit(20)
      .intersectionNodeByLabelsScan("a", Seq("A", "B"))
      .build()
  }

  test("should prefer union scan under LIMIT") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 700)
      .setLabelCardinality("B", 200)
      .build()

    val plan = planner.plan("MATCH (ab:A|B) RETURN ab LIMIT 20").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .limit(20)
      .unionNodeByLabelsScan("ab", Seq("A", "B"))
      .build()
  }

  test("should prefer subtraction scan under LIMIT") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 700)
      .setLabelCardinality("B", 200)
      .build()

    val plan = planner.plan("MATCH (a:A&!B) RETURN a LIMIT 20").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .limit(20)
      .subtractionNodeByLabelsScan("a", Seq("A"), Seq("B"))
      .build()
  }
}
