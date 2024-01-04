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
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType
import org.scalatest.Assertion

import scala.collection.immutable.ListSet

class LimitPropagationPlanningIntegrationTest
    extends CypherFunSuite
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
      .setRelationshipCardinality("()-[:REL_AB]->()", 555)
      .setRelationshipCardinality("(:C)-[:REL_CB]->()", 4444)
      .setRelationshipCardinality("(:C)-[:REL_CB]->(:B)", 4444)
      .setRelationshipCardinality("()-[:REL_CB]->(:B)", 10000)
      .setRelationshipCardinality("()-[:REL_CB]->()", 10000)
      .addNodeIndex("A", Seq("id"), 0.5, 1.0 / 111.0, providesOrder = IndexOrderCapability.BOTH)
      .addNodeIndex("C", Seq("id"), 0.5, 1.0 / 2222.0, providesOrder = IndexOrderCapability.BOTH)
      .addRelationshipIndex(
        "REL_CB",
        Seq("id"),
        0.5,
        1.0 / 10000,
        providesOrder = IndexOrderCapability.BOTH,
        indexType = IndexType.RANGE
      )
      .addRelationshipIndex(
        "REL_CB",
        Seq("id"),
        0.5,
        1.0 / 10000,
        providesOrder = IndexOrderCapability.BOTH,
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
        .produceResults("a", "c")
        .limit(10)
        .nodeHashJoin("b")
        .|.expandAll("(c)-[cb:REL_CB]->(b)")
        .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .limit(1)
        .nodeHashJoin("b")
        .|.filterExpression(hasLabels("c", "C"))
        .|.relationshipIndexOperator(
          "(c)-[cb:REL_CB(id)]->(b)",
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .filterExpression(hasLabels("b", "B"))
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan lazy relationship index contains scan instead of sort when under limit") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE cb.id CONTAINS 'sub'
         |RETURN a, c ORDER BY cb.id LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults("a", "c")
        .limit(10)
        .nodeHashJoin("b")
        .|.filterExpression(hasLabels("c", "C"))
        .|.relationshipIndexOperator(
          "(c)-[cb:REL_CB(id CONTAINS 'sub')]->(b)",
          indexOrder = IndexOrderAscending,
          indexType = IndexType.TEXT
        )
        .filterExpression(hasLabels("b", "B"))
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan lazy relationship index ends with scan instead of sort when under limit") {
    val query =
      s"""
         |MATCH (a:A {id: 123})-[ab:REL_AB]->(b:B)
         |MATCH (c:C)-[cb:REL_CB]->(b) WHERE cb.id ENDS WITH 'suff'
         |RETURN a, c ORDER BY cb.id LIMIT 10
         |""".stripMargin

    assertExpectedPlanForQueryGivenStatistics(query, statisticsForLimitPropagationTests) { planBuilder =>
      planBuilder
        .produceResults("a", "c")
        .limit(10)
        .nodeHashJoin("b")
        .|.filterExpression(hasLabels("c", "C"))
        .|.relationshipIndexOperator(
          "(c)-[cb:REL_CB(id ENDS WITH 'suff')]->(b)",
          indexOrder = IndexOrderAscending,
          indexType = IndexType.TEXT
        )
        .filterExpression(hasLabels("b", "B"))
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .limit(1)
        .nodeHashJoin("b")
        .|.filterExpression(hasLabels("c", "C"))
        .|.relationshipIndexOperator(
          "(c)-[cb:REL_CB(id > 123)]->(b)",
          indexOrder = IndexOrderAscending,
          indexType = IndexType.RANGE
        )
        .filterExpression(hasLabels("b", "B"))
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .limit(10)
        .nodeHashJoin("b")
        .|.expandAll("(c)-[cb:REL_CB]->(b)")
        .|.nodeIndexOperator("c:C(id)", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .limit(10)
        .nodeHashJoin("b")
        .|.expandAll("(c)-[cb:REL_CB]->(b)")
        .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .limit(10)
        .distinct("a AS a", "c AS c")
        .nodeHashJoin("b")
        .|.expandAll("(c)-[cb:REL_CB]->(b)")
        .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .limit(10)
        .projection("a AS aaa")
        .projection("1 AS foo")
        .distinct("a AS a", "c AS c")
        .nodeHashJoin("b")
        .|.expandAll("(c)-[cb:REL_CB]->(b)")
        .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .|.expandAll("(c)-[cb:REL_CB]->(b)")
        .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
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
        .produceResults("a", "c")
        .skip(7)
        .limit(add(literalInt(10), literalInt(7)))
        .nodeHashJoin("b")
        .|.expandAll("(c)-[cb:REL_CB]->(b)")
        .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .skip(100000)
        .top(Seq(Ascending(v"c.id")), add(literalInt(10), literalInt(100000)))
        .projection("cache[c.id] AS `c.id`")
        .filter("c:C", "cacheNFromStore[c.id] STARTS WITH ''")
        .expandAll("(b)<-[cb:REL_CB]-(c)")
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .skip(100000)
        .top(Seq(Ascending(v"cb.id")), add(literalInt(1), literalInt(100000)))
        .projection("cacheR[cb.id] AS `cb.id`")
        .filterExpression(
          hasLabels("c", "C"),
          isNotNull(cachedRelPropFromStore("cb", "id"))
        )
        .expandAll("(b)<-[cb:REL_CB]-(c)")
        .filterExpression(hasLabels("b", "B"))
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .top(10, "`c.id` ASC")
        .projection("cache[c.id] AS `c.id`")
        .eager(ListSet(EagernessReason.Unknown))
        .setNodeProperty("b", "prop", "5")
        .filter("c:C", "cacheNFromStore[c.id] STARTS WITH ''")
        .expandAll("(b)<-[cb:REL_CB]-(c)")
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .skip(7)
        .limit(add(literalInt(10), literalInt(7)))
        .distinct("a AS a", "c AS c")
        .nodeHashJoin("b")
        .|.expandAll("(c)-[cb:REL_CB]->(b)")
        .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .skip(100000)
        .top(Seq(Ascending(v"c.id")), add(literalInt(10), literalInt(100000)))
        .projection("cache[c.id] AS `c.id`")
        .distinct("a AS a", "c AS c")
        .filter("c:C", "cacheNFromStore[c.id] STARTS WITH ''")
        .expandAll("(b)<-[cb:REL_CB]-(c)")
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .limit(10)
        .skip(7)
        .distinct("a AS a", "c AS c")
        .nodeHashJoin("b")
        .|.expandAll("(c)-[cb:REL_CB]->(b)")
        .|.nodeIndexOperator("c:C(id STARTS WITH '')", indexOrder = IndexOrderAscending, indexType = IndexType.RANGE)
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
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
        .produceResults("a", "c")
        .top(10, "`c.id` ASC")
        .projection("cache[c.id] AS `c.id`")
        .skip(100000)
        .distinct("a AS a", "c AS c")
        .filter("c:C", "cacheNFromStore[c.id] STARTS WITH ''")
        .expandAll("(b)<-[cb:REL_CB]-(c)")
        .filter("b:B")
        .expandAll("(a)-[ab:REL_AB]->(b)")
        .nodeIndexOperator("a:A(id = 123)", indexType = IndexType.RANGE)
        .build()
    }
  }
}
