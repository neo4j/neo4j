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

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.LimitBeforeCountRewriter.limitSafeExpressionFrom
import org.neo4j.cypher.internal.logical.plans.LogicalPlanAstConstructionTestSupport

class LimitBeforeCountPlanningIntegrationTest extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with LogicalPlanAstConstructionTestSupport {

  private val supportedOperators: Seq[String] = Seq(">", ">=", "<", "<=", "=") // <> is rewritten to NOT =

  test("should plan limit before count() aggregation") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 1000)
      .build()

    for {
      op <- supportedOperators
      isCountStar <- Seq(true, false)
    } withClue(s"operator: $op, isCountStar: $isCountStar\n\n") {

      val queryExpr = if (isCountStar) "*" else "n.prop"
      val countExpr = if (isCountStar) "*" else "cacheN[n.prop]"
      val filterExpr = if (isCountStar) "" else "cacheNFromStore[n.prop]"

      val query = s"MATCH (n:A) USING SCAN n:A RETURN count($queryExpr) $op 10 AS result"
      val plan = planner.plan(query)
      plan shouldEqual planner.planBuilder()
        .produceResults("result")
        .projection(s"anon_0 $op 10 AS result")
        .aggregation(Seq.empty, Seq(s"count($countExpr) AS anon_0"))
        .limit(limitSafeExpressionFrom(literal(10)))
        .filterIfNotCountStar(isCountStar, s"$filterExpr IS NOT NULL")
        .nodeByLabelScan("n", "A")
        .build()
    }
  }

  test("should plan limit before count() aggregation, not equals predicate") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 1000)
      .build()

    for {
      isCountStar <- Seq(true, false)
    } withClue(s"isCountStar: $isCountStar\n\n") {

      val queryExpr = if (isCountStar) "*" else "n.prop"
      val countExpr = if (isCountStar) "*" else "cacheN[n.prop]"
      val filterExpr = if (isCountStar) "" else "cacheNFromStore[n.prop]"

      val query = s"MATCH (n:A) USING SCAN n:A RETURN count($queryExpr) <> 10 AS result"
      val plan = planner.plan(query)
      plan shouldEqual planner.planBuilder()
        .produceResults("result")
        .projection(s"NOT anon_0 = 10 AS result")
        .aggregation(Seq.empty, Seq(s"count($countExpr) AS anon_0"))
        .limit(limitSafeExpressionFrom(literal(10)))
        .filterIfNotCountStar(isCountStar, s"$filterExpr IS NOT NULL")
        .nodeByLabelScan("n", "A")
        .build()
    }
  }

  test("should plan limit before count() aggregation in CALL") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(5000)
      .build()

    for {
      op <- supportedOperators
      isCountStar <- Seq(true, false)
    } withClue(s"operator: $op, isCountStar: $isCountStar\n\n") {

      val queryExpr = if (isCountStar) "*" else "n.prop"
      val countExpr = if (isCountStar) "*" else "cacheN[n.prop]"
      val filterExpr = if (isCountStar) "" else "cacheNFromStore[n.prop]"

      val query =
        s"""MATCH (n)
           |CALL (n) {
           |  MATCH (n)-[r]->(m)
           |  RETURN count($queryExpr) $op 10 AS result
           |}
           |RETURN n.x AS x, result
           |""".stripMargin

      val plan = planner.plan(query)
      plan shouldEqual planner.planBuilder()
        .produceResults("x", "result")
        .projection("n.x AS x")
        .projection(s"anon_0 $op 10 AS result")
        .apply()
        .|.aggregation(Seq.empty, Seq(s"count($countExpr) AS anon_0"))
        .|.limit(limitSafeExpressionFrom(literal(10)))
        .|.filterIfNotCountStar(isCountStar, s"$filterExpr IS NOT NULL")
        .|.expandAll("(n)-[]->()")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    }
  }

  test("should not plan limit before count() aggregation if result is used") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 1000)
      .build()

    for {
      op <- supportedOperators
      isCountStar <- Seq(true, false)
    } withClue(s"operator: $op, isCountStar: $isCountStar\n\n") {
      val countExpr = if (isCountStar) "*" else "n.prop"
      val query =
        s"""MATCH (n:A) USING SCAN n:A
           |WITH count($countExpr) AS c
           |RETURN c, c $op 10 AS result
           |""".stripMargin

      val plan = planner.plan(query)
      plan shouldEqual planner.planBuilder()
        .produceResults("c", "result")
        .projection(s"c $op 10 AS result")
        .aggregation(Seq.empty, Seq(s"count($countExpr) AS c"))
        .nodeByLabelScan("n", "A")
        .build()
    }
  }

  test("should plan limit for COUNT {} in WHERE") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(5000)
      .build()

    for {
      op <- supportedOperators
    } withClue(s"operator: $op\n\n") {

      val query =
        s"""MATCH (n)
           |WHERE COUNT { (n)-[r]->(m) WHERE n.prop > m.prop } $op 10
           |RETURN n.x AS x
           |""".stripMargin

      val plan = planner.plan(query)
      plan shouldEqual planner.planBuilder()
        .produceResults("x")
        .projection("n.x AS x")
        .filter(s"anon_0 $op 10")
        .apply()
        .|.aggregation(Seq.empty, Seq(s"count(*) AS anon_0"))
        .|.limit(limitSafeExpressionFrom(literal(10)))
        .|.filter("n.prop > m.prop")
        .|.expandAll("(n)-[]->(m)")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    }
  }

  test("should plan limit for COUNT {} in RETURN") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10000)
      .setAllRelationshipsCardinality(5000)
      .build()

    for {
      op <- supportedOperators
    } withClue(s"operator: $op\n\n") {

      val query =
        s"""MATCH (n)
           |RETURN COUNT { (n)-[r]->(m) WHERE n.prop > m.prop } $op 10 AS result
           |""".stripMargin

      val plan = planner.plan(query)
      plan shouldEqual planner.planBuilder()
        .produceResults("result")
        .projection(s"anon_0 $op 10 AS result")
        .apply()
        .|.aggregation(Seq.empty, Seq(s"count(*) AS anon_0"))
        .|.limit(limitSafeExpressionFrom(literal(10)))
        .|.filter("n.prop > m.prop")
        .|.expandAll("(n)-[]->(m)")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    }
  }

  implicit private class FilterIfNotCountStarExtension(planBuilder: LogicalPlanBuilder) {

    def filterIfNotCountStar(isCountStar: Boolean, filterExpression: String): LogicalPlanBuilder =
      if (isCountStar) {
        planBuilder.resetIndent()
      } else {
        planBuilder.filter(filterExpression)
      }
  }
}
