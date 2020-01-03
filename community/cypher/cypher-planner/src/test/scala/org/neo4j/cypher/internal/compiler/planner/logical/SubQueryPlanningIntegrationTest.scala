/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SubQueryPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  // Uncorrelated subqueries

  test("CALL around single query") {
    val query = "CALL { RETURN 1 as x } RETURN 2 as y"

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("y")
        .projection("2 AS y")
        .cartesianProduct()
        .|.projection("1 AS x")
        .|.argument()
        .argument()
        .build()
    )
  }

  test("CALL around single query - using returned var in outer query") {
    val query = "CALL { RETURN 1 as x } RETURN x"
    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .cartesianProduct()
        .|.projection("1 AS x")
        .|.argument()
        .argument()
        .build()
    )
  }

  test("CALLs in sequence") {
    val query = "CALL { RETURN 1 AS x } CALL { RETURN 2 AS y } RETURN x, y"
    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("x", "y")
        .cartesianProduct()
        .|.projection("2 AS y")
        .|.argument()
        .cartesianProduct()
        .|.projection("1 AS x")
        .|.argument()
        .argument()
        .build()
    )
  }

  test("Simple nested subqueries") {
    val query = "CALL { CALL { CALL { RETURN 1 as x } RETURN x } RETURN x } RETURN x"
    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .cartesianProduct()
        .|.cartesianProduct()
        .|.|.cartesianProduct()
        .|.|.|.projection("1 AS x")
        .|.|.|.argument()
        .|.|.argument()
        .|.argument()
        .argument()
        .build()
    )
  }

  test("Nested subqueries") {
    val query =
      """CALL {
        |  CALL {
        |    CALL {
        |      MATCH (a) RETURN a
        |    }
        |    MATCH (b) RETURN a, b
        |  }
        |  MATCH (c) RETURN a, b, c
        |}
        |RETURN a, b, c""".stripMargin
    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("a", "b", "c")
        .cartesianProduct()
        .|.apply()
        .|.|.allNodeScan("c", "a", "b")
        .|.cartesianProduct()
        .|.|.apply()
        .|.|.|.allNodeScan("b", "a")
        .|.|.cartesianProduct()
        .|.|.|.allNodeScan("a")
        .|.|.argument()
        .|.argument()
        .argument()
        .build()
    )
  }

  test("CALL around union query") {
    val query = "CALL { RETURN 1 as x UNION RETURN 2 as x } RETURN 3 as y"
    val Seq(x1, x2, x3) = namespaced("x", 19, 21, 39)

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("y")
        .projection("3 AS y")
        .cartesianProduct()
        .|.distinct(s"$x2 AS $x2")
        .|.union()
        .|.|.projection(s"$x3 AS $x2")
        .|.|.projection(s"2 AS $x3")
        .|.|.argument()
        .|.projection(s"$x1 AS $x2")
        .|.projection(s"1 AS $x1")
        .|.argument()
        .argument()
        .build()
    )
  }

  test("CALL around union query - using returned var in outer query") {
    val query = "CALL { RETURN 1 as x UNION RETURN 2 as x } RETURN x"
    val Seq(x1, x2, x3) = namespaced("x", 19, 21, 39)

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults(x2)
        .cartesianProduct()
        .|.distinct(s"$x2 AS $x2")
        .|.union()
        .|.|.projection(s"$x3 AS $x2")
        .|.|.projection(s"2 AS $x3")
        .|.|.argument()
        .|.projection(s"$x1 AS $x2")
        .|.projection(s"1 AS $x1")
        .|.argument()
        .argument()
        .build()
    )
  }

  test("CALL around union query - using returned var in outer query with MATCH afterwards") {
    val query = "CALL { RETURN 1 as x UNION RETURN 2 as x } MATCH (y) WHERE y.prop = x RETURN y"
    val Seq(x1, x2, x3) = namespaced("x", 19, 21, 39)

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("y")
        .filter(s"y.prop = $x2")
        .cartesianProduct()
        .|.allNodeScan("y", x2)
        .cartesianProduct()
        .|.distinct(s"$x2 AS $x2")
        .|.union()
        .|.|.projection(s"$x3 AS $x2")
        .|.|.projection(s"2 AS $x3")
        .|.|.argument()
        .|.projection(s"$x1 AS $x2")
        .|.projection(s"1 AS $x1")
        .|.argument()
        .argument()
        .build()
    )
  }

  test("CALL around union query - using returned var in outer query with STUFF afterwards") {
    val query = "CALL { RETURN 1 as x UNION RETURN 2 as x } MATCH (y) WHERE y.prop = x RETURN sum(y.number) AS sum"
    val Seq(x1, x2, x3) = namespaced("x", 19, 21, 39)

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("sum")
        .aggregation(Seq.empty, Seq("sum(y.number) AS sum"))
        .filter(s"y.prop = $x2")
        .cartesianProduct()
        .|.allNodeScan("y", x2)
        .cartesianProduct()
        .|.distinct(s"$x2 AS $x2")
        .|.union()
        .|.|.projection(s"$x3 AS $x2")
        .|.|.projection(s"2 AS $x3")
        .|.|.argument()
        .|.projection(s"$x1 AS $x2")
        .|.projection(s"1 AS $x1")
        .|.argument()
        .argument()
        .build()
    )
  }

  test("This should solve the WHERE clause") {
    val query = "WITH 1 AS x CALL { MATCH (y) WHERE y.prop = 5 RETURN y } RETURN x + 1 AS res, y"
    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("res", "y")
        .projection("x + 1 AS res")
        .cartesianProduct()
        .|.filter("y.prop = 5")
        .|.allNodeScan("y")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("This should solve the aggregation on the RHS of the Apply") {
    val query = "WITH 1 AS x CALL { MATCH (y) RETURN sum(y.prop) AS sum } RETURN x + 1 AS res, sum"
    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("res", "sum")
        .projection("x + 1 AS res")
        .cartesianProduct()
        .|.aggregation(Seq.empty, Seq("sum(y.prop) AS sum"))
        .|.allNodeScan("y")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("Complex query") {
    val query =
      """MATCH (x:X)-[r]->(n) WHERE x.prop = 5
        |CALL {
        |  MATCH (y:Y) RETURN sum(y.number) AS sum
        |   UNION
        |  UNWIND range(0, 10) AS i MATCH (x:X) WHERE x.prop = i RETURN sum(x.number) AS sum
        |}
        |RETURN count(n) AS c, sum""".stripMargin

    val Seq(x1, x2) = namespaced("x", 7, 130)
    val Seq(sum1, sum2, sum3) = namespaced("sum", 83, 176, 90)

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("c", sum3)
        .aggregation(Seq(s"$sum3 AS $sum3"), Seq("count(n) AS c"))
        .cartesianProduct()
        .|.distinct(s"$sum3 AS $sum3")
        .|.union()
        .|.|.projection(s"$sum2 AS $sum3")
        .|.|.aggregation(Seq.empty, Seq(s"sum($x2.number) AS $sum2"))
        .|.|.filter(s"$x2.prop = i")
        .|.|.cartesianProduct()
        .|.|.|.nodeByLabelScan(x2, "X", "i")
        .|.|.unwind("range(0, 10) AS i")
        .|.|.argument()
        .|.projection(s"$sum1 AS $sum3")
        .|.aggregation(Seq.empty, Seq(s"sum(y.number) AS $sum1"))
        .|.nodeByLabelScan("y", "Y")
        .expand(s"($x1)-[r]->(n)")
        .filter(s"$x1.prop = 5")
        .nodeByLabelScan(x1, "X")
        .build()
    )
  }

  test("Should treat variables with the same name but different scopes correctly") {
    // Here x and x are two different things
    val query = "MATCH (x) CALL { MATCH (y)-[r]->(x) RETURN y } RETURN 5 AS five"
    val Seq(x1, x2) = namespaced("x", 7, 33)

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("five")
        .projection("5 AS five")
        .cartesianProduct()
        .|.expand(s"($x2)<-[r]-(y)")
        .|.allNodeScan(x2)
        .allNodeScan(x1)
        .build()
    )
  }
}
