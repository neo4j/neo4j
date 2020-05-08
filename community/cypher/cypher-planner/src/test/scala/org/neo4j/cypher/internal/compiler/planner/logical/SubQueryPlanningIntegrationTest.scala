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
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SubQueryPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  // Uncorrelated subqueries

  test("CALL around single query") {
    val query = "CALL { RETURN 1 as x } RETURN 2 as y"

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("y")
        .projection("2 AS y")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("CALL around single query - using returned var in outer query") {
    val query = "CALL { RETURN 1 as x } RETURN x"
    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("1 AS x")
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
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("Simple nested subqueries") {
    val query = "CALL { CALL { CALL { RETURN 1 as x } RETURN x } RETURN x } RETURN x"
    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("1 AS x")
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
        .apply()
        .|.allNodeScan("c", "a", "b")
        .apply()
        .|.allNodeScan("b", "a")
        .allNodeScan("a")
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
        .distinct(s"$x2 AS $x2")
        .union()
        .|.projection(s"$x3 AS $x2")
        .|.projection(s"2 AS $x3")
        .|.argument()
        .projection(s"$x1 AS $x2")
        .projection(s"1 AS $x1")
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
        .distinct(s"$x2 AS $x2")
        .union()
        .|.projection(s"$x3 AS $x2")
        .|.projection(s"2 AS $x3")
        .|.argument()
        .projection(s"$x1 AS $x2")
        .projection(s"1 AS $x1")
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
        .apply()
        .|.allNodeScan("y", x2)
        .distinct(s"$x2 AS $x2")
        .union()
        .|.projection(s"$x3 AS $x2")
        .|.projection(s"2 AS $x3")
        .|.argument()
        .projection(s"$x1 AS $x2")
        .projection(s"1 AS $x1")
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
        .apply()
        .|.allNodeScan("y", x2)
        .distinct(s"$x2 AS $x2")
        .union()
        .|.projection(s"$x3 AS $x2")
        .|.projection(s"2 AS $x3")
        .|.argument()
        .projection(s"$x1 AS $x2")
        .projection(s"1 AS $x1")
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

  test("This should solve the aggregation on the RHS of the CartesianProduct") {
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
        .|.|.apply()
        .|.|.|.nodeByLabelScan(x2, "X", IndexOrderNone, "i")
        .|.|.unwind("range(0, 10) AS i")
        .|.|.argument()
        .|.projection(s"$sum1 AS $sum3")
        .|.aggregation(Seq.empty, Seq(s"sum(y.number) AS $sum1"))
        .|.nodeByLabelScan("y", "Y", IndexOrderNone)
        .expand(s"($x1)-[r]->(n)")
        .filter(s"$x1.prop = 5")
        .nodeByLabelScan(x1, "X", IndexOrderNone)
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

  test("should plan count store lookup in uncorrelated subquery") {
    val query =
      """MATCH (n)
        |CALL {
        | MATCH (x)-[r:REL]->(y)
        | RETURN count(*) AS c
        |}
        |RETURN n, c""".stripMargin

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("n", "c")
        .cartesianProduct()
        .|.relationshipCountFromCountStore("c", None, Seq("REL"), None)
        .allNodeScan("n")
        .build()
    )
  }

  // Correlated subqueries

  test("CALL around single correlated query") {
    val query = "WITH 1 AS x CALL { WITH x RETURN x as y } RETURN y"

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("y")
        .projection("x AS y")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("nested correlated subqueries") {
    val query = "WITH 1 AS a CALL { WITH a CALL { WITH a CALL { WITH a RETURN a AS b } RETURN b AS c } RETURN c AS d } RETURN d"
    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("d")
        .projection("c AS d")
        .projection("b AS c")
        .projection("a AS b")
        .projection("1 AS a")
        .argument()
        .build()
    )
  }

  test("CALL around correlated union query") {
    val query =
      """
        |WITH 1 AS x, 2 AS y CALL {
        |  WITH x RETURN x AS z
        |  UNION
        |  WITH y RETURN y AS z
        |} RETURN z""".stripMargin

    val Seq(z49, z53, z80) = namespaced("z", 49, 53, 80)

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults(z53)
        .apply()
        .|.distinct(s"$z53 AS $z53")
        .|.union()
        .|.|.projection(s"$z80 AS $z53")
        .|.|.projection(s"y AS $z80")
        .|.|.argument("y")
        .|.projection(s"$z49 AS $z53")
        .|.projection(s"x AS $z49")
        .|.argument("x")
        .projection("2 AS y", "1 AS x")
        .argument()
        .build()
    )
  }

  test("This should solve the aggregation on the RHS of the Apply") {
    val query = "WITH 1 AS x CALL { WITH x MATCH (y) WHERE y.value > x RETURN sum(y.prop) AS sum } RETURN sum"
    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("sum")
        .apply()
        .|.aggregation(Seq.empty, Seq("sum(y.prop) AS sum"))
        .|.filter("y.value > x")
        .|.allNodeScan("y", "x")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("correlated CALL in a sequence with ambiguous variable names") {
    val query = "WITH 1 AS x CALL { WITH x RETURN x as y } CALL { MATCH (x) RETURN 1 AS z } RETURN y"

    val Seq(x10, x56) = namespaced("x", 10, 56)

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("y")
        .cartesianProduct()
        .|.projection("1 AS z")
        .|.allNodeScan(x56)
        .projection(s"$x10 AS y")
        .projection(s"1 AS $x10")
        .argument()
        .build()
    )
  }

  test("nested correlated CALLs with aggregation") {
    val query =
      """WITH 1 AS x
        |CALL {
        | WITH x
        | CALL { WITH x RETURN max(x) AS xmax }
        | CALL { WITH x RETURN min(x) AS xmin }
        | RETURN xmax, xmin
        |}
        |RETURN x, xmax, xmin
        |""".stripMargin

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("x", "xmax", "xmin")
        .apply()
        .|.apply()
        .|.|.aggregation(Seq.empty, Seq("min(x) as xmin"))
        .|.|.argument("x")
        .|.aggregation(Seq.empty, Seq("max(x) as xmax"))
        .|.argument("x")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("correlated CALL with ordered aggregation") {
    val query =
      """WITH 1 AS x
        |CALL {
        | WITH x
        | WITH x AS y
        | ORDER BY y
        | RETURN y, max(y) as ymax
        |}
        |RETURN x, y, ymax
        |""".stripMargin

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("x", "y", "ymax")
        .apply()
        .|.orderedAggregation(Seq("y AS y"), Seq("max(y) AS ymax"), Seq("y") )
        .|.sort(Seq(Ascending("y")))
        .|.projection("x AS y")
        .|.argument("x")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("excessive aliasing should not confuse namespacer") {
    val query =
      """WITH 1 AS q
        |CALL {
        |  MATCH (a:A)
        |  RETURN a AS a, 1 AS b
        |  UNION
        |  WITH q
        |  MATCH (a:B)
        |  RETURN q AS b, a AS a
        |}
        |RETURN a AS q, b AS a, q AS b
        |""".stripMargin

    val Seq(a28, a59, a83, a134) = namespaced("a", 28, 59, 83, 134)
    val Seq(b55, b59, b102, b142) = namespaced("b", 55, 59, 102, 142)
    val Seq(q10, q126) = namespaced("q", 10, 126)

    planFor(query, stripProduceResults = false)._2 should equal {
      new LogicalPlanBuilder()
        .produceResults(q126, a134, b142)
        .projection(s"$a59 AS $q126", s"$b59 AS $a134", s"$q10 AS $b142")
        .apply()
        .|.distinct(s"$a59 AS $a59", s"$b59 AS $b59")
        .|.union()
        .|.|.projection(s"$a83 AS $a59", s"$b102 AS $b59")
        .|.|.projection(s"$q10 AS $b102")
        .|.|.nodeByLabelScan(a83, "B", IndexOrderNone, q10)
        .|.projection(s"$a28 AS $a59", s"$b55 AS $b59")
        .|.projection(s"1 AS $b55")
        .|.nodeByLabelScan(a28, "A", IndexOrderNone)
        .projection(s"1 AS $q10")
        .argument()
        .build()
    }
  }

  test("should not plan count store lookup in correlated subquery when node-variable is already bound") {
    val query =
      """MATCH (n)
        |CALL {
        | WITH n
        | MATCH (n)-[r:REL]->(m)
        | RETURN count(*) AS c
        |}
        |RETURN n, c""".stripMargin

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("n", "c")
        .apply()
        .|.aggregation(Seq.empty, Seq("count(*) AS c"))
        .|.expand("(n)-[r:REL]->(m)")
        .|.argument("n")
        .allNodeScan("n")
        .build()
    )
  }

  test("should not plan count store lookup in correlated subquery when relationship-variable is already bound") {
    val query =
      """MATCH (n)-[r:REL]->(m)
        |CALL {
        | WITH r
        | MATCH (x)-[r:REL]->(y)
        | RETURN count(*) AS c
        |}
        |RETURN n, c""".stripMargin

    planFor(query, stripProduceResults = false)._2 should equal(
      new LogicalPlanBuilder()
        .produceResults("n", "c")
        .apply()
        .|.aggregation(Seq.empty, Seq("count(*) AS c"))
        .|.projectEndpoints("(x)-[r:REL]->(y)", startInScope = false, endInScope = false)
        .|.argument("r")
        .expand("(m)<-[r:REL]-(n)")
        .allNodeScan("m")
        .build()
    )
  }
}
