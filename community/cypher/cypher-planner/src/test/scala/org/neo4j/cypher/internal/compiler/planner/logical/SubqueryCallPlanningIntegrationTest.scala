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
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorBreak
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorContinue
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SubqueryCallPlanningIntegrationTest
    extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with LogicalPlanningAttributesTestSupport {

  private def planFor(query: String): LogicalPlan = {
    plannerBuilder()
      .setAllNodesCardinality(1000)
      .build()
      .plan(query)
  }

  private def plannerCfgBuilder: StatisticsBackedLogicalPlanningConfigurationBuilder = {
    plannerBuilder()
      .setAllNodesCardinality(1000)
  }

  // Uncorrelated subqueries

  test("CALL around single query") {
    val query = "CALL { RETURN 1 as x } RETURN 2 as y"

    planFor(query) should equal(
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
    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("CALLs in sequence") {
    val query = "CALL { RETURN 1 AS x } CALL { RETURN 2 AS y } RETURN x, y"
    planFor(query) should equal(
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
    planFor(query) should equal(
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
    planFor(query) should equal(
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

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("y")
        .projection("3 AS y")
        .distinct("x AS x")
        .union()
        .|.projection("x AS x")
        .|.projection("2 AS x")
        .|.argument()
        .projection("x AS x")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("CALL around union query - using returned var in outer query") {
    val query = "CALL { RETURN 1 as x UNION RETURN 2 as x } RETURN x"

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .distinct("x AS x")
        .union()
        .|.projection("x AS x")
        .|.projection("2 AS x")
        .|.argument()
        .projection("x AS x")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("CALL around union query - using returned var in outer query with MATCH afterwards") {
    val query = "CALL { RETURN 1 as x UNION RETURN 2 as x } MATCH (y) WHERE y.prop = x RETURN y"

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("y")
        .filter("y.prop = x")
        .apply()
        .|.allNodeScan("y", "x")
        .distinct("x AS x")
        .union()
        .|.projection("x AS x")
        .|.projection("2 AS x")
        .|.argument()
        .projection("x AS x")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("CALL around union query - using returned var in outer query with STUFF afterwards") {
    val query = "CALL { RETURN 1 as x UNION RETURN 2 as x } MATCH (y) WHERE y.prop = x RETURN sum(y.number) AS sum"

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("sum")
        .aggregation(Seq.empty, Seq("sum(y.number) AS sum"))
        .filter("y.prop = x")
        .apply()
        .|.allNodeScan("y", "x")
        .distinct("x AS x")
        .union()
        .|.projection("x AS x")
        .|.projection("2 AS x")
        .|.argument()
        .projection("x AS x")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("This should solve the WHERE clause") {
    val query = "WITH 1 AS x CALL { MATCH (y) WHERE y.prop = 5 RETURN y } RETURN x + 1 AS res, y"
    planFor(query) should equal(
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
    planFor(query) should equal(
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
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("X", 100)
      .setLabelCardinality("Y", 200)
      .setRelationshipCardinality("(:X)-[]->()", 100)
      .setRelationshipCardinality("()-[]->()", 500)
      .build()

    val query =
      """MATCH (x:X)-[r]->(n) WHERE x.prop = 5
        |CALL {
        |  MATCH (y:Y) RETURN sum(y.number) AS sum
        |   UNION
        |  UNWIND range(0, 10) AS i MATCH (x:X) WHERE x.prop = i RETURN sum(x.number) AS sum
        |}
        |RETURN count(n) AS c, sum""".stripMargin

    cfg.plan(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("c", "sum")
        .aggregation(Seq("sum AS sum"), Seq("count(n) AS c"))
        .cartesianProduct()
        .|.distinct("sum AS sum")
        .|.union()
        .|.|.projection("sum AS sum")
        .|.|.aggregation(Seq.empty, Seq("sum(x.number) AS sum"))
        .|.|.filter("x.prop = i")
        .|.|.apply()
        .|.|.|.nodeByLabelScan("x", "X", IndexOrderNone, "i")
        .|.|.unwind("range(0, 10) AS i")
        .|.|.argument()
        .|.projection("sum AS sum")
        .|.aggregation(Seq.empty, Seq("sum(y.number) AS sum"))
        .|.nodeByLabelScan("y", "Y", IndexOrderNone)
        .expand("(x)-[r]->(n)")
        .filter("x.prop = 5")
        .nodeByLabelScan("x", "X", IndexOrderNone)
        .build()
    )
  }

  test("Should treat variables with the same name but different scopes correctly") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("X", 100)
      .setRelationshipCardinality("()-[]->(:X)", 100)
      .setRelationshipCardinality("()-[]->()", 200)
      .build()

    // Here x and x are two different things
    val query = "MATCH (x) CALL { MATCH (y)-[r]->(x:X) RETURN y } RETURN 5 AS five"

    cfg.plan(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("five")
        .projection("5 AS five")
        .cartesianProduct()
        .|.expand("(x)<-[r]-(y)")
        .|.nodeByLabelScan("x", "X")
        .allNodeScan("x")
        .build()
    )
  }

  test("should plan count store lookup in uncorrelated subquery") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .build()

    val query =
      """MATCH (n)
        |CALL {
        | MATCH (x)-[r:REL]->(y)
        | RETURN count(*) AS c
        |}
        |RETURN n, c""".stripMargin

    cfg.plan(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("n", "c")
        .cartesianProduct()
        .|.relationshipCountFromCountStore("c", None, Seq("REL"), None)
        .allNodeScan("n")
        .build()
    )
  }

  test("Query containing unit subquery CALL ending with CREATE") {
    val query = "UNWIND [1, 2] AS x CALL { CREATE (n:N) } RETURN x"

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .subqueryForeach()
        .|.create(createNode("n", "N"))
        .|.argument()
        .unwind("[1, 2] AS x")
        .argument()
        .build()
    )
  }

  test("Query containing unit subquery CALL under LIMIT") {
    val query = "UNWIND [1, 2] AS x CALL { CREATE (n:N) } RETURN x LIMIT 0"

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .exhaustiveLimit(0)
        .subqueryForeach()
        .|.create(createNode("n", "N"))
        .|.argument()
        .unwind("[1, 2] AS x")
        .argument()
        .build()
    )
  }

  test("Query containing unit union subquery CALL") {
    val query = "UNWIND [1, 2] AS x CALL { CREATE (n:N) UNION CREATE (n:N) } RETURN x"

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .subqueryForeach()
        .|.union()
        .|.|.projection()
        .|.|.create(createNode("n", "N"))
        .|.|.argument()
        .|.projection()
        .|.create(createNode("n", "N"))
        .|.argument()
        .unwind("[1, 2] AS x")
        .argument()
        .build()
    )
  }

  test("Query containing unit union subquery CALL under LIMIT") {
    val query = "UNWIND [1, 2] AS x CALL { CREATE (n:N) UNION CREATE (n:N) } RETURN x LIMIT 0"

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("x")
        .exhaustiveLimit(0)
        .subqueryForeach()
        .|.union()
        .|.|.projection()
        .|.|.create(createNode("n", "N"))
        .|.|.argument()
        .|.projection()
        .|.create(createNode("n", "N"))
        .|.argument()
        .unwind("[1, 2] AS x")
        .argument()
        .build()
    )
  }

  test("Query containing unit subquery CALL ending with void procedure CALL") {
    val query = "WITH 1 AS x CALL { WITH 2 AS y CALL my.println(y) } RETURN x"

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .addProcedure(
        procedureSignature("my.println")
          .withInputField("value", CTInteger)
          .withAccessMode(ProcedureReadWriteAccess)
          .build()
      )
      .build()

    planner
      .plan(query)
      .shouldEqual(planner
        .planBuilder()
        .produceResults("x")
        .subqueryForeach()
        .|.procedureCall("my.println(y)")
        .|.projection("2 AS y")
        .|.argument()
        .projection("1 AS x")
        .argument()
        .build())
  }

  test("Query ending with unit subquery CALL") {
    val query = "UNWIND [1, 2] AS x CALL { CREATE (n:N) }"

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .emptyResult()
        .subqueryForeach()
        .|.create(createNode("n", "N"))
        .|.argument()
        .unwind("[1, 2] AS x")
        .argument()
        .build()
    )
  }

  test("Query ending with unit union subquery CALL") {
    val query = "UNWIND [1, 2] AS x CALL { CREATE (n:N) UNION CREATE (n:N) }"

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .emptyResult()
        .subqueryForeach()
        .|.union()
        .|.|.projection()
        .|.|.create(createNode("n", "N"))
        .|.|.argument()
        .|.projection()
        .|.create(createNode("n", "N"))
        .|.argument()
        .unwind("[1, 2] AS x")
        .argument()
        .build()
    )
  }

  test("Query ending with nested unit subquery CALL") {
    val query = "UNWIND [1, 2] AS x CALL { CALL { CREATE (n:N) } }"

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults()
        .emptyResult()
        .subqueryForeach()
        .|.subqueryForeach()
        .|.|.create(createNode("n", "N"))
        .|.|.argument()
        .|.argument()
        .unwind("[1, 2] AS x")
        .argument()
        .build()
    )
  }

  test("CALL unit subquery should not affect incoming cardinality estimate") {
    val query = "UNWIND [1, 2] AS x CALL { UNWIND [1, 2, 3] AS x CREATE (n:N)} RETURN x"

    val planner = plannerBuilder().setAllNodesCardinality(1000).build()
    val actual = planner.planState(query)

    val expected = new LogicalPlanBuilder()
      .produceResults("x").withCardinality(2)
      .subqueryForeach().withCardinality(2) // <-- here we take the cardinality from unwind and ignore the one from RHS
      .|.create(createNode("n", "N")).withCardinality(3)
      .|.unwind("[1, 2, 3] AS x").withCardinality(3)
      .|.argument().withCardinality(1)
      .unwind("[1, 2] AS x").withCardinality(2)
      .argument().withCardinality(1)

    actual should haveSamePlanAndCardinalitiesAsBuilder(expected)
  }

  // Correlated subqueries

  test("CALL around single correlated query") {
    val query = "WITH 1 AS x CALL { WITH x RETURN x as y } RETURN y"

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("y")
        .projection("x AS y")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("nested correlated subqueries") {
    val query =
      "WITH 1 AS a CALL { WITH a CALL { WITH a CALL { WITH a RETURN a AS b } RETURN b AS c } RETURN c AS d } RETURN d"
    planFor(query) should equal(
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

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("z")
        .apply()
        .|.distinct(s"z AS z")
        .|.union()
        .|.|.projection(s"z AS z")
        .|.|.projection(s"y AS z")
        .|.|.argument("y")
        .|.projection(s"z AS z")
        .|.projection(s"x AS z")
        .|.argument("x")
        .projection("2 AS y", "1 AS x")
        .argument()
        .build()
    )
  }

  test("This should solve the aggregation on the RHS of the Apply") {
    val query = "WITH 1 AS x CALL { WITH x MATCH (y) WHERE y.value > x RETURN sum(y.prop) AS sum } RETURN sum"
    planFor(query) should equal(
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

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("y")
        .cartesianProduct()
        .|.projection("1 AS z")
        .|.allNodeScan("x")
        .projection(s"x AS y")
        .projection(s"1 AS x")
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

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("x", "xmax", "xmin")
        .apply()
        .|.apply()
        .|.|.aggregation(Seq.empty, Seq("min(x) as xmin"))
        .|.|.argument("x")
        .|.apply()
        .|.|.aggregation(Seq(), Seq("max(x) AS xmax"))
        .|.|.argument("x")
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

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("x", "y", "ymax")
        .apply()
        .|.orderedAggregation(Seq("y AS y"), Seq("max(y) AS ymax"), Seq("y"))
        .|.sort("y ASC")
        .|.projection("x AS y")
        .|.argument("x")
        .projection("1 AS x")
        .argument()
        .build()
    )
  }

  test("excessive aliasing should not confuse namespacer") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 100)
      .setLabelCardinality("B", 100)
      .enableDeduplicateNames(false)
      .build()

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

    cfg.plan(query) should equal {
      new LogicalPlanBuilder()
        .produceResults("`  q@7`", "`  a@8`", "`  b@9`")
        .projection("`  a@5` AS `  q@7`", "`  b@6` AS `  a@8`", "`  q@0` AS `  b@9`")
        .apply()
        .|.distinct("`  a@5` AS `  a@5`", "`  b@6` AS `  b@6`")
        .|.union()
        .|.|.projection("`  a@3` AS `  a@5`", "`  b@4` AS `  b@6`")
        .|.|.projection("`  q@0` AS `  b@4`")
        .|.|.nodeByLabelScan("`  a@3`", "B", IndexOrderNone, "`  q@0`")
        .|.projection("`  a@1` AS `  a@5`", "`  b@2` AS `  b@6`")
        .|.projection("1 AS `  b@2`")
        .|.nodeByLabelScan("`  a@1`", "A", IndexOrderNone)
        .projection("1 AS `  q@0`")
        .argument()
        .build()
    }
  }

  test("should not plan count store lookup in correlated subquery when node-variable is already bound") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 10000)
      .build()

    val query =
      """MATCH (n)
        |CALL {
        | WITH n
        | MATCH (n)-[r:REL]->(m)
        | RETURN count(*) AS c
        |}
        |RETURN n, c""".stripMargin

    cfg.plan(query) should equal(
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
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("M", 100)
      .setRelationshipCardinality("()-[:REL]->()", 200)
      .setRelationshipCardinality("()-[:REL]->(:M)", 100)
      .build()

    val query =
      """MATCH (n)-[r:REL]->(m:M)
        |CALL {
        | WITH r
        | MATCH (x)-[r:REL]->(y)
        | RETURN count(*) AS c
        |}
        |RETURN n, c""".stripMargin

    cfg.plan(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("n", "c")
        .apply()
        .|.aggregation(Seq.empty, Seq("count(*) AS c"))
        .|.projectEndpoints("(x)-[r:REL]->(y)", startInScope = false, endInScope = false)
        .|.argument("r")
        .expand("(m)<-[r:REL]-(n)")
        .nodeByLabelScan("m", "M")
        .build()
    )
  }

  test("should plan uncorrelated subquery with updates with Apply (which gets unnested)") {
    val query =
      """MATCH (x)
        |CALL {
        |  CREATE (y:Label)
        |  RETURN *
        |}
        |RETURN count(*) AS count
        |""".stripMargin

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .apply()
        .|.create(createNode("y", "Label"))
        .|.argument()
        .allNodeScan("x")
        .build()
    )
  }

  test("should plan nested uncorrelated subqueries with updates with Apply") {
    val query =
      """MATCH (x)
        |CALL {
        |  CALL {
        |    CREATE (y:Label)
        |    RETURN *
        |  }
        |  RETURN *
        |}
        |RETURN count(*) AS count
        |""".stripMargin

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .apply()
        .|.create(createNode("y", "Label"))
        .|.argument()
        .allNodeScan("x")
        .build()
    )
  }

  test("should plan nested uncorrelated subqueries with updates in outer subquery with Apply / CartesianProduct") {
    val query =
      """MATCH (x)
        |CALL {
        |  CREATE (y:Label)
        |  WITH y
        |  CALL {
        |    RETURN 5 as literal
        |  }
        |  RETURN *
        |}
        |RETURN count(*) AS count
        |""".stripMargin

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("count")
        .aggregation(Seq.empty, Seq("count(*) AS count"))
        .apply()
        .|.cartesianProduct()
        .|.|.projection("5 AS literal")
        .|.|.argument()
        .|.create(createNode("y", "Label"))
        .|.argument()
        .allNodeScan("x")
        .build()
    )
  }

  test("Returning a variable that is no longer bound outside should work") {
    val query =
      """WITH 1 AS n
        |WITH n AS x
        |CALL {
        |  WITH x
        |  MATCH (n)
        |  WITH n
        |  RETURN *
        |}
        |RETURN n, x
        |""".stripMargin

    planFor(query) should equal(
      new LogicalPlanBuilder()
        .produceResults("n", "x")
        .apply()
        .|.allNodeScan("n", "x")
        .projection("n AS x")
        .projection("1 AS n")
        .argument()
        .build()
    )
  }

  test("call unit subquery in transactions") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |} IN TRANSACTIONS
        |RETURN a
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionForeach()
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call unit subquery in transactions with specified batch size") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |} IN TRANSACTIONS OF 42 ROWS
        |RETURN a
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionForeach(42)
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call correlated unit subquery in transactions") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  WITH a
        |  CREATE (b {prop: a.prop + 1})
        |} IN TRANSACTIONS
        |RETURN a
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionForeach()
      .|.create(createNodeWithProperties("b", Seq.empty, "{prop: a.prop + 1}"))
      .|.argument("a")
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call returning subquery in transactions") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |  RETURN b
        |} IN TRANSACTIONS
        |RETURN a, b
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionApply()
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call returning subquery in transactions with specified batch size") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |  RETURN b
        |} IN TRANSACTIONS OF 400 ROWS
        |RETURN a, b
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionApply(400)
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call correlated returning subquery in transactions") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  WITH a
        |  CREATE (b {prop: a.prop + 1})
        |  RETURN b
        |} IN TRANSACTIONS
        |RETURN a, b
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionApply()
      .|.create(createNodeWithProperties("b", Seq.empty, "{prop: a.prop + 1}"))
      .|.argument("a")
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call returning subquery in transactions with specified batch size, on error behaviour and status report") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |  RETURN b
        |} IN TRANSACTIONS OF 400 ROWS
        |  ON ERROR CONTINUE
        |  REPORT STATUS AS s
        |RETURN a, b, s
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionApply(400, onErrorBehaviour = OnErrorContinue, maybeReportAs = Some("s"))
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call returning subquery in transactions with specified batch size and on error behavior") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |  RETURN b
        |} IN TRANSACTIONS OF 400 ROWS
        |  ON ERROR FAIL
        |RETURN a, b
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionApply(400, onErrorBehaviour = OnErrorFail)
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call returning subquery in transactions with specified error behavior and status report") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |  RETURN b
        |} IN TRANSACTIONS
        |  ON ERROR BREAK
        |  REPORT STATUS AS s
        |RETURN a, b, s
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionApply(onErrorBehaviour = OnErrorBreak, maybeReportAs = Some("s"))
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call returning subquery in transactions with on error behaviour") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |  RETURN b
        |} IN TRANSACTIONS
        |  ON ERROR BREAK
        |RETURN a, b
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionApply(onErrorBehaviour = OnErrorBreak)
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test(
    "call with non-returning subquery in transactions with specified batch size, on error behaviour and status report"
  ) {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |} IN TRANSACTIONS OF 400 ROWS
        |  ON ERROR BREAK
        |  REPORT STATUS AS s
        |RETURN a, s
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionForeach(400, onErrorBehaviour = OnErrorBreak, maybeReportAs = Some("s"))
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call with non-returning subquery in transactions with specified batch size and on error behaviour") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |} IN TRANSACTIONS OF 400 ROWS
        |  ON ERROR CONTINUE
        |RETURN a
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionForeach(400, onErrorBehaviour = OnErrorContinue)
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call with non-returning subquery in transactions with specified on error behaviour and status report") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |} IN TRANSACTIONS
        |  ON ERROR CONTINUE
        |  REPORT STATUS AS s
        |RETURN a, s
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionForeach(onErrorBehaviour = OnErrorContinue, maybeReportAs = Some("s"))
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call with non-returning subquery in transactions with on error behaviour") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |} IN TRANSACTIONS
        |  ON ERROR FAIL
        |RETURN a
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionForeach(onErrorBehaviour = OnErrorFail)
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call with non-returning subquery in transactions with on error continue") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (b)
        |} IN TRANSACTIONS ON ERROR CONTINUE
        |RETURN a
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionForeach(onErrorBehaviour = OnErrorContinue)
      .|.create(createNode("b"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call subquery in transactions with internal read-write conflict is eagerized") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  MATCH (b)
        |  CREATE (c)
        |} IN TRANSACTIONS
        |RETURN a
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .transactionForeach()
      .|.create(createNode("c"))
      .|.eager()
      .|.allNodeScan("b")
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call subquery in transactions with internal read-write, and external write-read conflict is eagerized") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  MATCH (b)
        |  CREATE (c)
        |} IN TRANSACTIONS
        |MATCH (d)
        |RETURN a
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .apply()
      .|.allNodeScan("d", "a")
      .eager()
      .transactionForeach()
      .|.create(createNode("c"))
      .|.eager()
      .|.allNodeScan("b")
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("call subquery in transactions with external property write-read conflict is eagerized") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  WITH a
        |  SET a.prop = 1
        |} IN TRANSACTIONS
        |RETURN a.prop
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("a.prop AS `a.prop`")
      .eager()
      .transactionForeach()
      .|.setNodeProperty("a", "prop", "1")
      .|.argument("a")
      .allNodeScan("a")
      .build()
  }

  test("consecutive call subquery in transactions with write after load csv is not eagerized") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |LOAD CSV FROM 'https://neo4j.com/test.csv' AS line
        |CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS
        |CALL {
        |  MATCH (n)
        |  SET n.prop = 1
        |} IN TRANSACTIONS
        |RETURN 1 AS x
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("1 AS x")
      .transactionForeach()
      .|.setNodeProperty("n", "prop", "1")
      .|.allNodeScan("n")
      .eager()
      .transactionForeach()
      .|.create(createNode("n"))
      .|.argument()
      .loadCSV("'https://neo4j.com/test.csv'", "line", NoHeaders)
      .argument()
      .build()
  }

  test("consecutive call subquery in transactions with write-read conflict is eagerized") {
    val cfg = plannerCfgBuilder.build()

    val query =
      """
        |MATCH (a)
        |CALL {
        |  CREATE (n)
        |} IN TRANSACTIONS
        |CALL {
        |  MATCH (n)
        |  SET n.prop = 1
        |} IN TRANSACTIONS
        |RETURN 1 AS x
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("1 AS x")
      .transactionForeach()
      .|.setNodeProperty("n", "prop", "1")
      .|.allNodeScan("n")
      .eager()
      .transactionForeach()
      .|.create(createNode("n"))
      .|.argument()
      .eager()
      .allNodeScan("a")
      .build()
  }

  test("Should not push down property reads past transactionForeach") {
    val cfg = plannerCfgBuilder
      .setRelationshipCardinality("()-[]->()", 10000)
      .build()

    val query =
      """
        |MATCH (a)
        |WITH *, 1 AS dummy
        |MATCH (a)-->(b)
        |CALL {
        |  MATCH (n)
        |  SET n.otherProp = 17
        |} IN TRANSACTIONS
        |MATCH (b)-->(c)
        |RETURN a.prop as otherProp
        |""".stripMargin

    val plan = cfg.plan(query).stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[a.prop] AS otherProp")
      .expandAll("(b)-[anon_1]->(c)")
      .cacheProperties("cacheNFromStore[a.prop]")
      .transactionForeach()
      .|.setNodeProperty("n", "otherProp", "17")
      .|.allNodeScan("n")
      .expandAll("(a)-[anon_0]->(b)")
      .projection("1 AS dummy")
      .allNodeScan("a")
      .build()
  }
}
