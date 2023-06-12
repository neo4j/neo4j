/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.HasHeaders
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class WithPlanningIntegrationTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  private val planner = plannerBuilder()
    .setAllNodesCardinality(100)
    .setAllRelationshipsCardinality(100)
    .setRelationshipCardinality("()-[]->()", 10)
    .build()

  test("should build plans for simple WITH that adds a constant to the rows") {

    val plan = planner.plan("MATCH (a) WITH a LIMIT 1 RETURN 1 as `b`")

    plan should equal(
      planner.planBuilder()
        .produceResults("b")
        .projection(project = Seq("1 AS b"), discard = Set("a"))
        .limit(1)
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans that contain multiple WITH") {

    val plan = planner.plan("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WITH a, b, r1 LIMIT 1 RETURN b as `b`")

    plan should equal(
      planner.planBuilder()
        .produceResults("b")
        .limit(1)
        .expandAll("(a)-[r1]->(b)")
        .limit(1)
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans with WITH and selections") {
    val plan = planner.plan("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WHERE r1.prop = 42 RETURN r1")

    plan should equal(
      planner.planBuilder()
        .produceResults("r1")
        .filter("r1.prop = 42")
        .expandAll("(a)-[r1]->(b)")
        .limit(1)
        .allNodeScan("a")
        .build()
    )
  }

  test("should build plans for two matches separated by WITH") {

    val plan = planner.plan("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN b")

    plan should equal(
      planner.planBuilder()
        .produceResults("b")
        .expandAll("(a)-[r]->(b)")
        .limit(1)
        .allNodeScan("a")
        .build()
    )
  }

  test("WHERE clause on WITH uses argument from previous WITH") {

    val plan = planner.plan(
      """WITH 0.1 AS p
        |MATCH (n1)
        |WITH n1 LIMIT 10 WHERE rand() < p
        |RETURN n1""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n1")
        .filter("rand() < p")
        .limit(10)
        .projection("0.1 AS p")
        .allNodeScan("n1")
        .build()
    )
  }

  test("WHERE clause on WITH DISTINCT uses argument from previous WITH") {

    val plan = planner.plan(
      """WITH 0.1 AS p
        |MATCH (n1)
        |WITH DISTINCT n1, p LIMIT 10 WHERE rand() < p
        |RETURN n1
      """.stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n1")
        .filter("rand() < p")
        .limit(10)
        .distinct("n1 AS n1", "p AS p")
        .projection("0.1 AS p")
        .allNodeScan("n1")
        .build()
    )
  }

  test("WHERE clause on WITH AGGREGATION uses argument from previous WITH") {

    val plan = planner.plan(
      """WITH 0.1 AS p
        |MATCH (n1)
        |WITH count(n1) AS n, p WHERE rand() < p
        |RETURN n""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        .filter("rand() < p")
        .aggregation(Seq("p AS p"), Seq("count(n1) AS n"))
        .projection("0.1 AS p")
        .allNodeScan("n1")
        .build()
    )
  }

  test("WHERE clause on WITH with PatternExpression") {

    val plan = planner.plan(
      """MATCH (n1)-->(n2)
        |WITH n1 LIMIT 10 WHERE NOT (n1)<--(n2)
        |RETURN n1""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n1")
        .antiSemiApply()
        .|.expandInto("(n1)<-[anon_1]-(n2)")
        .|.argument("n1", "n2")
        .limit(10)
        .allRelationshipsScan("(n1)-[anon_0]->(n2)")
        .build()
    )
  }

  test("WHERE clause on WITH DISTINCT with PatternExpression") {

    val plan = planner.plan(
      """MATCH (n1)-->(n2)
        |WITH DISTINCT n1, n2 LIMIT 10 WHERE NOT (n1)<--(n2)
        |RETURN n1""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n1")
        .antiSemiApply()
        .|.expandInto("(n1)<-[anon_1]-(n2)")
        .|.argument("n1", "n2")
        .limit(10)
        .distinct("n1 AS n1", "n2 AS n2")
        .allRelationshipsScan("(n1)-[anon_0]->(n2)")
        .build()
    )
  }

  test("WHERE clause on WITH AGGREGATION with PatternExpression") {

    val plan = planner.plan(
      """MATCH (n1)-->(n2)
        |WITH count(n1) AS n, n2 LIMIT 10 WHERE NOT ()<--(n2)
        |RETURN n""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        .filterExpression(
          not(HasDegreeGreaterThan(
            varFor("n2"),
            None,
            SemanticDirection.OUTGOING,
            literalInt(0)
          )(pos))
        )
        .limit(10)
        .aggregation(Seq("n2 AS n2"), Seq("count(n1) AS n"))
        .allRelationshipsScan("(n1)-[anon_0]->(n2)")
        .build()
    )
  }

  test("SKIP and LIMIT with rand() should get planned correctly") {
    val plan = planner.plan(
      """MATCH (n)
        |WITH n SKIP toInteger(rand() * 10) LIMIT 1
        |RETURN n
        |  """.stripMargin
    ).stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .limit(1)
        .skip(function("toInteger", multiply(function("rand"), literalInt(10))))
        .allNodeScan("n")
        .build()
    )
  }

  test("Complex star pattern with WITH in front should not trip up joinSolver") {
    // created after https://github.com/neo4j/neo4j/issues/12212
    val planner = plannerBuilder()
      .setAllNodesCardinality(10)
      .setLabelCardinality("LabelA", 10)
      .setLabelCardinality("LabelB", 10)
      .setLabelCardinality("LabelC", 10)
      .setLabelCardinality("LabelD", 10)
      .setLabelCardinality("LabelK", 10)
      .setLabelCardinality("LabelSp", 10)
      .setLabelCardinality("LabelV", 10)
      .setLabelCardinality("LabelX", 10)
      .setRelationshipCardinality("()-[:LabelE]->()", 10)
      .setRelationshipCardinality("(:LabelC)-[:LabelE]->()", 10)
      .setRelationshipCardinality("(:LabelC)-[:LabelE]->(:LabelC)", 10)
      .setRelationshipCardinality("()-[:LabelE]->(:LabelC)", 10)
      .setRelationshipCardinality("()-[:relTypeA]->()", 10)
      .setRelationshipCardinality("(:LabelA)-[:relTypeA]->()", 10)
      .setRelationshipCardinality("(:LabelA)-[:relTypeA]->(:LabelB)", 10)
      .setRelationshipCardinality("()-[:relTypeA]->(:LabelB)", 10)
      .setRelationshipCardinality("(:LabelA)-[:relTypeA]->(:LabelSp)", 10)
      .setRelationshipCardinality("()-[:relTypeA]->(:LabelSp)", 10)
      .setRelationshipCardinality("()-[:relTypeB]->()", 10)
      .setRelationshipCardinality("(:LabelSp)-[:relTypeB]->(:LabelC)", 10)
      .setRelationshipCardinality("(:LabelSp)-[:relTypeB]->()", 10)
      .setRelationshipCardinality("()-[:relTypeB]->(:LabelC)", 10)
      .setRelationshipCardinality("(:LabelB)-[:relTypeB]->(:LabelC)", 10)
      .setRelationshipCardinality("(:LabelB)-[:relTypeB]->()", 10)
      .setRelationshipCardinality("()-[:relTypeLink]->()", 10)
      .setRelationshipCardinality("(:LabelD)-[:relTypeLink]->()", 10)
      .setRelationshipCardinality("(:LabelD)-[:relTypeLink]->(:LabelA)", 10)
      .setRelationshipCardinality("()-[:relTypeLink]->(:LabelA)", 10)
      .setRelationshipCardinality("()-[:relTypeZ]->()", 10)
      // the goal here is to force compaction as fast as possible
      .withSetting(GraphDatabaseInternalSettings.cypher_idp_solver_table_threshold, Int.box(16))
      .withSetting(GraphDatabaseInternalSettings.cypher_idp_solver_duration_threshold, Long.box(10))
      .build()

    planner.plan(
      """WITH 20000 AS param1, 5000 AS param2
        |MATCH (gknA:LabelA {propertyA: 4})-[r1:relTypeA]->(:LabelB)-[r2:relTypeB]->(commonLe:LabelC) <-[r3:relTypeB]-(:LabelB)<-[r4:relTypeA]-(gknB:LabelA {propertyA: 4})
        |WHERE NOT exists( (gknA)<-[:relTypeLink]-(:LabelD) ) AND NOT exists( (gknB)<-[:relTypeLink]-(:LabelD) ) AND substring(gknA.propertyB, 0, size(gknA.propertyB) - 1) = substring(gknB.propertyB, 0, size(gknB.propertyB) - 1) AND gknA.propertyC < gknB.propertyC
        |MATCH (gknA)-[r7:relTypeA]->(n3:LabelB)-[assocA:relTypeB]->(thing4:LabelC)-[r8:LabelE {propertyC: 'None'}]-(commonLe)
        |MATCH (gknB)-[r9:relTypeA]->(n5:LabelB)-[assocB:relTypeB]->(thing6:LabelC)-[r10:LabelE {propertyC: 'None'}]-(commonLe)
        |MATCH (gknA)-[r11:relTypeA]->(n6:LabelSp)-[assocLabelSpA:relTypeB]->(LabelSpA:LabelC)
        |MATCH (gknB)-[r12:relTypeA]->(n7:LabelSp)-[assocLabelSpB:relTypeB]->(LabelSpB:LabelC)
        |WITH gknA, gknB, param1, param2, [{ LabelC: thing4, thing3: assocA.propertyD, thing5: CASE WHEN assocA.propertyD = 0.0 THEN 'normal' ELSE 'reverse' END, thing: CASE WHEN assocA.propertyD = 0.0 THEN 'reverse' ELSE 'normal' END, thing2: param1 }, { LabelC: thing6, thing3: assocB.propertyD, thing5: CASE WHEN assocB.propertyD = 0.0 THEN 'normal' ELSE 'reverse' END, thing: CASE WHEN assocB.propertyD = 0.0 THEN 'reverse' ELSE 'normal' END, thing2: param1 }, { LabelC: LabelSpA, thing3: assocLabelSpA.propertyD, thing5: CASE WHEN assocLabelSpA.propertyD = 0.0 THEN 'normal' ELSE 'reverse' END, thing: CASE WHEN assocLabelSpA.propertyD = 0.0 THEN 'reverse' ELSE 'normal' END, thing2: param2 }, { LabelC: LabelSpB, thing3: assocLabelSpB.propertyD, thing5: CASE WHEN assocLabelSpB.propertyD = 0.0 THEN 'normal' ELSE 'reverse' END, thing: CASE WHEN assocLabelSpB.propertyD = 0.0 THEN 'reverse' ELSE 'normal' END, thing2: param2 }] AS bounds
        |CREATE (gknA)<-[:relTypeLink]-(newLabelD:LabelD:LabelX)-[:relTypeLink]->(gknB) SET newLabelD = { uuidKey: gknA.id + '-' + gknB.id, propertyB: substring(gknA.propertyB, 0, size(gknA.propertyB) - 1), propY: 'SOMETHING', typ: 'EKW' }
        |CREATE (newLabelD)-[:relTypeA]->(areaLocation:LabelV:LabelK)
        |WITH bounds, areaLocation UNWIND bounds AS bound
        |WITH areaLocation, bound, bound.LabelC AS LabelC, bound.thing AS thing
        |WITH areaLocation, bound, LabelC
        |WITH areaLocation, bound, LabelC
        |CREATE (areaLocation)-[boundRel:relTypeZ]->(LabelC)
        |SET boundRel.thing = bound.thing, boundRel.propertyD = 2
      """.stripMargin
    )
    // if we fail planning for this query the test fails
  }

  test("should discard unused variable") {

    val plan = planner.plan(
      """MATCH (n1)
        |WITH collect(n1.value) AS bigList
        |WITH size(bigList) AS listSize
        |MATCH (foo) WHERE foo.size = listSize
        |RETURN foo""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("foo")
        .filter("foo.size = listSize")
        .apply()
        .|.allNodeScan("foo", "listSize")
        .projection(project = Seq("size(bigList) AS listSize"), discard = Set("bigList"))
        .aggregation(Seq.empty, Seq("collect(n1.value) AS bigList"))
        .allNodeScan("n1")
        .build()
    )
  }

  test("should keep used variable") {

    val plan = planner.plan(
      """MATCH (n1)
        |WITH n1, n1.foo AS foo
        |RETURN n1, foo""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n1", "foo")
        .projection("n1.foo AS foo")
        .allNodeScan("n1")
        .build()
    )
  }

  test("should discard variable that is projected for ORDER BY") {

    val plan = planner.plan(
      """MATCH (n1)
        |WITH n1.foo AS foo ORDER BY n1.bar
        |RETURN foo""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("foo")
        .projection(project = Seq("n1.foo AS foo"), discard = Set("n1", "n1.bar"))
        .sort(Seq(Ascending("n1.bar")))
        .projection("n1.bar AS `n1.bar`")
        .allNodeScan("n1")
        .build()
    )
  }

  test("should keep variable that is projected for ORDER BY and kept in WITH") {

    val plan = planner.plan(
      """MATCH (n1)
        |WITH n1.bar AS bar, n1.foo AS foo ORDER BY bar
        |RETURN bar, foo""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("bar", "foo")
        .projection(project = Seq("n1.foo AS foo"), discard = Set("n1"))
        .sort(Seq(Ascending("bar")))
        .projection("n1.bar AS bar")
        .allNodeScan("n1")
        .build()
    )
  }

  test("should keep more variables that are projected for ORDER BY and kept in WITH") {

    val plan = planner.plan(
      """MATCH (n1)
        |WITH n1, n1.bar AS bar, n1.foo AS foo ORDER BY bar
        |RETURN n1, bar, foo""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n1", "bar", "foo")
        .projection("n1.foo AS foo")
        .sort(Seq(Ascending("bar")))
        .projection("n1.bar AS bar")
        .allNodeScan("n1")
        .build()
    )
  }

  test("should not plan projection directly before RETURN") {

    val plan = planner.plan(
      """MATCH (n1), (n2)
        |RETURN n1""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n1")
        .cartesianProduct()
        .|.allNodeScan("n2")
        .allNodeScan("n1")
        .build()
    )
  }

  test("should keep everything and project one extra variable") {
    val plan = planner.plan(
      """MATCH (n1), (n2)
        |WITH n1, n2, n1.prop AS prop
        |RETURN *""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n1", "n2", "prop")
        .projection("cacheN[n1.prop] AS prop")
        .cartesianProduct()
        .|.allNodeScan("n2")
        .cacheProperties("cacheNFromStore[n1.prop]")
        .allNodeScan("n1")
        .build()
    )
  }

  test("should not discard outer variables in sub query") {
    val plan = planner.plan(
      """MATCH (n)
        |WITH n AS n1, n.prop AS prop
        |CALL {
        |  WITH n1
        |  MATCH (n1)-->(n2)
        |  WITH n2.prop AS prop2
        |  RETURN prop2
        |}
        |RETURN n1, prop, prop2""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n1", "prop", "prop2")
        .projection("n2.prop AS prop2")
        .expandAll("(n1)-[anon_0]->(n2)")
        .projection(project = Seq("n AS n1", "n.prop AS prop"), discard = Set("n"))
        .allNodeScan("n")
        .build()
    )
  }

  test("should not discard variables in sub query 2") {
    val query =
      """WITH 1 AS x, 2 AS y 
        |CALL { 
        |  WITH x
        |  MATCH (y) 
        |  WHERE y.value > x
        |  WITH y AS y2
        |  RETURN sum(y2.prop) AS sum 
        |} RETURN sum""".stripMargin

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("sum")
        .apply()
        .|.aggregation(Seq(), Seq("sum(y2.prop) AS sum"))
        .|.projection("y AS y2")
        .|.filter("y.value > x")
        .|.allNodeScan("y", "x")
        .projection("1 AS x", "2 AS y")
        .argument()
        .build()
    )
  }

  test("should not discard outer variables in write sub query") {
    val plan = planner.plan(
      """MATCH (n)
        |WITH n AS n1, n.prop AS prop
        |CALL {
        |  WITH n1
        |  CREATE (n1)-[:LIKES]->(n2 {prop: n1.x})
        |  WITH n2.prop AS prop2
        |  RETURN prop2
        |}
        |RETURN n1, prop, prop2""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n1", "prop", "prop2")
        .projection("n2.prop AS prop2")
        .apply()
        .|.create(
          createNodeWithProperties("n2", Seq(), "{prop: n1.x}"),
          createRelationship("anon_0", "n1", "LIKES", "n2", OUTGOING)
        )
        .|.argument("n1")
        .projection(project = Seq("n AS n1", "n.prop AS prop"), discard = Set("n"))
        .allNodeScan("n")
        .build()
    )
  }

  test("should not discard outer variables in nested sub query") {
    val plan = planner.plan(
      """WITH 1 AS a, 2 AS b, 3 AS c
        |CALL {
        |  WITH a
        |  WITH 2*a AS aa, 4 AS d, 5 AS e
        |  CALL {
        |    WITH d
        |    WITH 2*d AS dd, 6 AS f, 7 AS g
        |    RETURN dd, f
        |  }
        |  RETURN dd, f, aa, d
        |}
        |WITH a, b, dd, f, aa, d, 2*c AS cc 
        |RETURN a, b, dd, f, aa, d, cc""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("a", "b", "dd", "f", "aa", "d", "cc")
        .projection(project = Seq("c * 2 AS cc"), discard = Set("e", "g", "c"))
        .projection("d * 2 AS dd", "6 AS f", "7 AS g")
        .projection("a * 2 AS aa", "4 AS d", "5 AS e")
        .projection("1 AS a", "2 AS b", "3 AS c")
        .argument()
        .build()
    )
  }

  test("should discard in load csv with call in transactions") {
    val plan = planner.plan(
      """LOAD CSV WITH HEADERS from $param AS row
        |CALL {
        |  WITH row
        |  CREATE (n {name: row.name, age: toInteger(row.age)})
        |  RETURN n
        |} IN TRANSACTIONS
        |RETURN n.name, n.age ORDER BY n.age ASC""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("`n.name`", "`n.age`")
        .projection(project = Seq("n.name AS `n.name`"), discard = Set("row", "n"))
        .sort(Seq(Ascending("n.age")))
        .projection("n.age AS `n.age`")
        .transactionApply(1000, OnErrorFail)
        .|.create(createNodeWithProperties("n", Seq(), "{name: row.name, age: toInteger(row.age)}"))
        .|.argument("row")
        .loadCSV("$param", "row", HasHeaders, None)
        .argument()
        .build()
    )
  }

}
