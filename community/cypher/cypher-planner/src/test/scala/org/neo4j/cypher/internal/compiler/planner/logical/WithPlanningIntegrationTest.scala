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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverWithGreedyConnectComponents
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.configurationThatForcesCompacting
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ConfigurableIDPSolverConfig
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class WithPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2
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
        .projection("1 AS b")
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

  test("should build plans that project endpoints of re-matched directed relationship arguments") {

    val plan = planner.plan("MATCH (a)-[r]->(b) WITH r LIMIT 1 MATCH (u)-[r]->(v) RETURN r")

    plan should equal(
      planner.planBuilder()
        .produceResults("r")
        .apply()
        .|.projectEndpoints("(u)-[r]->(v)", startInScope = false, endInScope = false)
        .|.argument("r")
        .limit(1)
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("should build plans that project endpoints of re-matched reversed directed relationship arguments") {

    val plan = planner.plan("MATCH (a)-[r]->(b) WITH r AS r, a AS a LIMIT 1 MATCH (b2)<-[r]-(a) RETURN r")

    plan should equal(
      planner.planBuilder()
        .produceResults("r")
        .apply()
        .|.projectEndpoints("(a)-[r]->(b2)", startInScope = true, endInScope = false)
        .|.argument("a", "r")
        .limit(1)
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("should build plans that verify endpoints of re-matched directed relationship arguments") {

    val plan = planner.plan("MATCH (a)-[r]->(b) WITH * LIMIT 1 MATCH (a)-[r]->(b) RETURN r")

    plan should equal(
      planner.planBuilder()
        .produceResults("r")
        .apply()
        .|.projectEndpoints("(a)-[r]->(b)", startInScope = true, endInScope = true)
        .|.argument("a", "b", "r")
        .limit(1)
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("should build plans that project and verify endpoints of re-matched directed relationship arguments") {

    val plan = planner.plan("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]->(b2) RETURN r")

    plan should equal(
      planner.planBuilder()
        .produceResults("r")
        .apply()
        .|.projectEndpoints("(a)-[r]->(b2)", startInScope = true, endInScope = false)
        .|.argument("a", "r")
        .limit(1)
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("should build plans that project and verify endpoints of re-matched undirected relationship arguments") {

    val plan = planner.plan("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]-(b2) RETURN r")

    plan should equal(
      planner.planBuilder()
        .produceResults("r")
        .apply()
        .|.projectEndpoints("(a)-[r]-(b2)", startInScope = true, endInScope = false)
        .|.argument("a", "r")
        .limit(1)
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test(
    "should build plans that project and verify endpoints of re-matched directed var length relationship arguments"
  ) {

    val plan = planner.plan("MATCH (a)-[r*]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r*]->(b2) RETURN r")

    plan should equal(
      planner.planBuilder()
        .produceResults("r")
        .apply()
        .|.projectEndpoints("(a)-[r*1..]->(b2)", startInScope = true, endInScope = false)
        .|.argument("a", "r")
        .limit(1)
        .expand("(a)-[r*1..]->(b)")
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
        .|.expandInto("(n1)<-[anon_3]-(n2)")
        .|.argument("n1", "n2")
        .limit(10)
        .allRelationshipsScan("(n1)-[anon_2]->(n2)")
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
        .|.expandInto("(n1)<-[anon_3]-(n2)")
        .|.argument("n1", "n2")
        .limit(10)
        .distinct("n1 AS n1", "n2 AS n2")
        .allRelationshipsScan("(n1)-[anon_2]->(n2)")
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
        .antiSemiApply()
        .|.expandAll("(n2)-[anon_4]->(anon_3)")
        .|.argument("n2")
        .limit(10)
        .aggregation(Seq("n2 AS n2"), Seq("count(n1) AS n"))
        .allRelationshipsScan("(n1)-[anon_2]->(n2)")
        .build()
    )
  }

  test("Complex star pattern with WITH in front should not trip up joinSolver") {
    // created after https://github.com/neo4j/neo4j/issues/12212

    val maxIterationTime = 1 // the goal here is to force compaction as fast as possible
    val queryGraphSolver =
      QueryGraphSolverWithGreedyConnectComponents.queryGraphSolver(new ConfigurableIDPSolverConfig(1, maxIterationTime))

    planFor(
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
      """.stripMargin,
      configurationThatForcesCompacting,
      queryGraphSolver
    )
    // if we fail planning for this query the test fails
  }
}
