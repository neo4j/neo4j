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
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.QueryGraphSolverWithGreedyConnectComponents
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2.configurationThatForcesCompacting
import org.neo4j.cypher.internal.compiler.planner.logical.idp.ConfigurableIDPSolverConfig
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.HasDegreeGreaterThan
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SelectionMatcher
import org.neo4j.cypher.internal.logical.plans.VarExpand
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
    val result = planFor("MATCH (a) WITH a LIMIT 1 RETURN 1 as `b`")._2
    val expected =
      Projection(
        Limit(
          AllNodesScan("a", Set.empty),
          literalInt(1)
        ),
        Map[String, Expression]("b" -> literalInt(1))
      )

    result should equal(expected)
  }

  test("should build plans that contain multiple WITH") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WITH a, b, r1 LIMIT 1 RETURN b as `b`")._2
    val expected = Limit(
      Expand(
        Limit(
          AllNodesScan("a", Set()),
          literalInt(1)
        ),
        "a", OUTGOING, List(), "b", "r1", ExpandAll
      ),
      literalInt(1)
    )

    result should equal(expected)
  }

  test("should build plans with WITH and selections") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WHERE r1.prop = 42 RETURN r1")._2
    val expected = Selection(
      Seq(equals(prop("r1", "prop"), literalInt(42))),
      Expand(
        Limit(
          AllNodesScan("a", Set()),
          literalInt(1)
        ),
        "a", OUTGOING, List(), "b", "r1", ExpandAll
      )
    )

    result should equal(expected)
  }

  test("should build plans for two matches separated by WITH") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN b")._2
    val expected = Expand(
      Limit(
        AllNodesScan("a", Set()),
        literalInt(1)
      ),
      "a", OUTGOING, List(), "b", "r", ExpandAll
    )

    result should equal(expected)
  }

  test("should build plans that project endpoints of re-matched directed relationship arguments") {
    val result = planFor("MATCH (a)-[r]->(b) WITH r LIMIT 1 MATCH (u)-[r]->(v) RETURN r")._2
    val expected = Apply(
      Limit(
        Expand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, List(), "b", "r", ExpandAll
        ),
        literalInt(1)
      ),
      ProjectEndpoints(
        Argument(Set("r")),
        "r", "u", startInScope = false, "v", endInScope = false, None, directed = true, SimplePatternLength
      )
    )

    result should equal(expected)
  }

  test("should build plans that project endpoints of re-matched reversed directed relationship arguments") {
    val result = planFor("MATCH (a)-[r]->(b) WITH r AS r, a AS a LIMIT 1 MATCH (b2)<-[r]-(a) RETURN r")._2
    val expected = Apply(
      Limit(
        Expand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, List(), "b", "r", ExpandAll
        ),
        literalInt(1)
      ),
      ProjectEndpoints(
        Argument(Set("a", "r")),
        "r", "a", startInScope = true, "b2", endInScope = false, None, directed = true, SimplePatternLength
      )
    )

    result should equal(expected)
  }

  test("should build plans that verify endpoints of re-matched directed relationship arguments") {
    val result = planFor("MATCH (a)-[r]->(b) WITH * LIMIT 1 MATCH (a)-[r]->(b) RETURN r")._2
    val expected = Apply(
      Limit(
        Expand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, List(), "b", "r", ExpandAll
        ),
        literalInt(1)
      ),
      ProjectEndpoints(
        Argument(Set("a", "b", "r")),
        "r", "a", startInScope = true, "b", endInScope = true, None, directed = true, SimplePatternLength
      )
    )

    result should equal(expected)
  }

  test("should build plans that project and verify endpoints of re-matched directed relationship arguments") {
    val result = planFor("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]->(b2) RETURN r")._2
    val expected = Apply(
      Limit(
        Expand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, List(), "b", "r", ExpandAll
        ),
        literalInt(1)
      ),
      ProjectEndpoints(
        Argument(Set("a", "r")),
        "r", "a", startInScope = true, "b2", endInScope = false, None, directed = true, SimplePatternLength
      )
    )

    result should equal(expected)
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
        .expandAll("(a)-[r]->(b)")
        .allNodeScan("a")
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
        .|.filter("size(r) >= 1")
        .|.argument("a", "r")
        .limit(1)
        .expand("(a)-[r*1..]->(b)")
        .allNodeScan("a")
        .build()
    )
  }

  test("WHERE clause on WITH uses argument from previous WITH"){
    val result = planFor( """
                      WITH 0.1 AS p
                      MATCH (n1)
                      WITH n1 LIMIT 10 WHERE rand() < p
                      RETURN n1""")._2

    result should beLike {
      case
        SelectionMatcher(Seq(LessThan(FunctionInvocation(Namespace(List()),FunctionName("rand"),false,Vector()),Variable("p"))),
        Limit(
        Projection(
        AllNodesScan("n1", _), _),
         _)
        ) => ()
    }
  }

  test("WHERE clause on WITH DISTINCT uses argument from previous WITH"){
    val plan = planFor( normalizeNewLines("""
                      WITH 0.1 AS p
                      MATCH (n1)
                      WITH DISTINCT n1, p LIMIT 10 WHERE rand() < p
                      RETURN n1"""))._2
    plan should beLike {
      case
        SelectionMatcher(Seq(
        LessThan(FunctionInvocation(Namespace(List()), FunctionName("rand"), false, Vector()),
        Variable("p"))),
        Limit(
        Distinct(
        Projection(
        AllNodesScan("n1", _), _),
         _), _)
        ) => ()
    }
  }

  test("WHERE clause on WITH AGGREGATION uses argument from previous WITH"){
    val plan = planFor( normalizeNewLines("""
                      WITH 0.1 AS p
                      MATCH (n1)
                      WITH count(n1) AS n, p WHERE rand() < p
                      RETURN n"""))._2
    plan should beLike {
      case
        SelectionMatcher(Seq(LessThan(FunctionInvocation(Namespace(List()),FunctionName("rand"),false,Vector()),Variable("p"))),
        Aggregation(
        Projection(
        AllNodesScan("n1", _), _),
        _, _)
        ) => ()
    }
  }

  test("WHERE clause on WITH with PatternExpression"){
    val result = planFor( """
                      MATCH (n1)-->(n2)
                      WITH n1 LIMIT 10 WHERE NOT (n1)<--(n2)
                      RETURN n1""")._2

    result should beLike {
      case
        Selection(ands,
        Limit(_,_)
        ) if hasNestedPlanExpression(ands) => ()
    }
  }

  test("WHERE clause on WITH DISTINCT with PatternExpression"){
    val result = planFor( """
                      MATCH (n1)-->(n2)
                      WITH DISTINCT n1, n2 LIMIT 10 WHERE NOT (n1)<--(n2)
                      RETURN n1""")._2

    result should beLike {
      case
        Selection(ands,
        Limit(_,_)
        ) if hasNestedPlanExpression(ands)=> ()
    }
  }

  test("WHERE clause on WITH AGGREGATION with PatternExpression"){
    val result = planFor( """
                      MATCH (n1)-->(n2)
                      WITH count(n1) AS n, n2 LIMIT 10 WHERE NOT ()<--(n2)
                      RETURN n""")._2

    result should beLike {
      case
        Selection(ands,
        Limit(_,_)
        ) if containsHasDegreeGreaterThan(ands) => ()
    }
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

  test("Complex star pattern with WITH in front should not trip up joinSolver"){
    // created after https://github.com/neo4j/neo4j/issues/12212

    val maxIterationTime = 1 // the goal here is to force compaction as fast as possible
    val queryGraphSolver = QueryGraphSolverWithGreedyConnectComponents.queryGraphSolver(new ConfigurableIDPSolverConfig(1, maxIterationTime))

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
      """.stripMargin, configurationThatForcesCompacting, queryGraphSolver)
    // if we fail planning for this query the test fails
  }

  private def hasNestedPlanExpression(ands: Ands): Boolean = {
    ands.folder.treeExists {
      case _: NestedPlanExpression => true
    }
  }

  private def containsHasDegreeGreaterThan(ands: Ands): Boolean = {
    ands.folder.treeExists {
      case _: HasDegreeGreaterThan => true
    }
  }

  test("Cannot use IndexScan if not all aggregations use property") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 100)
      .addNodeIndex("N", Seq("prop"), 0.5, 0.1)
      .build()

    val plan = planner.plan(
      """MATCH (n:N)
        |RETURN count(n)    AS count,
        |       avg(n.prop) AS avg
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("count", "avg")
        .aggregation(Seq(), Seq("count(n) AS count", "avg(n.prop) AS avg"))
        .nodeByLabelScan("n", "N")
        .build()
    )
  }
}
