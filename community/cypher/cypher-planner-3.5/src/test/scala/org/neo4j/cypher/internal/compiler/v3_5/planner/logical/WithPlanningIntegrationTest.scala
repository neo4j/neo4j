/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp.ConfigurableIDPSolverConfig
import org.neo4j.cypher.internal.ir.v3_5.{SimplePatternLength, VarPatternLength}
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.{CypherFunSuite, WindowsStringSafe}

class WithPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  implicit val windowsSafe = WindowsStringSafe

  test("should build plans for simple WITH that adds a constant to the rows") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 RETURN 1 as `b`")._2
    val expected =
      Projection(
        Limit(
          AllNodesScan("a", Set.empty),
          SignedDecimalIntegerLiteral("1")(pos),
          DoNotIncludeTies
        ),
        Map[String, Expression]("b" -> SignedDecimalIntegerLiteral("1") _)
      )

    result should equal(expected)
  }

  test("should build plans that contain multiple WITH") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WITH a, b, r1 LIMIT 1 RETURN b as `b`")._2
    val expected = Limit(
      Expand(
        Limit(
          AllNodesScan("a", Set()),
          SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
        ),
        "a", OUTGOING, List(), "b", "r1", ExpandAll
      ),
      SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
    )

    result should equal(expected)
  }

  test("should build plans with WITH and selections") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WHERE r1.prop = 42 RETURN r1")._2
    val expected = Selection(
      Seq(In(Property(Variable("r1")(pos), PropertyKeyName("prop")(pos))(pos), ListLiteral(List(SignedDecimalIntegerLiteral("42")(pos)))(pos))(pos)),
      Expand(
        Limit(
          AllNodesScan("a", Set()),
          SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
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
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
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
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
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
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
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
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
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
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
      ),
      ProjectEndpoints(
        Argument(Set("a", "r")),
        "r", "a", startInScope = true, "b2", endInScope = false, None, directed = true, SimplePatternLength
      )
    )

    result should equal(expected)
  }

  test("should build plans that project and verify endpoints of re-matched undirected relationship arguments") {
    val result = planFor("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]-(b2) RETURN r")._2
    val expected = Apply(
      Limit(
        Expand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, List(), "b", "r", ExpandAll
        ),
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
      ),
      ProjectEndpoints(
        Argument(Set("a", "r")),
        "r", "a", startInScope = true, "b2", endInScope = false, None, directed = false, SimplePatternLength
      )
    )

    result should equal(expected)
  }

  test("should build plans that project and verify endpoints of re-matched directed var length relationship arguments") {
    val result = planFor("MATCH (a)-[r*]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r*]->(b2) RETURN r")._2
    val expected = Apply(
      Limit(
        VarExpand(
          AllNodesScan("a", Set()),
          "a", OUTGOING, OUTGOING, List(), "b", "r", VarPatternLength(1, None), ExpandAll, "r_NODES", "r_RELS", True()(pos), True()(pos), Seq()
        ),
        SignedDecimalIntegerLiteral("1")(pos), DoNotIncludeTies
      ),
      ProjectEndpoints(
        Argument(Set("a", "r")),
        "r", "a", startInScope = true, "b2", endInScope = false, None, directed = true, VarPatternLength(1, None)
      )
    )

    result should equal(expected)
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
        Apply(
        Projection(_, _),
        AllNodesScan("n1", _)
        ), _, _)
        ) => ()
    }
  }

  test("WHERE clause on WITH DISTINCT uses argument from previous WITH"){
    val result = planFor( """
                      WITH 0.1 AS p
                      MATCH (n1)
                      WITH DISTINCT n1, p LIMIT 10 WHERE rand() < p
                      RETURN n1""")._2

    result should beLike {
      case
        SelectionMatcher(Seq(LessThan(FunctionInvocation(Namespace(List()),FunctionName("rand"),false,Vector()),Variable("p"))),
        Limit(
        Distinct(
        Apply(
        Projection(_, _),
        AllNodesScan("n1", _)
        ),_), _, _)
        ) => ()
    }
  }

  test("WHERE clause on WITH AGGREGATION uses argument from previous WITH"){
    val result = planFor( """
                      WITH 0.1 AS p
                      MATCH (n1)
                      WITH count(n1) AS n, p WHERE rand() < p
                      RETURN n""")._2

    result should beLike {
      case
        SelectionMatcher(Seq(LessThan(FunctionInvocation(Namespace(List()),FunctionName("rand"),false,Vector()),Variable("p"))),
        Aggregation(
        Apply(
        Projection(_, _),
        AllNodesScan("n1", _)
        ), _, _)
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
        Limit(_,_,_)
        ) if hasPathExpression(ands) => ()
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
        Limit(_,_,_)
        ) if hasPathExpression(ands)=> ()
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
        Limit(_,_,_)
        ) if hasGetDegree(ands) => ()
    }
  }

  test("Complex star pattern with WITH in front should not trip up joinSolver"){
    // created after https://github.com/neo4j/neo4j/issues/12212

    val maxIterationTime = 1 // the goal here is to force compaction as fast as possible
    val configurationThatForcesCompacting = cypherCompilerConfig.copy(idpIterationDuration = maxIterationTime)
    val queryGraphSolver = createQueryGraphSolver(new ConfigurableIDPSolverConfig(1, maxIterationTime))

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

  private def hasPathExpression(ands: Ands): Boolean = {
    ands.treeExists {
      case _: PathExpression => true
    }
  }

  private def hasGetDegree(ands: Ands): Boolean = {
    ands.treeExists {
      case _: GetDegree => true
    }
  }
}
