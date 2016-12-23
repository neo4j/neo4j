/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans.{AllNodesScan, DoNotIncludeTies, Limit, Projection}
import org.neo4j.cypher.internal.compiler.v3_2.test_helpers.WindowsStringSafe
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Expression, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

class WithPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  implicit val windowsSafe = WindowsStringSafe

  test("should build plans for simple WITH that adds a constant to the rows") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 RETURN 1 as `b`")._2
    val expected =
      Projection(
        Limit(
          AllNodesScan("a", Set.empty)(solved),
          SignedDecimalIntegerLiteral("1")(pos),
          DoNotIncludeTies
        )(solved),
        Map[String, Expression]("b" -> SignedDecimalIntegerLiteral("1") _)
      )(solved)

    result should equal(expected)
  }

  test("should build plans that contain multiple WITH") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WITH a, b, r1 LIMIT 1 RETURN b as `b`")._2

    result.toString should equal(
      """Limit(SignedDecimalIntegerLiteral(1), DoNotIncludeTies) {
        |  LHS -> Expand(IdName(a), OUTGOING, List(), IdName(b), IdName(r1), ExpandAll) {
        |    LHS -> Limit(SignedDecimalIntegerLiteral(1), DoNotIncludeTies) {
        |      LHS -> AllNodesScan(IdName(a), Set()) {}
        |    }
        |  }
        |}""".stripMargin)
  }

  test("should build plans with WITH and selections") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WHERE r1.prop = 42 RETURN r1")._2

    result.toString should equal(
      """Selection(MutableList(In(Property(Variable(r1),PropertyKeyName(prop)),ListLiteral(List(SignedDecimalIntegerLiteral(42)))))) {
        |  LHS -> Expand(IdName(a), OUTGOING, List(), IdName(b), IdName(r1), ExpandAll) {
        |    LHS -> Limit(SignedDecimalIntegerLiteral(1), DoNotIncludeTies) {
        |      LHS -> AllNodesScan(IdName(a), Set()) {}
        |    }
        |  }
        |}""".stripMargin)
  }

  test("should build plans for two matches separated by WITH") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN b")._2

    result.toString should equal(
      """Expand(IdName(a), OUTGOING, List(), IdName(b), IdName(r), ExpandAll) {
        |  LHS -> Limit(SignedDecimalIntegerLiteral(1), DoNotIncludeTies) {
        |    LHS -> AllNodesScan(IdName(a), Set()) {}
        |  }
        |}""".stripMargin)
  }

  test("should build plans that project endpoints of re-matched directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH r LIMIT 1 MATCH (u)-[r]->(v) RETURN r")._2

    plan.toString should equal(
      """Apply() {
        |  LHS -> Limit(SignedDecimalIntegerLiteral(1), DoNotIncludeTies) {
        |    LHS -> Expand(IdName(b), INCOMING, List(), IdName(a), IdName(r), ExpandAll) {
        |      LHS -> AllNodesScan(IdName(b), Set()) {}
        |    }
        |  }
        |  RHS -> ProjectEndpoints(IdName(r), IdName(u), false, IdName(v), false, None, true, SimplePatternLength) {
        |    LHS -> Argument(Set(IdName(r))) {}
        |  }
        |}""".stripMargin)
  }

  test("should build plans that project endpoints of re-matched reversed directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH r AS r, a AS a LIMIT 1 MATCH (b2)<-[r]-(a) RETURN r")._2

    plan.toString should equal(
      """Apply() {
        |  LHS -> Limit(SignedDecimalIntegerLiteral(1), DoNotIncludeTies) {
        |    LHS -> Expand(IdName(b), INCOMING, List(), IdName(a), IdName(r), ExpandAll) {
        |      LHS -> AllNodesScan(IdName(b), Set()) {}
        |    }
        |  }
        |  RHS -> ProjectEndpoints(IdName(r), IdName(a), true, IdName(b2), false, None, true, SimplePatternLength) {
        |    LHS -> Argument(Set(IdName(a), IdName(r))) {}
        |  }
        |}""".stripMargin)
  }

  test("should build plans that verify endpoints of re-matched directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH * LIMIT 1 MATCH (a)-[r]->(b) RETURN r")._2

    plan.toString should equal(
      """Apply() {
        |  LHS -> Limit(SignedDecimalIntegerLiteral(1), DoNotIncludeTies) {
        |    LHS -> Expand(IdName(b), INCOMING, List(), IdName(a), IdName(r), ExpandAll) {
        |      LHS -> AllNodesScan(IdName(b), Set()) {}
        |    }
        |  }
        |  RHS -> ProjectEndpoints(IdName(r), IdName(a), true, IdName(b), true, None, true, SimplePatternLength) {
        |    LHS -> Argument(Set(IdName(a), IdName(b), IdName(r))) {}
        |  }
        |}""".stripMargin)
  }

  test("should build plans that project and verify endpoints of re-matched directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]->(b2) RETURN r")._2

    plan.toString should equal(
      """Apply() {
        |  LHS -> Limit(SignedDecimalIntegerLiteral(1), DoNotIncludeTies) {
        |    LHS -> Expand(IdName(b), INCOMING, List(), IdName(a), IdName(r), ExpandAll) {
        |      LHS -> AllNodesScan(IdName(b), Set()) {}
        |    }
        |  }
        |  RHS -> ProjectEndpoints(IdName(r), IdName(a), true, IdName(b2), false, None, true, SimplePatternLength) {
        |    LHS -> Argument(Set(IdName(a), IdName(r))) {}
        |  }
        |}""".stripMargin)
  }

  test("should build plans that project and verify endpoints of re-matched undirected relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]-(b2) RETURN r")._2

    plan.toString should equal(
      """Apply() {
        |  LHS -> Limit(SignedDecimalIntegerLiteral(1), DoNotIncludeTies) {
        |    LHS -> Expand(IdName(b), INCOMING, List(), IdName(a), IdName(r), ExpandAll) {
        |      LHS -> AllNodesScan(IdName(b), Set()) {}
        |    }
        |  }
        |  RHS -> ProjectEndpoints(IdName(r), IdName(a), true, IdName(b2), false, None, false, SimplePatternLength) {
        |    LHS -> Argument(Set(IdName(a), IdName(r))) {}
        |  }
        |}""".stripMargin)
  }

  test("should build plans that project and verify endpoints of re-matched directed var length relationship arguments") {
    val plan = planFor("MATCH (a)-[r*]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r*]->(b2) RETURN r")._2

    plan.toString should equal(
      """Apply() {
        |  LHS -> Limit(SignedDecimalIntegerLiteral(1), DoNotIncludeTies) {
        |    LHS -> VarExpand(IdName(b), INCOMING, OUTGOING, List(), IdName(a), IdName(r), VarPatternLength(1,None), ExpandAll, Vector()) {
        |      LHS -> AllNodesScan(IdName(b), Set()) {}
        |    }
        |  }
        |  RHS -> ProjectEndpoints(IdName(r), IdName(a), true, IdName(b2), false, None, true, VarPatternLength(1,None)) {
        |    LHS -> Argument(Set(IdName(a), IdName(r))) {}
        |  }
        |}""".stripMargin)
  }
}
