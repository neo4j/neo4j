/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{AllNodesScan, Limit, Projection}
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Expression, SignedDecimalIntegerLiteral}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class WithPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("should build plans for simple WITH that adds a constant to the rows") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 RETURN 1 as `b`").plan
    val expected =
      Projection(
        Limit(
          AllNodesScan("a", Set.empty)(solved),
          SignedDecimalIntegerLiteral("1")(pos)
        )(solved),
        Map[String, Expression]("b" -> SignedDecimalIntegerLiteral("1") _)
      )(solved)

    result should equal(expected)
  }

  test("should build plans that contain multiple WITH") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WITH a, b, r1 LIMIT 1 RETURN b as `b`").plan

    val resultText = result.toString

    resultText should equal(
      // oh perty where art thou!?
      "Limit(Expand(Limit(AllNodesScan(IdName(a),Set()),SignedDecimalIntegerLiteral(1)),IdName(a),OUTGOING,List(),IdName(b),IdName(r1),ExpandAll),SignedDecimalIntegerLiteral(1))")
  }

  test("should build plans with WITH and selections") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WHERE r1.prop = 42 RETURN r1").plan

    result.toString should equal(
      "Selection(Vector(In(Property(Identifier(r1),PropertyKeyName(prop)),Collection(List(SignedDecimalIntegerLiteral(42))))),Expand(Limit(AllNodesScan(IdName(a),Set()),SignedDecimalIntegerLiteral(1)),IdName(a),OUTGOING,List(),IdName(b),IdName(r1),ExpandAll))")
  }

  test("should build plans for two matches separated by WITH") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN b").plan

    result.toString should equal(
      "Expand(Limit(AllNodesScan(IdName(a),Set()),SignedDecimalIntegerLiteral(1)),IdName(a),OUTGOING,List(),IdName(b),IdName(r),ExpandAll)")
  }

  test("should build plans that project endpoints of re-matched directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH r LIMIT 1 MATCH (u)-[r]->(v) RETURN r").plan

    plan.toString should equal(
      "Apply(Limit(Expand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,List(),IdName(a),IdName(r),ExpandAll),SignedDecimalIntegerLiteral(1)),ProjectEndpoints(Argument(Set(IdName(r))),IdName(r),IdName(u),false,IdName(v),false,None,true,SimplePatternLength))")
  }

  test("should build plans that project endpoints of re-matched reversed directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH r AS r, a AS a LIMIT 1 MATCH (b2)<-[r]-(a) RETURN r").plan

    plan.toString should equal(
      "Apply(Limit(Expand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,List(),IdName(a),IdName(r),ExpandAll),SignedDecimalIntegerLiteral(1)),ProjectEndpoints(Argument(Set(IdName(a), IdName(r))),IdName(r),IdName(a),true,IdName(b2),false,None,true,SimplePatternLength))")
  }

  test("should build plans that verify endpoints of re-matched directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH * LIMIT 1 MATCH (a)-[r]->(b) RETURN r").plan

    plan.toString should equal(
      "Apply(Limit(Expand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,List(),IdName(a),IdName(r),ExpandAll),SignedDecimalIntegerLiteral(1)),ProjectEndpoints(Argument(Set(IdName(a), IdName(b), IdName(r))),IdName(r),IdName(a),true,IdName(b),true,None,true,SimplePatternLength))")
  }

  test("should build plans that project and verify endpoints of re-matched directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]->(b2) RETURN r").plan

    plan.toString should equal(
      "Apply(Limit(Expand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,List(),IdName(a),IdName(r),ExpandAll),SignedDecimalIntegerLiteral(1)),ProjectEndpoints(Argument(Set(IdName(a), IdName(r))),IdName(r),IdName(a),true,IdName(b2),false,None,true,SimplePatternLength))")
  }

  test("should build plans that project and verify endpoints of re-matched undirected relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]-(b2) RETURN r").plan

    plan.toString should equal(
      "Apply(Limit(Expand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,List(),IdName(a),IdName(r),ExpandAll),SignedDecimalIntegerLiteral(1)),ProjectEndpoints(Argument(Set(IdName(a), IdName(r))),IdName(r),IdName(a),true,IdName(b2),false,None,false,SimplePatternLength))")
  }

  test("should build plans that project and verify endpoints of re-matched directed var length relationship arguments") {
    val plan = planFor("MATCH (a)-[r*]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r*]->(b2) RETURN r").plan

    plan.toString should equal(
      "Apply(Limit(VarExpand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,OUTGOING,List(),IdName(a),IdName(r),VarPatternLength(1,None),ExpandAll,Vector()),SignedDecimalIntegerLiteral(1)),ProjectEndpoints(Argument(Set(IdName(a), IdName(r))),IdName(r),IdName(a),true,IdName(b2),false,None,true,VarPatternLength(1,None)))")
  }
}
