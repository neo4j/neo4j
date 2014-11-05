/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.graphdb.Direction
import LogicalPlanProducer._

class WithPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("should build plans for simple WITH that adds a constant to the rows") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 RETURN 1 as `b`").plan
    val expected =
      planRegularProjection(

         planStarProjection(
            planLimit(
              planAllNodesScan("a", Set.empty),
              UnsignedDecimalIntegerLiteral("1") _
            ),
            Map[String, Expression]("a" -> ident("a"))
          ),
        Map[String, Expression]("b" -> SignedDecimalIntegerLiteral("1") _)
      )

    result should equal(expected)
  }

  test("should build plans that contain multiple WITH") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WITH a, b, r1 LIMIT 1 RETURN b as `b`").plan

    result.toString should equal(
    // oh perty where art thou!?
      "Projection(Limit(Expand(Apply(Limit(AllNodesScan(IdName(a),Set()),UnsignedDecimalIntegerLiteral(1)),SingleRow(Set(IdName(a)))),IdName(a),OUTGOING,OUTGOING,List(),IdName(b),IdName(r1),SimplePatternLength,Vector()),UnsignedDecimalIntegerLiteral(1)),Map(b -> Identifier(b)))")
  }

  test("should build plans with WITH and selections") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r1]->(b) WHERE r1.prop = 42 RETURN r1").plan

    result.toString should equal(
      "Projection(Selection(Vector(In(Property(Identifier(r1),PropertyKeyName(prop)),Collection(List(SignedDecimalIntegerLiteral(42))))),Apply(Limit(AllNodesScan(IdName(a),Set()),UnsignedDecimalIntegerLiteral(1)),Expand(SingleRow(Set(IdName(a))),IdName(a),OUTGOING,OUTGOING,List(),IdName(b),IdName(r1),SimplePatternLength,Vector()))),Map(r1 -> Identifier(r1)))" )
  }

  test("should build plans for two matches separated by WITH") {
    val result = planFor("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN b").plan

    result.toString should equal(
      "Projection(Expand(Apply(Limit(AllNodesScan(IdName(a),Set()),UnsignedDecimalIntegerLiteral(1)),SingleRow(Set(IdName(a)))),IdName(a),OUTGOING,OUTGOING,List(),IdName(b),IdName(r),SimplePatternLength,Vector()),Map(b -> Identifier(b)))")
  }

  test("should build plans that project endpoints of re-matched directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH r LIMIT 1 MATCH (u)-[r]->(v) RETURN r").plan

    plan.toString should equal(
      "Projection(Selection(List(Equals(Identifier(u),Identifier(u$$$_))),Apply(Limit(Expand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,OUTGOING,List(),IdName(a),IdName(r),SimplePatternLength,Vector()),UnsignedDecimalIntegerLiteral(1)),ProjectEndpoints(AllNodesScan(IdName(u),Set(IdName(r))),IdName(r),IdName(u$$$_),IdName(v),true,SimplePatternLength))),Map(r -> Identifier(r)))")
  }

  test("should build plans that project endpoints of re-matched reversed directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH r AS r, a AS a LIMIT 1 MATCH (b2)<-[r]-(a) RETURN r").plan

    plan.toString should equal(
      "Projection(Selection(List(Equals(Identifier(a),Identifier(a$$$_))),Apply(Limit(Expand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,OUTGOING,List(),IdName(a),IdName(r),SimplePatternLength,Vector()),UnsignedDecimalIntegerLiteral(1)),ProjectEndpoints(SingleRow(Set(IdName(a), IdName(r))),IdName(r),IdName(a$$$_),IdName(b2),true,SimplePatternLength))),Map(r -> Identifier(r)))")
  }

  test("should build plans that verify endpoints of re-matched directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH * LIMIT 1 MATCH (a)-[r]->(b) RETURN r").plan

    plan.toString should equal(
      "Projection(Selection(List(Equals(Identifier(a),Identifier(a$$$_)), Equals(Identifier(b),Identifier(b$$$_))),Apply(Limit(Expand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,OUTGOING,List(),IdName(a),IdName(r),SimplePatternLength,Vector()),UnsignedDecimalIntegerLiteral(1)),ProjectEndpoints(SingleRow(Set(IdName(a), IdName(b), IdName(r))),IdName(r),IdName(a$$$_),IdName(b$$$_),true,SimplePatternLength))),Map(r -> Identifier(r)))")
  }

  test("should build plans that project and verify endpoints of re-matched directed relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]->(b2) RETURN r").plan

    plan.toString should equal(
      "Projection(Selection(List(Equals(Identifier(a),Identifier(a$$$_))),Apply(Limit(Expand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,OUTGOING,List(),IdName(a),IdName(r),SimplePatternLength,Vector()),UnsignedDecimalIntegerLiteral(1)),ProjectEndpoints(SingleRow(Set(IdName(a), IdName(r))),IdName(r),IdName(a$$$_),IdName(b2),true,SimplePatternLength))),Map(r -> Identifier(r)))")
  }

  test("should build plans that project and verify endpoints of re-matched undirected relationship arguments") {
    val plan = planFor("MATCH (a)-[r]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r]-(b2) RETURN r").plan

    plan.toString should equal(
      "Projection(Selection(List(Equals(Identifier(a),Identifier(a$$$_))),Apply(Limit(Expand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,OUTGOING,List(),IdName(a),IdName(r),SimplePatternLength,Vector()),UnsignedDecimalIntegerLiteral(1)),ProjectEndpoints(SingleRow(Set(IdName(a), IdName(r))),IdName(r),IdName(a$$$_),IdName(b2),false,SimplePatternLength))),Map(r -> Identifier(r)))")
  }

  test("should build plans that project and verify endpoints of re-matched directed var length relationship arguments") {
    val plan = planFor("MATCH (a)-[r*]->(b) WITH a AS a, r AS r LIMIT 1 MATCH (a)-[r*]->(b2) RETURN r").plan

    plan.toString should equal(
      "Projection(Selection(List(Equals(Identifier(a),Identifier(a$$$_))),Apply(Limit(Expand(AllNodesScan(IdName(b),Set()),IdName(b),INCOMING,OUTGOING,List(),IdName(a),IdName(r),VarPatternLength(1,None),Vector()),UnsignedDecimalIntegerLiteral(1)),ProjectEndpoints(SingleRow(Set(IdName(a), IdName(r))),IdName(r),IdName(a$$$_),IdName(b2),true,VarPatternLength(1,None)))),Map(r -> Identifier(r)))")
  }
}
