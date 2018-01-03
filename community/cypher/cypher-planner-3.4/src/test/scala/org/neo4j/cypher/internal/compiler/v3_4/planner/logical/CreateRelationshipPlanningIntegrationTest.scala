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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_4.IdName
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.{RelTypeName, Variable}
import org.neo4j.cypher.internal.v3_4.logical.plans._

class CreateRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should plan single create") {
    planFor("CREATE (a)-[r:R]->(b)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          CreateNode(
            CreateNode(Argument(), IdName("a"), Seq.empty, None),
            IdName("b"), Seq.empty, None),
          IdName("r"), IdName("a"), relType("R"), IdName("b"), None)
      )
    )
  }

  test("should plan complicated create") {
    planFor("CREATE (a)-[r1:R1]->(b)<-[r2:R2]-(c)-[r3:R3]->(d)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          CreateRelationship(
            CreateRelationship(
              CreateNode(
                CreateNode(
                  CreateNode(
                    CreateNode(Argument(),IdName("a"),Seq.empty,None),
                    IdName("b"),Seq.empty,None),
                  IdName("c"),Seq.empty,None),
                IdName("d"),Seq.empty,None),
              IdName("r1"),IdName("a"),relType("R1"),IdName("b"),None),
            IdName("r2"),IdName("c"),relType("R2"),IdName("b"),None),
          IdName("r3"),IdName("c"),relType("R3"),IdName("d"),None)
      )
    )
  }

  test("should plan reversed create pattern") {
    planFor("CREATE (a)<-[r1:R1]-(b)<-[r2:R2]-(c)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          CreateRelationship(
            CreateNode(
              CreateNode(
                CreateNode(Argument(),IdName("a"),Seq.empty,None),
                IdName("b"),Seq.empty,None),
              IdName("c"),Seq.empty,None),
            IdName("r1"),IdName("b"),relType("R1"),IdName("a"),None),
          IdName("r2"),IdName("c"),relType("R2"),IdName("b"),None)
      )
    )
  }

  test("should plan only one create node when the other node is already in scope when creating a relationship") {
    planFor("MATCH (n) CREATE (n)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          CreateNode(
            AllNodesScan(IdName("n"), Set()),
            IdName("b"), Seq.empty, None),
          IdName("r"), IdName("n"), RelTypeName("T")(pos), IdName("b"), None)
      )
    )
  }

  test("should not plan two create nodes when they are already in scope when creating a relationship") {
    planFor("MATCH (n) MATCH (m) CREATE (n)-[r:T]->(m)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          CartesianProduct(
            AllNodesScan(IdName("n"), Set()),
            AllNodesScan(IdName("m"), Set())
          ),
          IdName("r"), IdName("n"), RelTypeName("T")(pos), IdName("m"), None)
      )
    )
  }

  test("should not plan two create nodes when they are already in scope and aliased when creating a relationship") {
    planFor("MATCH (n) MATCH (m) WITH n AS a, m AS b CREATE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          Projection(
            CartesianProduct(
              AllNodesScan(IdName("n"), Set()),
              AllNodesScan(IdName("m"), Set())
            ), Map("a" -> Variable("n")(pos), "b" -> Variable("m")(pos))),
          IdName("r"), IdName("a"), RelTypeName("T")(pos), IdName("b"), None)
      )
    )
  }

  test("should plan only one create node when the other node is already in scope and aliased when creating a relationship") {
    planFor("MATCH (n) WITH n AS a CREATE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          CreateNode(
            Projection(
              AllNodesScan(IdName("n"), Set()),
              Map("a" -> Variable("n")(pos))),
            IdName("b"), Seq.empty, None),
          IdName("r"), IdName("a"), RelTypeName("T")(pos), IdName("b"), None)
      )
    )
  }

  private def relType(name: String): RelTypeName = RelTypeName(name)(pos)
}
