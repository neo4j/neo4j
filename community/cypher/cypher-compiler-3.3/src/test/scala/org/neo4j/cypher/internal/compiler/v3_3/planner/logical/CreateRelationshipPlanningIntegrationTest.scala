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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical

import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_3.logical.plans._

class CreateRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should plan single create") {
    planFor("CREATE (a)-[r:R]->(b)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          CreateNode(
            CreateNode(SingleRow()(solved), "a", Seq.empty, None)(solved),
            "b", Seq.empty, None)(solved),
          "r", "a", relType("R"), "b", None)(solved)
      )(solved)
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
                    CreateNode(SingleRow()(solved),"a",Seq.empty,None)(solved),
                    "b",Seq.empty,None)(solved),
                  "c",Seq.empty,None)(solved),
                "d",Seq.empty,None)(solved),
              "r1","a",relType("R1"),"b",None)(solved),
            "r2","c",relType("R2"),"b",None)(solved),
          "r3","c",relType("R3"),"d",None)(solved)
      )(solved)
    )
  }

  test("should plan reversed create pattern") {
    planFor("CREATE (a)<-[r1:R1]-(b)<-[r2:R2]-(c)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          CreateRelationship(
            CreateNode(
              CreateNode(
                CreateNode(SingleRow()(solved),"a",Seq.empty,None)(solved),
                "b",Seq.empty,None)(solved),
              "c",Seq.empty,None)(solved),
            "r1","b",relType("R1"),"a",None)(solved),
          "r2","c",relType("R2"),"b",None)(solved)
      )(solved)
    )
  }

  test("should plan only one create node when the other node is already in scope when creating a relationship") {
    planFor("MATCH (n) CREATE (n)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          CreateNode(
            AllNodesScan("n", Set())(solved),
            "b", Seq.empty, None)(solved),
          "r", "n", RelTypeName("T")(pos), "b", None)(solved)
      )(solved)
    )
  }

  test("should not plan two create nodes when they are already in scope when creating a relationship") {
    planFor("MATCH (n) MATCH (m) CREATE (n)-[r:T]->(m)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          CartesianProduct(
            AllNodesScan("n", Set())(solved),
            AllNodesScan("m", Set())(solved)
          )(solved),
          "r", "n", RelTypeName("T")(pos), "m", None)(solved)
      )(solved)
    )
  }

  test("should not plan two create nodes when they are already in scope and aliased when creating a relationship") {
    planFor("MATCH (n) MATCH (m) WITH n AS a, m AS b CREATE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          Projection(
            CartesianProduct(
              AllNodesScan("n", Set())(solved),
              AllNodesScan("m", Set())(solved)
            )(solved), Map("a" -> Variable("n")(pos), "b" -> Variable("m")(pos)))(solved),
          "r", "a", RelTypeName("T")(pos), "b", None)(solved)
      )(solved)
    )
  }

  test("should plan only one create node when the other node is already in scope and aliased when creating a relationship") {
    planFor("MATCH (n) WITH n AS a CREATE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        CreateRelationship(
          CreateNode(
            Projection(
              AllNodesScan("n", Set())(solved),
              Map("a" -> Variable("n")(pos)))(solved),
            "b", Seq.empty, None)(solved),
          "r", "a", RelTypeName("T")(pos), "b", None)(solved)
      )(solved)
    )
  }

  private def relType(name: String): RelTypeName = RelTypeName(name)(pos)
}
