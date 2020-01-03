/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_5.{CreateNode, CreateRelationship}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_5.expressions.{RelTypeName, SemanticDirection, Variable}
import org.neo4j.cypher.internal.v3_5.logical.plans._

class CreateRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should plan single create") {
    planFor("CREATE (a)-[r:R]->(b)")._2 should equal(
      EmptyResult(
        Create(
          Argument(),
          List(
            CreateNode("a", Seq.empty, None),
            CreateNode("b", Seq.empty, None)
          ),
          List(
            CreateRelationship("r", "a", relType("R"), "b", SemanticDirection.OUTGOING, None)
          )
        )
      )
    )
  }

  test("should plan complicated create") {
    planFor("CREATE (a)-[r1:R1]->(b)<-[r2:R2]-(c)-[r3:R3]->(d)")._2 should equal(
      EmptyResult(
        Create(
          Argument(),
          List(
            CreateNode("a", Seq.empty, None),
            CreateNode("b", Seq.empty, None),
            CreateNode("c", Seq.empty, None),
            CreateNode("d", Seq.empty, None)
          ),
          List(
            CreateRelationship("r1", "a", relType("R1"), "b", SemanticDirection.OUTGOING, None),
            CreateRelationship("r2", "b", relType("R2"), "c", SemanticDirection.INCOMING, None),
            CreateRelationship("r3", "c", relType("R3"), "d", SemanticDirection.OUTGOING, None)
          )
        )
      )
    )
  }

  test("should plan reversed create pattern") {
    planFor("CREATE (a)<-[r1:R1]-(b)<-[r2:R2]-(c)")._2 should equal(
      EmptyResult(
        Create(
          Argument(),
          List(
            CreateNode("a", Seq.empty, None),
            CreateNode("b", Seq.empty, None),
            CreateNode("c", Seq.empty, None)
          ),
          List(
            CreateRelationship("r1", "a", relType("R1"), "b", SemanticDirection.INCOMING, None),
            CreateRelationship("r2", "b", relType("R2"), "c", SemanticDirection.INCOMING, None)
          )
        )
      )
    )
  }

  test("should plan only one create node when the other node is already in scope when creating a relationship") {
    planFor("MATCH (n) CREATE (n)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Create(
          AllNodesScan("n", Set()),
          List(
            CreateNode("b", Seq.empty, None)
          ),
          List(
            CreateRelationship("r", "n", relType("T"), "b", SemanticDirection.OUTGOING, None)
          )
        )
      )
    )
  }

  test("should not plan two create nodes when they are already in scope when creating a relationship") {
    planFor("MATCH (n) MATCH (m) CREATE (n)-[r:T]->(m)")._2 should equal(
      EmptyResult(
        Create(
          CartesianProduct(
            AllNodesScan("n", Set()),
            AllNodesScan("m", Set())
          ),
          Nil,
          List(
            CreateRelationship("r", "n", relType("T"), "m", SemanticDirection.OUTGOING, None)
          )
        )
      )
    )
  }

  test("should not plan two create nodes when they are already in scope and aliased when creating a relationship") {
    planFor("MATCH (n) MATCH (m) WITH n AS a, m AS b CREATE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Create(
          Projection(
            CartesianProduct(
              AllNodesScan("n", Set()),
              AllNodesScan("m", Set())
            ),
            Map(
              "a" -> Variable("n")(pos),
              "b" -> Variable("m")(pos)
            )
          ),
          Nil,
          List(
            CreateRelationship("r", "a", relType("T"), "b", SemanticDirection.OUTGOING, None)
          )
        )
      )
    )
  }

  test("should plan only one create node when the other node is already in scope and aliased when creating a relationship") {
    planFor("MATCH (n) WITH n AS a CREATE (a)-[r:T]->(b)")._2 should equal(
      EmptyResult(
        Create(
          Projection(
            AllNodesScan("n", Set()),
            Map("a" -> Variable("n")(pos))
          ),
          List(
            CreateNode("b", Seq.empty, None)
          ),
          List(
            CreateRelationship("r", "a", relType("T"), "b", SemanticDirection.OUTGOING, None)
          )
        )
      )
    )
  }

  private def relType(name: String): RelTypeName = RelTypeName(name)(pos)
}
