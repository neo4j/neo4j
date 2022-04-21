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
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CreateRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  test("should plan single create") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a)-[r:R]->(b)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        Seq(
          CreateNode("a", Seq.empty, None),
          CreateNode("b", Seq.empty, None)
        ),
        Seq(
          CreateRelationship("r", "a", relTypeName("R"), "b", SemanticDirection.OUTGOING, None)
        )
      )
      .argument()
      .build()
  }

  test("should plan complicated create") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a)-[r1:R1]->(b)<-[r2:R2]-(c)-[r3:R3]->(d)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        Seq(
          CreateNode("a", Seq.empty, None),
          CreateNode("b", Seq.empty, None),
          CreateNode("c", Seq.empty, None),
          CreateNode("d", Seq.empty, None)
        ),
        Seq(
          CreateRelationship("r1", "a", relTypeName("R1"), "b", SemanticDirection.OUTGOING, None),
          CreateRelationship("r2", "b", relTypeName("R2"), "c", SemanticDirection.INCOMING, None),
          CreateRelationship("r3", "c", relTypeName("R3"), "d", SemanticDirection.OUTGOING, None)
        )
      )
      .argument()
      .build()
  }

  test("should plan reversed create pattern") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a)<-[r1:R1]-(b)<-[r2:R2]-(c)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        Seq(
          CreateNode("a", Seq.empty, None),
          CreateNode("b", Seq.empty, None),
          CreateNode("c", Seq.empty, None)
        ),
        Seq(
          CreateRelationship("r1", "a", relTypeName("R1"), "b", SemanticDirection.INCOMING, None),
          CreateRelationship("r2", "b", relTypeName("R2"), "c", SemanticDirection.INCOMING, None)
        )
      )
      .argument()
      .build()
  }

  test("should plan only one create node when the other node is already in scope when creating a relationship") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("MATCH (n) CREATE (n)-[r:T]->(b)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        Seq(
          CreateNode("b", Seq.empty, None)
        ),
        Seq(
          CreateRelationship("r", "n", relTypeName("T"), "b", SemanticDirection.OUTGOING, None)
        )
      )
      .allNodeScan("n")
      .build()
  }

  test("should not plan two create nodes when they are already in scope when creating a relationship") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("MATCH (n) MATCH (m) CREATE (n)-[r:T]->(m)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        Seq.empty,
        Seq(
          CreateRelationship("r", "n", relTypeName("T"), "m", SemanticDirection.OUTGOING, None)
        )
      )
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
      .build()
  }

  test("should not plan two create nodes when they are already in scope and aliased when creating a relationship") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("MATCH (n) MATCH (m) WITH n AS a, m AS b CREATE (a)-[r:T]->(b)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        Seq.empty,
        Seq(
          CreateRelationship("r", "a", relTypeName("T"), "b", SemanticDirection.OUTGOING, None)
        )
      )
      .projection("n AS a", "m AS b")
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
      .build()
  }

  test(
    "should plan only one create node when the other node is already in scope and aliased when creating a relationship"
  ) {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("MATCH (n) WITH n AS a CREATE (a)-[r:T]->(b)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        Seq(
          CreateNode("b", Seq.empty, None)
        ),
        Seq(
          CreateRelationship("r", "a", relTypeName("T"), "b", SemanticDirection.OUTGOING, None)
        )
      )
      .projection("n AS a")
      .allNodeScan("n")
      .build()
  }
}
