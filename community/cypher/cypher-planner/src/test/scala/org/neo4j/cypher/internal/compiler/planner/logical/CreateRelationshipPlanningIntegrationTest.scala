/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CreateRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  test("should plan single create") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a)-[r:R]->(b)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        createNode("a"),
        createNode("b"),
        createRelationship("r", "a", "R", "b", SemanticDirection.OUTGOING)
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
        createNode("a"),
        createNode("b"),
        createRelationship("r1", "a", "R1", "b", SemanticDirection.OUTGOING),
        createNode("c"),
        createRelationship("r2", "b", "R2", "c", SemanticDirection.INCOMING),
        createNode("d"),
        createRelationship("r3", "c", "R3", "d", SemanticDirection.OUTGOING)
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
        createNode("a"),
        createNode("b"),
        createRelationship("r1", "a", "R1", "b", SemanticDirection.INCOMING),
        createNode("c"),
        createRelationship("r2", "b", "R2", "c", SemanticDirection.INCOMING)
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
        createNode("b"),
        createRelationship("r", "n", "T", "b", SemanticDirection.OUTGOING)
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
        createRelationship("r", "n", "T", "m", SemanticDirection.OUTGOING)
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
        createRelationship("r", "a", "T", "b", SemanticDirection.OUTGOING)
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
        createNode("b"),
        createRelationship("r", "a", "T", "b", SemanticDirection.OUTGOING)
      )
      .projection("n AS a")
      .allNodeScan("n")
      .build()
  }
}
