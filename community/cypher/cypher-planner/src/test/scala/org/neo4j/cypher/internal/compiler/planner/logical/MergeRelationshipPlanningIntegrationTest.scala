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

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.UsingMatcher.using
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class MergeRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  private def plannerConfigForSimpleExpandTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .setRelationshipCardinality("(:A)-[:R]->()", 100)
      .setRelationshipCardinality("()-[:R]->()", 100)
      .build()

  test("should plan simple expand") {
    val cfg = plannerConfigForSimpleExpandTests()

    val plan = cfg.plan("MERGE (a:A)-[r:R]->(b)").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .merge(
        nodes = Seq(createNode("a", "A"), createNode("b")),
        relationships = Seq(createRelationship("r", "a", "R", "b", OUTGOING)))
      .expandAll("(a)-[r:R]->(b)")
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()
  }

  test("should plan simple expand with argument dependency") {
    val cfg = plannerConfigForSimpleExpandTests()

    val plan = cfg.plan("WITH 42 AS arg MERGE (a:A {p: arg})-[r:R]->(b)").stripProduceResults

    val mergeNodes = Seq(createNodeWithProperties("a", Seq("A"), "{p: arg}"), createNode("b"))
    val mergeRelationships = Seq(createRelationship("r", "a", "R", "b", OUTGOING))

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(mergeNodes, mergeRelationships)
      .|.expandAll("(a)-[r:R]->(b)")
      .|.filter("a.p = arg")
      .|.nodeByLabelScan("a", "A", "arg")
      .projection("42 AS arg")
      .argument()
      .build()
  }

  test("should use AssertSameNode when multiple unique index matches") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("X", 50)
      .setLabelCardinality("Y", 50)
      .addNodeIndex("X", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, isUnique = true)
      .addNodeIndex("Y", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, isUnique = true)
      .setRelationshipCardinality("(:X)-[:T]->()", 100)
      .setRelationshipCardinality("(:Y)-[:T]->()", 100)
      .build()

    val plan = cfg.plan("MERGE (a:X:Y {prop: 42})-[:T]->(b)").stripProduceResults

    plan shouldBe using[AssertSameNode]
    plan shouldBe using[NodeUniqueIndexSeek]
  }

  test("should not use AssertSameNode when one unique index matches") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("X", 50)
      .setLabelCardinality("Y", 50)
      .addNodeIndex("X", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, isUnique = true)
      .setRelationshipCardinality("(:X)-[:T]->()", 100)
      .setRelationshipCardinality("(:Y)-[:T]->()", 100)
      .build()

    val plan = cfg.plan("MERGE (a:X:Y {prop: 42})").stripProduceResults

    plan should not be using[AssertSameNode]
    plan shouldBe using[NodeUniqueIndexSeek]
  }

  test("should plan only one create node when the other node is already in scope when creating a relationship") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:T]->()", 100)
      .build()

    val plan = cfg.plan("MATCH (n) MERGE (n)-[r:T]->(b)").stripProduceResults

    val mergeNodes = Seq(createNode("b"))
    val mergeRelationships = Seq(createRelationship("r", "n", "T", "b", OUTGOING))

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(mergeNodes, mergeRelationships, lockNodes = Set("n"))
      .|.expandAll("(n)-[r:T]->(b)")
      .|.argument("n")
      .allNodeScan("n")
      .build()
  }

  private def plannerConfigForMergeOnExistingVariableTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:T]->()", 10000)
      .build()

  test("should not plan two create nodes when they are already in scope when creating a relationship") {
    val cfg = plannerConfigForMergeOnExistingVariableTests()

    val plan = cfg.plan("MATCH (n) MATCH (m) MERGE (n)-[r:T]->(m)").stripProduceResults

    val mergeRelationships = Seq(createRelationship("r", "n", "T", "m", OUTGOING))
    val mergeLockNodes = Set("n", "m")

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(relationships = mergeRelationships, lockNodes = mergeLockNodes)
      .|.expandInto("(n)-[r:T]->(m)")
      .|.argument("n", "m")
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
      .build()
  }

  test("should not plan two create nodes when they are already in scope and aliased when creating a relationship") {
    val cfg = plannerConfigForMergeOnExistingVariableTests()

    val plan = cfg.plan("MATCH (n) MATCH (m) WITH n AS a, m AS b MERGE (a)-[r:T]->(b)").stripProduceResults

    val mergeRelationships = Seq(createRelationship("r", "a", "T", "b", OUTGOING))
    val mergeLockNodes = Set("a", "b")

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(relationships = mergeRelationships, lockNodes = mergeLockNodes)
      .|.expandInto("(a)-[r:T]->(b)")
      .|.argument("a", "b")
      .projection("n AS a", "m AS b")
      .cartesianProduct()
      .|.allNodeScan("m")
      .allNodeScan("n")
      .build()
  }

  test("should plan only one create node when the other node is already in scope and aliased when creating a relationship") {
    val cfg = plannerConfigForMergeOnExistingVariableTests()

    val plan = cfg.plan("MATCH (n) WITH n AS a MERGE (a)-[r:T]->(b)").stripProduceResults

    val mergeNodes = Seq(createNode("b"))
    val mergeRelationships = Seq(createRelationship("r", "a", "T", "b", OUTGOING))

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .apply()
      .|.merge(mergeNodes, mergeRelationships, lockNodes = Set("a"))
      .|.expandAll("(a)-[r:T]->(b)")
      .|.argument("a")
      .projection("n AS a")
      .allNodeScan("n")
      .build()
  }
}
