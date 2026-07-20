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
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.column
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.schema.EndpointType

class GraphSchemaOptimizationsPlanningIntegrationTest extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport
    with LogicalPlanningAttributesTestSupport
    with AstConstructionTestSupport {

  val plannerWithNodeLabelConstraints: StatisticsBackedLogicalPlanningConfigurationBuilder =
    plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .setLabelCardinality("X", 800)
      .setLabelCardinality("Y", 1200)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "X", impliedLabel = "Y")
      .setAllNodesCardinality(3000)
      .setAllRelationshipsCardinality(30000)
      .setRelationshipCardinality("()-[:R]->()", 20000)
      .setRelationshipCardinality("(:Actor)-[:R]->()", 2000)
      .setRelationshipCardinality("(:Person)-[:R]->()", 2000)
      .setRelationshipCardinality("(:X)-[:R]->()", 2000)
      .setRelationshipCardinality("(:Y)-[:R]->()", 2000)
      .setRelationshipCardinality("()-[:R]->(:X)", 2000)
      .setRelationshipCardinality("()-[:R]->(:Y)", 2000)
      .setRelationshipCardinality("(:Actor)-[:R]->(:X)", 2000)
      .setRelationshipCardinality("(:Person)-[:R]->(:X)", 2000)
      .setRelationshipCardinality("(:X)-[:R]->(:X)", 2000)
      .setRelationshipCardinality("(:Y)-[:R]->(:X)", 2000)
      .setRelationshipCardinality("(:Actor)-[:R]->(:Y)", 2000)
      .setRelationshipCardinality("(:Person)-[:R]->(:Y)", 2000)
      .setRelationshipCardinality("(:X)-[:R]->(:Y)", 2000)
      .setRelationshipCardinality("(:Y)-[:R]->(:Y)", 2000)

  test("should not plan filter for a directly implied label") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .setAllNodesCardinality(3000)
      .build()

    val query = "MATCH (n:Actor:Person) RETURN n"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .nodeByLabelScan("n", "Actor")
      .build()
  }

  test("should not plan filter, one constrained label, multiple implied labels") {
    val planner = plannerBuilder()
      .setLabelCardinality("Entity", 1500)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Entity")
      .setAllNodesCardinality(3000)
      .build()

    val query = "MATCH (n:Actor:Person:Entity) RETURN n"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .nodeByLabelScan("n", "Actor")
      .build()
  }

  test("should plan intersection scan if filter cannot be removed") {
    val planner = plannerBuilder()
      .setLabelCardinality("Entity", 1500)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .setLabelCardinality("Other", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Entity")
      .setAllNodesCardinality(3000)
      .build()

    val query = "MATCH (n:Actor:Person:Entity:Other) RETURN n"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .intersectionNodeByLabelsScan("n", Seq("Actor", "Other"))
      .build()
  }

  test(
    "should plan intersection scan on constrained label implying other label if the constrained label is hinted on"
  ) {
    val planner = plannerBuilder()
      .setLabelCardinality("Entity", 1500)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .setLabelCardinality("Other", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Entity")
      .setAllNodesCardinality(3000)
      .build()

    val query =
      """MATCH (n:Actor:Person:Entity:Other)
        |USING SCAN n:Entity
        |RETURN n""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      // Entity is the last label in the scan because it was added due to the hint and would have been eliminated otherwise
      .intersectionNodeByLabelsScan("n", Seq("Actor", "Entity", "Other"))
      .build()
  }

  test("should plan union scan on implied label as well as hinted labels") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1500)
      .setLabelCardinality("Director", 1000)
      .setLabelCardinality("Actor", 500)
      .setLabelCardinality("Other", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "Director", impliedLabel = "Person")
      .setAllNodesCardinality(3000)
      .build()

    val query =
      """MATCH (n:Actor|Person|Director|Other)
        |USING SCAN n:Director
        |RETURN n""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      // Director is the last label in the scan because it was added due to the hint and would have been eliminated otherwise
      .unionNodeByLabelsScan("n", Seq("Person", "Other", "Director"))
      .build()
  }

  test("should plan union scan on implied label, ignoring the constrained labels") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1500)
      .setLabelCardinality("Director", 1000)
      .setLabelCardinality("Actor", 500)
      .setLabelCardinality("Other", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "Director", impliedLabel = "Person")
      .setAllNodesCardinality(3000)
      .build()

    val query =
      """MATCH (n:Actor|Person|Director|Other)
        |USING SCAN n:Person
        |RETURN n""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .unionNodeByLabelsScan("n", Seq("Person", "Other"))
      .build()
  }

  test("should plan subtraction scan on implied label") {
    val planner = plannerBuilder()
      .setLabelCardinality("SciFiFan", 1500)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .setLabelCardinality("StarTrekFan", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "StarTrekFan", impliedLabel = "SciFiFan")
      .setAllNodesCardinality(3000)
      .build()

    val query =
      """MATCH (n:Actor&Person&!StarTrekFan&!SciFiFan)
        |RETURN n""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      // We subtract the largest implied label from the smallest constrained labels.
      .subtractionNodeByLabelsScan("n", Seq("Actor"), Seq("SciFiFan"))
      .build()
  }

  test("should plan normal label scan if all but one label of intersection are implied") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1500)
      .setLabelCardinality("Director", 1000)
      .setLabelCardinality("Actor", 500)
      .setLabelCardinality("Other", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "Director", impliedLabel = "Person")
      .setAllNodesCardinality(3000)
      .build()

    val query =
      """MATCH (n:Actor:Person)
        |RETURN n""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .nodeByLabelScan("n", "Actor")
      .build()
  }

  test("should plan normal label scan if all but one label of union are implied") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1500)
      .setLabelCardinality("Director", 1000)
      .setLabelCardinality("Actor", 500)
      .setLabelCardinality("Other", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "Director", impliedLabel = "Person")
      .setAllNodesCardinality(3000)
      .build()

    val query =
      """MATCH (n:Actor|Person)
        |RETURN n""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .nodeByLabelScan("n", "Person")
      .build()
  }

  test("should plan filter + index scan on implied label") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeIndex("Person", List("pid"), 0.1, 0.2)
      .setAllNodesCardinality(3000)
      .build()

    val query =
      """MATCH (n:Actor)
        |WHERE n.pid IS NOT NULL
        |RETURN n
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("n:Actor")
      .nodeIndexOperator("n:Person(pid)", _ => GetValue)
      .build()
  }

  test("Should not plan a label filter if that label is implied by the relationship type and end-node constraint") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 50)
      .setAllRelationshipsCardinality(200)
      .setRelationshipCardinality("()-[:R]->()", 200)
      .setRelationshipCardinality("()-[:R]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R]->()", 100)
      .setRelationshipCardinality("(:A)-[:R]->(:B)", 100)
      .addRelationshipEndpointLabelConstraint("()-[:R]->(:B)")
      .build()

    val query =
      """
        |MATCH (a:A)-[r:R]->(b:B)
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[:R]->()")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("Should plan selections when it cannot be implied from its source plan") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("B", 70)
      .setAllRelationshipsCardinality(200)
      .setRelationshipCardinality("()-[:R]->()", 100)
      .setRelationshipCardinality("()-[:R]->(:B)", 100)
      .setRelationshipCardinality("(:A)-[:R]->()", 100)
      .setRelationshipCardinality("(:A)-[:R]->(:B)", 100)
      .setRelationshipCardinality("()-[:S]->()", 100)
      .setRelationshipCardinality("(:B)-[:S]->()", 100)
      .addRelationshipEndpointLabelConstraint("(:B)-[:S]->()")
      .build()

    val query =
      """
        |MATCH (a:A)-[r:R]->(b:B)-[s:S]->()
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .expandAll("(b)-[:S]->()")
      // this could be implied from :S but we cannot imply it from the source plan
      .filter("b:B")
      .expandAll("(a)-[:R]->(b)")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("Should not plan selections when it can be implied from its source plan") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .setLabelCardinality("B", 70)
      .setAllRelationshipsCardinality(200)
      .setRelationshipCardinality("()-[:R]->()", 100)
      .setRelationshipCardinality("()-[:R]->(:B)", 100)
      .setRelationshipCardinality("(:A)-[:R]->()", 100)
      .setRelationshipCardinality("(:A)-[:R]->(:B)", 100)
      .setRelationshipCardinality("()-[:S]->()", 100)
      .setRelationshipCardinality("(:B)-[:S]->()", 100)
      .addRelationshipEndpointLabelConstraint("()-[:R]->(:B)")
      .build()

    val query =
      """
        |MATCH (a:A)-[r:R]->(b:B)-[s:S]->()
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .expandAll("(b)-[:S]->()")
      .expandAll("(a)-[:R]->(b)")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("Should not plan selection :B but should plan selection :C when :C cannon be implied from its source plan") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 5)
      .setLabelCardinality("B", 700)
      .setLabelCardinality("C", 700)
      .setAllRelationshipsCardinality(200)
      .setRelationshipCardinality("()-[:R]->()", 100)
      .setRelationshipCardinality("()-[:R]->(:A)", 100)
      .setRelationshipCardinality("(:B)-[:R]->()", 100)
      .setRelationshipCardinality("(:C)-[:R]->()", 100)
      .setRelationshipCardinality("(:B)-[:R]->(:A)", 100)
      .setRelationshipCardinality("(:C)-[:R]->(:A)", 100)
      .setRelationshipCardinality("()-[:S]->()", 100)
      .setRelationshipCardinality("()-[:S]->(:B)", 100)
      .setRelationshipCardinality("()-[:S]->(:C)", 100)
      .addRelationshipEndpointLabelConstraint("(:B)-[:R]->()")
      .addRelationshipEndpointLabelConstraint("()-[:S]->(:C)")
      .build()

    val query =
      """
        |MATCH (a:A)<-[r:R]-(bc:B&C)<-[s:S]-()
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .expandAll("(bc)<-[:S]-()")
      .filter("bc:C")
      .expandAll("(a)<-[:R]-(bc)")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test(
    "Should plan a relationshipScan when labels on both sides can be implied by the relationship types and end-node constraints and label scans are more expansive"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(2000)
      .setLabelCardinality("A", 700)
      .setLabelCardinality("B", 1200)
      .setAllRelationshipsCardinality(200)
      .setRelationshipCardinality("()-[:R]->()", 200)
      .setRelationshipCardinality("()-[:R]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R]->()", 200)
      .setRelationshipCardinality("(:A)-[:R]->(:B)", 200)
      .addRelationshipEndpointLabelConstraint("(:A)-[:R]->(:B)")
      .build()

    val query =
      """
        |MATCH (a:A)-[r:R]->(b:B)
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .relationshipTypeScan("(a)-[:R]->()")
      .build()
  }

  test("Should plan a label filter when it cannot be implied by the relationship type and end-node constraint") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 20)
      .setAllRelationshipsCardinality(200)
      .setRelationshipCardinality("()-[:R]->()", 200)
      .setRelationshipCardinality("()-[:R]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R]->()", 100)
      .setRelationshipCardinality("(:A)-[:R]->(:B)", 100)
      .setRelationshipCardinality("(:B)-[]->(:B)", 15)
      .setRelationshipCardinality("(:B)-[]->()", 15)
      .setRelationshipCardinality("()-[]->(:B)", 200)
      .addRelationshipEndpointLabelConstraint("()-[:R]->(:B)")
      .build()

    val query =
      """
        |MATCH (a:A)-[r1:R]->(m)-[r2]->(b:B)
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .filter("b:B", "NOT r2 = r1")
      .expandAll("(m)-[r2]->(b)")
      .expandAll("(a)-[r1:R]->(m)")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test(
    "Should still plan to start from label B when that is cheaper, even though B is implied by R using an end-node constraint"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 900)
      .setLabelCardinality("B", 10)
      .setAllRelationshipsCardinality(200)
      .setRelationshipCardinality("()-[:R]->()", 200)
      .setRelationshipCardinality("()-[:R]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R]->()", 100)
      .setRelationshipCardinality("(:A)-[:R]->(:B)", 100)
      .addRelationshipEndpointLabelConstraint("()-[:R]->(:B)")
      .build()

    val query =
      """
        |MATCH (a:A)-[r:R]->(b:B)
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .filter("a:A")
      .expandAll("(b)<-[:R]-(a)")
      .nodeByLabelScan("b", "B")
      .build()
  }

  test(
    "Should not plan a label filter for B if that label is implied by the relationship type and end-node constraint, but should still plan a label filter for C that is not implied by a constraint"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 90)
      .setLabelCardinality("C", 90)
      .setAllRelationshipsCardinality(200)
      .setRelationshipCardinality("()-[:R]->()", 200)
      .setRelationshipCardinality("()-[:R]->(:B)", 200)
      .setRelationshipCardinality("()-[:R]->(:C)", 150)
      .setRelationshipCardinality("(:A)-[:R]->()", 100)
      .setRelationshipCardinality("(:A)-[:R]->(:B)", 100)
      .setRelationshipCardinality("(:A)-[:R]->(:C)", 90)
      .addRelationshipEndpointLabelConstraint("()-[:R]->(:B)")
      .build()

    val query =
      """
        |MATCH (a:A)-[r:R]->(bc:B&C)
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .filter("bc:C")
      .expandAll("(a)-[:R]->(bc)")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("Should plan a label scan for omitted implied label") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setAllRelationshipsCardinality(200)
      .setRelationshipCardinality("()-[:R]->()", 200)
      .setRelationshipCardinality("(:A)-[:R]->()", 200)
      .addRelationshipEndpointLabelConstraint("(:A)-[:R]->()")
      .build()

    val query =
      """
        |MATCH (a)-[r:R]->(b)
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[:R]->()")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("Should plan a label scan for specified implied label") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setAllRelationshipsCardinality(200)
      .setRelationshipCardinality("()-[:R]->()", 200)
      .setRelationshipCardinality("(:A)-[:R]->()", 200)
      .addRelationshipEndpointLabelConstraint("(:A)-[:R]->()")
      .build()

    val query =
      """
        |MATCH (a:A)-[r:R]->(b)
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[:R]->()")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test(
    "Should plan a label filter when not all relationship types in a disjunction imply the label"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 90)
      .setLabelCardinality("C", 90)
      .setAllRelationshipsCardinality(2000)
      .setRelationshipCardinality("()-[:R1]->()", 200)
      .setRelationshipCardinality("()-[:R1]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R1]->()", 100)
      .setRelationshipCardinality("(:A)-[:R1]->(:B)", 100)
      .setRelationshipCardinality("()-[:R2]->()", 200)
      .setRelationshipCardinality("()-[:R2]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R2]->()", 100)
      .setRelationshipCardinality("(:A)-[:R2]->(:B)", 100)
      .addRelationshipEndpointLabelConstraint("()-[:R1]->(:B)")
      .build()

    val query =
      """
        |MATCH (a:A)-[r:R1|R2]->(b:B)
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .filter("b:B")
      .expandAll("(a)-[:R1|R2]->(b)")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test(
    "Should not plan an index when not all relationship types in a disjunction imply the label"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 90)
      .setLabelCardinality("C", 90)
      .setAllRelationshipsCardinality(2000)
      .setRelationshipCardinality("()-[:R1]->()", 200)
      .setRelationshipCardinality("()-[:R1]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R1]->()", 100)
      .setRelationshipCardinality("(:A)-[:R1]->(:B)", 100)
      .setRelationshipCardinality("()-[:R2]->()", 200)
      .setRelationshipCardinality("()-[:R2]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R2]->()", 100)
      .setRelationshipCardinality("(:A)-[:R2]->(:B)", 100)
      .addRelationshipEndpointLabelConstraint("()-[:R1]->(:B)")
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.01)
      .build()

    val query =
      """
        |MATCH (a)-[r:R1|R2]->(b)
        |  WHERE b.prop = 'foo'
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .expandAll("(b)<-[:R1|R2]-(a)")
      .filter("b.prop = 'foo'")
      .allNodeScan("b")
      .build()
  }

  test(
    "Should plan an index when all relationship types in a disjunction imply the label"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 90)
      .setLabelCardinality("C", 90)
      .setAllRelationshipsCardinality(2000)
      .setRelationshipCardinality("()-[:R1]->()", 200)
      .setRelationshipCardinality("()-[:R1]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R1]->()", 100)
      .setRelationshipCardinality("(:A)-[:R1]->(:B)", 100)
      .setRelationshipCardinality("()-[:R2]->()", 200)
      .setRelationshipCardinality("()-[:R2]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R2]->()", 100)
      .setRelationshipCardinality("(:A)-[:R2]->(:B)", 100)
      .addRelationshipEndpointLabelConstraint("()-[:R1]->(:B)")
      .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.01)
      .build()

    val query =
      """
        |MATCH (a)-[r:R1|R2]->(b)
        |  WHERE b.prop = 'foo'
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .expandAll("(b)<-[:R1|R2]-(a)")
      .nodeIndexOperator("b:B(prop = 'foo')")
      .build()
  }

  private val undirectedRelationshipPlannerBase = plannerBuilder()
    .setAllNodesCardinality(3000)
    .setLabelCardinality("Person", 1000)
    .setRelationshipCardinality("()-[:KNOWS]->()", 2000)
    .setRelationshipCardinality("(:Person)-[:KNOWS]->()", 2000)
    .setRelationshipCardinality("()-[:KNOWS]->(:Person)", 2000)
    .setRelationshipCardinality("(:Person)-[:KNOWS]->(:Person)", 2000)

  test(
    "should be able to use labels from undirected relationship for planning an index if constraint on start and end"
  ) {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:KNOWS]->(:Person)")
      .addNodeIndex("Person", Seq("name"), existsSelectivity = 0.5, uniqueSelectivity = 0.1)
      .build()

    val plan = planner.plan(
      """MATCH (n)-[r:KNOWS]-(m)
        |WHERE n.name = 'Alice'
        |RETURN n, m
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults(column("n", "cacheN[n.name]"), column("m"))
        .expandAll("(n)-[:KNOWS]-(m)")
        // we can use the index here because the label is implied through the relationship
        .nodeIndexOperator("n:Person(name = 'Alice')", _ => GetValue)
        .build()
    )
  }

  test("should NOT use labels from undirected relationship for planning an index if constraint only on start") {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:KNOWS]->()")
      .addNodeIndex("Person", Seq("name"), existsSelectivity = 0.5, uniqueSelectivity = 0.1)
      .build()

    val plan = planner.plan(
      """MATCH (n)-[r:KNOWS]-(m)
        |WHERE n.name = 'Alice'
        |RETURN n, m
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n", "m")
        .expandAll("(n)-[:KNOWS]-(m)")
        .filter("n.name = 'Alice'")
        .allNodeScan("n")
        .build()
    )
  }

  test("should use labels from undirected relationship for planning an index if relationship is a self-loop") {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:KNOWS]->()")
      .addNodeIndex("Person", Seq("name"), existsSelectivity = 0.5, uniqueSelectivity = 0.1)
      .build()

    val plan = planner.plan(
      """MATCH (n)-[r:KNOWS]-(n)
        |WHERE n.name = 'Alice'
        |RETURN n
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults(column("n", "cacheN[n.name]"))
        .expandInto("(n)-[:KNOWS]-(n)")
        // we can use the index here because the label is implied through the relationship
        .nodeIndexOperator("n:Person(name = 'Alice')", _ => GetValue)
        .build()
    )
  }

  test("should not plan a label filter when undirected relationship is a self-loop - START node constrained") {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:KNOWS]->()")
      .build()

    val plan = planner.plan(
      """MATCH (n:Person)-[r:KNOWS]-(n)
        |RETURN n
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        // no label filter needed here
        .filter("n = anon_0")
        .relationshipTypeScan("(n)-[:KNOWS]-(anon_0)")
        .build()
    )
  }

  test("should not plan a label filter when undirected relationship is a self-loop - END node constrained") {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("()-[:KNOWS]->(:Person)")
      .build()

    val plan = planner.plan(
      """MATCH (n:Person)-[r:KNOWS]-(n)
        |RETURN n
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        // no label filter needed here
        .filter("n = anon_0")
        .relationshipTypeScan("(n)-[:KNOWS]-(anon_0)")
        .build()
    )
  }

  test(
    "Should not plan a label filter when all relationship types in a disjunction imply the label"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setLabelCardinality("B", 90)
      .setLabelCardinality("C", 90)
      .setAllRelationshipsCardinality(2000)
      .setRelationshipCardinality("()-[:R1]->()", 200)
      .setRelationshipCardinality("()-[:R1]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R1]->()", 100)
      .setRelationshipCardinality("(:A)-[:R1]->(:B)", 100)
      .setRelationshipCardinality("()-[:R2]->()", 200)
      .setRelationshipCardinality("()-[:R2]->(:B)", 200)
      .setRelationshipCardinality("(:A)-[:R2]->()", 100)
      .setRelationshipCardinality("(:A)-[:R2]->(:B)", 100)
      .addRelationshipEndpointLabelConstraint("()-[:R1]->(:B)")
      .addRelationshipEndpointLabelConstraint("()-[:R2]->(:B)")
      .build()

    val query =
      """
        |MATCH (a:A)-[r:R1|R2]->(b:B)
        |RETURN a
        |""".stripMargin

    val res = planner.plan(query).stripProduceResults
    res shouldEqual planner.subPlanBuilder()
      .expandAll("(a)-[:R1|R2]->()")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("should plan index scan with implied label") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeIndex("Person", Seq("pet"), existsSelectivity = 0.5, uniqueSelectivity = 0.01)
      .setAllNodesCardinality(3000)
      .build()

    val query =
      """MATCH (n:Actor)
        |  WHERE n.pet IS NOT NULL
        |RETURN 1 AS result""".stripMargin
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("1 AS result")
      .filter("n:Actor")
      .nodeIndexOperator("n:Person(pet)")
      .build()
  }

  test("should not plan index scan with implied label if original label scan with IS NOT NULL predicate is cheaper") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 320)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeIndex("Person", Seq("pet"), existsSelectivity = 0.5, uniqueSelectivity = 0.01)
      .setAllNodesCardinality(3000)
      .build()

    val query = "MATCH (n:Actor) WHERE n.pet IS NOT NULL RETURN 1 AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("1 AS result")
      .filter("n.pet IS NOT NULL")
      .nodeByLabelScan("n", "Actor")
      .build()
  }

  test("should plan index seek with implied label") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeIndex("Person", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 0.01)
      .setAllNodesCardinality(3000)
      .build()

    for (op <- Seq("=", "<", ">", "<=", ">=", "STARTS WITH")) {
      val query =
        s"""MATCH (n:Actor)
           |WHERE n.prop $op 'Hello'
           |RETURN 1 AS result
           |""".stripMargin

      withClue(query) {
        val plan = planner.plan(query).stripProduceResults
        plan shouldEqual planner.subPlanBuilder()
          .projection("1 AS result")
          .filter("n:Actor")
          .nodeIndexOperator(s"n:Person(prop $op 'Hello')")
          .build()
      }
    }
  }

  test("should plan index seek with implied label, TEXT index") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeIndex("Person", Seq("name"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .setAllNodesCardinality(3000)
      .build()

    for (op <- Seq("STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val query =
        s"""MATCH (n:Actor)
           |WHERE n.name $op 'Bob'
           |RETURN 1 AS result
           |""".stripMargin

      withClue(query) {
        val plan = planner.plan(query).stripProduceResults
        plan shouldEqual planner.subPlanBuilder()
          .projection("1 AS result")
          .filter("n:Actor")
          .nodeIndexOperator(s"n:Person(name $op 'Bob')", indexType = IndexType.TEXT, supportPartitionedScan = false)
          .build()
      }
    }
  }

  test("should plan index seek with implied label, POINT index") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeIndex("Person", Seq("loc"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.POINT)
      .setAllNodesCardinality(3000)
      .build()

    val query =
      s"""MATCH (n:Actor)
         |WHERE point.distance(n.loc, point({x: 123, y: 456})) < 123.0
         |RETURN 1 AS result
         |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("1 AS result")
      .filter("point.distance(cacheN[n.loc], point({x: 123, y: 456})) < 123.0", "n:Actor")
      .pointDistanceNodeIndexSeek("n", "Person", "loc", "{x: 123, y: 456}", 123.0, getValue = GetValue)
      .build()
  }

  test("should plan index scan with implied label for type predicate") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeIndex("Person", Seq("x"), existsSelectivity = 0.5, uniqueSelectivity = 0.01, indexType = IndexType.POINT)
      .addNodeIndex("Person", Seq("x"), existsSelectivity = 0.5, uniqueSelectivity = 0.01, indexType = IndexType.TEXT)
      .setAllNodesCardinality(3000)
      .build()

    for ((typePred, indexType) <- Seq(("STRING", IndexType.TEXT), ("POINT", IndexType.POINT))) {
      val query =
        s"""MATCH (n:Actor)
           |WHERE n.x :: $typePred NOT NULL
           |RETURN 1 AS result
           |""".stripMargin

      withClue(query) {
        val plan = planner.plan(query).stripProduceResults
        plan shouldEqual planner.subPlanBuilder()
          .projection("1 AS result")
          .filter("n:Actor")
          .nodeIndexOperator("n:Person(x)", indexType = indexType, supportPartitionedScan = false)
          .build()
      }
    }
  }

  test("should plan composite index scan with implied label") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeIndex("Person", Seq("x", "y"), existsSelectivity = 0.5, uniqueSelectivity = 0.01)
      .setAllNodesCardinality(3000)
      .build()

    val query =
      """MATCH (n:Actor)
        |WHERE n.x IS NOT NULL AND n.y IS NOT NULL
        |RETURN 1 AS result""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("1 AS result")
      .filter("n:Actor")
      .nodeIndexOperator("n:Person(x, y)")
      .build()
  }

  test("should plan composite index seek with implied label") {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 800)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeIndex("Person", Seq("x", "y"), existsSelectivity = 0.5, uniqueSelectivity = 0.01)
      .setAllNodesCardinality(3000)
      .build()

    val query =
      """MATCH (n:Actor)
        |WHERE n.x = 123 AND n.y IS NOT NULL
        |RETURN 1 AS result""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("1 AS result")
      .filter("n:Actor")
      .nodeIndexOperator("n:Person(x = 123, y)", supportPartitionedScan = false)
      .build()
  }

  test("should use index with implied label to estimate cardinality") {
    val personCount = 1000
    val petExistsSel = 0.5
    val actorCount = 800

    val planner = plannerBuilder()
      .setLabelCardinality("Person", personCount)
      .setLabelCardinality("Actor", actorCount)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeIndex("Person", Seq("pet"), existsSelectivity = petExistsSel, uniqueSelectivity = 0.01)
      .setAllNodesCardinality(3000)
      .build()

    val query = "MATCH (n:Actor) WHERE n.pet IS NOT NULL RETURN 1 AS result"

    val resultCount = actorCount * petExistsSel
    val expected = planner.planBuilder()
      .produceResults("result").withCardinality(resultCount)
      .projection("1 AS result").withCardinality(resultCount)
      .filter("n:Actor").withCardinality(resultCount)
      .nodeIndexOperator("n:Person(pet)").withCardinality(personCount * petExistsSel)

    val actual = planner.planState(query)
    actual should haveSamePlanAndCardinalitiesAsBuilder(expected)
  }

  test(
    "Label implied by end-node constraint should raise cardinality even if not used"
  ) {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(3000)
        .setLabelCardinality("Person", 1000)
        .setLabelCardinality("Actor", 1000)
        .addRelationshipEndpointLabelConstraint("R", "Person", EndpointType.START)
        .addNodeIndex("Person", Seq("firstName"), existsSelectivity = 0.4, uniqueSelectivity = 0.5)
        .addNodeIndex("Actor", Seq("firstName"), existsSelectivity = 0.1, uniqueSelectivity = 0.1)
        .setRelationshipCardinality("()-[:R]->()", 1000)
        .setRelationshipCardinality("(:Person)-[:R]->()", 1000)
        .setRelationshipCardinality("()-[:R]->(:Actor)", 1000)
        .setRelationshipCardinality("(:Person)-[:R]->(:Actor)", 1000)
        .build()

    val query = "MATCH (n {firstName: 'Bosse'})-[r:R]->(m:Actor {firstName: 'Pierre'}) Return m"
    val expected = planner.planBuilder()
      .produceResults(column("m", "cacheN[m.firstName]")).withCardinality(2)
      .filter("n.firstName = 'Bosse'").withCardinality(2) // Without end node constraint, this would be 0.5
      .expandAll("(m)<-[:R]-(n)").withCardinality(10)
      .nodeIndexOperator("m:Actor(firstName = 'Pierre')", getValue = Map("firstName" -> GetValue)).withCardinality(10)
    val actual = planner.planState(query)
    actual should haveSamePlanAndCardinalitiesAsBuilder(expected)
  }

  test(
    "should not include a label filter in a selection when it can be implied by another label filter on the same expression"
  ) {
    val planner = plannerWithNodeLabelConstraints.build()

    val query = "MATCH (n:Actor:Person:X:Y) RETURN n.name AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("n.name AS result")
      .filter("n:X") // n:X implies n:Y
      .nodeByLabelScan("n", "Actor") // n:Actor implies n:Person
      .build()
  }

  test(
    "should not include label filters in a selection when it can be implied by other label filters on the same expression"
  ) {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .setLabelCardinality("X", 800)
      .setLabelCardinality("Y", 1200)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "X", impliedLabel = "Y")
      .setAllNodesCardinality(3000)
      .enableMinimumGraphStatistics()
      .defaultRelationshipCardinalityTo0()
      .build()

    val query = "MATCH (n:Actor:Person:X:Y)--(m:X:Y) RETURN n.name AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("n.name AS result")
      .filter(
        hasLabels("n", "Actor"),
        andsReorderable(
          hasLabels("m", "X"),
          hasLabels("n", "X")
        )
      ) // m:X implies m:Y, n:X implies n:Y, n:Actor implies n:Person
      .allRelationshipsScan("(n)-[]-(m)")
      .build()
  }

  test(
    "should not include label filters in a selection when it can be implied by other label filters - multiple selections"
  ) {
    val planner = plannerWithNodeLabelConstraints.build()

    val query = "MATCH (n:Actor:Person:X:Y)-[:R]->(m:X:Y) RETURN n.name AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[n.name] AS result")
      .filter("m:X") // m:X implies m:Y
      .expandAll("(n)-[:R]->(m)")
      .cacheProperties("cacheNFromStore[n.name]")
      .filter("n:X") // n:X implies n:Y
      .nodeByLabelScan("n", "Actor") // n:Actor implies n:Person
      .build()
  }

  test(
    "should plan a label filter when it cannot be implied from another label filter on the same expression - n:X implies n:Y but NOT m:Y"
  ) {
    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .setLabelCardinality("X", 800)
      .setLabelCardinality("Y", 1200)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "X", impliedLabel = "Y")
      .setAllNodesCardinality(3000)
      .setAllRelationshipsCardinality(10)
      .enableMinimumGraphStatistics()
      .defaultRelationshipCardinalityTo0()
      .build()

    val query = "MATCH (n:Actor:Person:X:Y)--(m:Y) WHERE n.birthYear < 1800 RETURN n.name AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("n.name AS result")
      .filter(
        "n:Actor",
        "n:X",
        "m:Y",
        "n.birthYear < 1800"
      ) // n:X implies n:Y, n:Actor implies n:Person, m:Y is NOT implied
      .allRelationshipsScan("(n)-[]-(m)")
      .build()
  }

  test(
    "should plan a label filter when it cannot be implied from another label filter on the same expression - n:X implies n:Y but not m:Y - multiple selections"
  ) {
    val planner = plannerWithNodeLabelConstraints.build()

    val query = "MATCH (n:Actor:Person:X:Y)-[:R]->(m:Y) WHERE n.birthYear = m.birthYear RETURN n.name AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("n.name AS result")
      .filter(
        hasLabels("m", "Y"),
        Equals(cachedNodeProp("n", "birthYear"), prop("m", "birthYear"))(InputPosition.NONE)
      ) // n:X does NOT imply m:Y
      .expandAll("(n)-[:R]->(m)")
      .cacheProperties("cacheNFromStore[n.birthYear]")
      .filter("n:X") // n:X implies n:Y
      .nodeByLabelScan("n", "Actor") // n:Actor implies n:Person
      .build()
  }

  test(
    "should plan a label filter when it cannot be implied from another label filter on the same expression - m:X implies m:Y but not n:Y - multiple selections"
  ) {
    val planner = plannerWithNodeLabelConstraints.build()

    val query = "MATCH (n:Actor:Person:Y)-[:R]->(m:X:Y) RETURN n.name AS result"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[n.name] AS result")
      .filter("m:X") // m:X implies m:Y
      .expandAll("(n)-[:R]->(m)")
      .cacheProperties("cacheNFromStore[n.name]")
      .filter("n:Y") // m:X does NOT implies n:Y
      .nodeByLabelScan("n", "Actor") // n:Actor implies n:Person
      .build()
  }

  test("Should plan indexSeek when label can be implied by relationship type and end-node constraint - END") {
    val planner = plannerBuilder()
      .setLabelCardinality("Entity", 1500)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .addRelationshipEndpointLabelConstraint("R", "Person", EndpointType.END)
      .addNodeIndex("Person", Seq("firstName"), existsSelectivity = 0.01, uniqueSelectivity = 0.01)
      .setRelationshipCardinality("()-[:R]->(:Person)", 500)
      .setAllNodesCardinality(3000)
      .setRelationshipCardinality("()-[:R]->()", 500)
      .build()

    val query = "MATCH (n)-[r:R]->(m {firstName: 'Pierre'}) Return *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(m)<-[r:R]-(n)")
      .nodeIndexOperator("m:Person(firstName = 'Pierre')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test("Should plan indexSeek when different labels can be implied by relationships from either side") {
    // In a previous version, this only inferred m:Actor and would therefore not plan the index on :Person.
    val planner = plannerBuilder()
      .setAllNodesCardinality(3000)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .addNodeIndex("Person", Seq("firstName"), existsSelectivity = 0.01, uniqueSelectivity = 0.01)
      .setRelationshipCardinality("()-[:R]->()", 500)
      .setRelationshipCardinality("()-[:R]->(:Person)", 500)
      .setRelationshipCardinality("()-[:R]->(:Actor)", 300)
      .addRelationshipEndpointLabelConstraint("R", "Person", EndpointType.END)
      .setRelationshipCardinality("()-[:R2]->()", 200)
      .setRelationshipCardinality("()-[:R2]->(:Person)", 100)
      .setRelationshipCardinality("()-[:R2]->(:Actor)", 200)
      .addRelationshipEndpointLabelConstraint("R2", "Actor", EndpointType.END)
      .build()

    val query = "MATCH (n)-[r:R]->(m {firstName: 'Pierre'})<-[:R2]-() Return *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(m)<-[r:R]-(n)")
      .expandAll("(m)<-[:R2]-()")
      .nodeIndexOperator("m:Person(firstName = 'Pierre')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test(
    "Should NOT plan indexSeek when label can be implied by relationship type and end-node constraint but GSO is turned off - END"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(3000)
      .setLabelCardinality("Entity", 1500)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .enableGraphSchemaOptimizations(false)
      .addNodeIndex("Person", Seq("firstName"), existsSelectivity = 0.01, uniqueSelectivity = 0.01)
      .setRelationshipCardinality("()-[:R]->()", 500)
      .setRelationshipCardinality("()-[:R]->(:Person)", 500)
      .addRelationshipEndpointLabelConstraint("()-[:R]->(:Person)")
      .build()

    val query = "MATCH (n)-[r:R]->(m {firstName: 'Pierre'}) Return *"

    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .filter("m.firstName = 'Pierre'")
      // We should not attempt to use the far more selective node index here
      .relationshipTypeScan("(n)-[r:R]->(m)")
      .build()
  }

  test("Should plan indexSeek when label can be implied by relationship type and end-node constraint - START") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1_200)
      .setLabelCardinality("A", 400)
      .setRelationshipCardinality("()-[:R]->()", 600)
      .setRelationshipCardinality("(:A)-[:R]->()", 600)
      .addNodeIndex("A", List("prop"), 0.4, 0.5)
      .addRelationshipEndpointLabelConstraint("R", "A", EndpointType.START)
      .build()

    val query = "MATCH (n:A {prop: 'Pierre'})-[r:R]->(m) Return *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(n)-[r:R]->(m)")
      .nodeIndexOperator("n:A(prop = 'Pierre')", getValue = Map("prop" -> GetValue))
      .build()
  }

  test(
    "Should plan indexSeek when label can be implied by relationship type and end-node constraint - Explicit label preference"
  ) {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(3000)
        .setLabelCardinality("Person", 1000)
        .setLabelCardinality("Actor", 1000)
        .addRelationshipEndpointLabelConstraint("R", "Person", EndpointType.START)
        .addNodeIndex("Person", Seq("firstName"), existsSelectivity = 0.4, uniqueSelectivity = 0.5)
        .addNodeIndex("Actor", Seq("firstName"), existsSelectivity = 0.1, uniqueSelectivity = 0.1)
        .setRelationshipCardinality("()-[:R]->()", 1000)
        .setRelationshipCardinality("(:Person)-[:R]->()", 1000)
        .setRelationshipCardinality("()-[:R]->(:Actor)", 1000)
        .setRelationshipCardinality("(:Person)-[:R]->(:Actor)", 1000)
        .build()

    val query = "MATCH (n {firstName: 'Bosse'})-[r:R]->(m:Actor {firstName: 'Pierre'}) Return *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("n.firstName = 'Bosse'")
      .expandAll("(m)<-[r:R]-(n)")
      .nodeIndexOperator("m:Actor(firstName = 'Pierre')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test(
    "Should plan indexSeek when label can be implied by relationship type and end-node constraint - Implicit label preference"
  ) {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(3000)
        .setLabelCardinality("Person", 1000)
        .setLabelCardinality("Actor", 1000)
        .addRelationshipEndpointLabelConstraint("R", "Person", EndpointType.START)
        .addNodeIndex("Person", Seq("firstName"), existsSelectivity = 0.1, uniqueSelectivity = 0.1)
        .addNodeIndex("Actor", Seq("firstName"), existsSelectivity = 0.4, uniqueSelectivity = 0.5)
        .setRelationshipCardinality("()-[:R]->()", 1000)
        .setRelationshipCardinality("(:Person)-[:R]->()", 1000)
        .setRelationshipCardinality("()-[:R]->(:Actor)", 1000)
        .setRelationshipCardinality("(:Person)-[:R]->(:Actor)", 1000)
        .build()

    val query = "MATCH (n {firstName: 'Bosse'})-[r:R]->(m:Actor {firstName: 'Pierre'}) Return *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("m.firstName = 'Pierre'", "m:Actor")
      .expandAll("(n)-[r:R]->(m)")
      .nodeIndexOperator("n:Person(firstName = 'Bosse')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test(
    "Should plan indexSeek when label can be implied by relationship type and end-node constraint - Should not add unnecessary filter on implied label"
  ) {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(3000)
        .setLabelCardinality("Person", 1000)
        .setLabelCardinality("Actor", 500)
        .addRelationshipEndpointLabelConstraint("R", "Person", EndpointType.START)
        .addNodeIndex("Person", Seq("firstName"), existsSelectivity = 0.4, uniqueSelectivity = 0.5)
        .addNodeIndex("Actor", Seq("firstName"), existsSelectivity = 0.1, uniqueSelectivity = 0.1)
        .setRelationshipCardinality("()-[:R]->()", 1000)
        .setRelationshipCardinality("(:Person)-[:R]->()", 1000)
        .setRelationshipCardinality("(:Actor)-[:R]->()", 500)
        .build()

    val query = "MATCH (n:Actor {firstName: 'Bosse'})-[r:R]->() Return *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(n)-[r:R]->()")
      .nodeIndexOperator("n:Actor(firstName = 'Bosse')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test(
    "Should plan indexSeek when label can be implied by relationship type and end-node constraint chained with node label constraint - START"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(3000)
      .setLabelCardinality("Entity", 1500)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .addNodeLabelConstraint("Actor", "Person")
      .addNodeIndex("Person", Seq("firstName"), existsSelectivity = 0.01, uniqueSelectivity = 0.01)
      .setRelationshipCardinality("()-[:R]->()", 1000)
      .setRelationshipCardinality("(:Person)-[:R]->()", 1000)
      .setRelationshipCardinality("(:Actor)-[:R]->()", 1000)
      .addRelationshipEndpointLabelConstraint("(:Actor)-[:R]->()")
      .build()

    val query = "MATCH (n {firstName: 'Pierre'})-[r:R]->(m) Return *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(n)-[r:R]->(m)")
      .nodeIndexOperator("n:Person(firstName = 'Pierre')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test(
    "Should plan indexSeek when label can be implied by relationship type and end-node constraint chained with node label constraint - END"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(3000)
      .setLabelCardinality("Entity", 1500)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .addNodeLabelConstraint("Actor", "Person")
      .addNodeIndex("Person", Seq("firstName"), existsSelectivity = 0.01, uniqueSelectivity = 0.01)
      .setRelationshipCardinality("()-[:R]->(:Person)", 1000)
      .setRelationshipCardinality("()-[:R]->(:Actor)", 1000)
      .setRelationshipCardinality("()-[:R]->()", 1000)
      .addRelationshipEndpointLabelConstraint("()-[:R]->(:Actor)")
      .build()

    val query = "MATCH (n)-[r:R]->(m {firstName: 'Pierre'}) Return *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(m)<-[r:R]-(n)")
      .nodeIndexOperator("m:Person(firstName = 'Pierre')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test(
    "Should plan indexSeek when label can be implied by relationship type and end-node constraint chained with node label constraint - Where clause"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(3000)
      .setLabelCardinality("Entity", 1500)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .addNodeLabelConstraint("Actor", "Person")
      .addNodeIndex("Person", Seq("firstName"), existsSelectivity = 0.01, uniqueSelectivity = 0.01)
      .setRelationshipCardinality("()-[:R]->(:Person)", 1000)
      .setRelationshipCardinality("()-[:R]->(:Actor)", 1000)
      .setRelationshipCardinality("()-[:R]->()", 1000)
      .addRelationshipEndpointLabelConstraint("()-[:R]->(:Actor)")
      .build()

    val query = "MATCH (n)-[r:R]->(m) WHERE m.firstName = 'Pierre' RETURN *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .expandAll("(m)<-[r:R]-(n)")
      .nodeIndexOperator("m:Person(firstName = 'Pierre')", getValue = Map("firstName" -> GetValue))
      .build()
  }

  test(
    "Should NOT plan indexSeek when label cannot be implied by relationship type and end-node constraint due to bidirectional relationship"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(3000)
      .setLabelCardinality("Entity", 1500)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", 500)
      .addNodeLabelConstraint("Actor", "Person")
      .addNodeIndex("Person", Seq("firstName"), existsSelectivity = 0.01, uniqueSelectivity = 0.01)
      .setRelationshipCardinality("()-[:R]->()", 1000)
      .setRelationshipCardinality("()-[:R]->(:Person)", 1000)
      .setRelationshipCardinality("()-[:R]->(:Actor)", 1000)
      .setRelationshipCardinality("(:Person)-[:R]->()", 1000)
      .setRelationshipCardinality("(:Actor)-[:R]->()", 1000)
      .addRelationshipEndpointLabelConstraint("()-[:R]->(:Actor)")
      .build()

    val query = "MATCH (n)-[r:R]-(m) WHERE m.firstName = 'Pierre' RETURN *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("m.firstName = 'Pierre'")
      .relationshipTypeScan("(n)-[r:R]-(m)")
      .build()
  }

  test("should be able to avoid label filter on undirected relationship if constraint on start and end") {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:KNOWS]->(:Person)")
      .addRelationshipIndex("KNOWS", Seq("since"), existsSelectivity = 0.5, uniqueSelectivity = 0.1)
      .build()

    val plan = planner.plan(
      """MATCH (n:Person)-[r:KNOWS]-(m:Person)
        |WHERE r.since > 2000
        |RETURN n, m
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n", "m")
        // No need to filter on labels since the relationship constraint implies both start and end node labels
        .relationshipIndexOperator("(n)-[:KNOWS(since > 2000)]-(m)")
        .build()
    )
  }

  test("should plan label filters on undirected relationship if constraint on only end node") {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("()-[:KNOWS]->(:Person)")
      .addRelationshipIndex("KNOWS", Seq("since"), existsSelectivity = 0.5, uniqueSelectivity = 0.1)
      .build()

    val plan = planner.plan(
      """MATCH (n:Person)-[r:KNOWS]-(m:Person)
        |WHERE r.since > 2000
        |RETURN n, m
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n", "m")
        // we do not know which way the returned relationship goes, so we need to filter on both nodes
        .filter(andsReorderable("n:Person", "m:Person"))
        .relationshipIndexOperator("(n)-[:KNOWS(since > 2000)]-(m)")
        .build()
    )
  }

  test("plan RelationshipCountFromCountStore when possible by end-node constraints") {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:WORKS_FOR]->(:Company)")
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 80)
      .setLabelCardinality("Company", 5)
      .setRelationshipCardinality("()-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("()-[:WORKS_FOR]->(:Person)", 0)
      .setRelationshipCardinality("()-[:WORKS_FOR]->(:Company)", 100)
      .setRelationshipCardinality("(:Company)-[:WORKS_FOR]->()", 0)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->(:Company)", 100)
      .setRelationshipCardinality("(:Company)-[:WORKS_FOR]->(:Person)", 0)
      .build()

    val planPWFC_right = planner.plan(
      """MATCH (:Person)-[:WORKS_FOR]->(:Company)
        |RETURN COUNT(*)
        |""".stripMargin
    )
    planPWFC_right should equal(
      planner.planBuilder()
        .produceResults("`COUNT(*)`")
        .relationshipCountFromCountStore("COUNT(*)", None, Seq("WORKS_FOR"), None)
        .build()
    )

    val planPWFC_left = planner.plan(
      """MATCH (:Company)<-[:WORKS_FOR]-(:Person)
        |RETURN COUNT(*)
        |""".stripMargin
    )
    planPWFC_left should equal(
      planner.planBuilder()
        .produceResults("`COUNT(*)`")
        .relationshipCountFromCountStore("COUNT(*)", None, Seq("WORKS_FOR"), None)
        .build()
    )

    // Cannot plan a relationshipCountFromCountStore, because Company is not implied on the source node for WORKS_FOR
    val planCWFP_right = planner.plan(
      """MATCH (:Company)-[:WORKS_FOR]->(:Person)
        |RETURN COUNT(*)
        |""".stripMargin
    )
    planCWFP_right should equal(
      planner.planBuilder()
        .produceResults("`COUNT(*)`")
        .aggregation(Seq(), Seq("count(*) AS `COUNT(*)`"))
        .filter("anon_1:Person")
        .expandAll("(anon_0)-[:WORKS_FOR]->(anon_1)")
        .nodeByLabelScan("anon_0", "Company")
        .build()
    )

    // Cannot plan a relationshipCountFromCountStore, because Company is not implied on the source node for WORKS_FOR
    val planCWFP_left = planner.plan(
      """MATCH (:Person)<-[:WORKS_FOR]-(:Company)
        |RETURN COUNT(*)
        |""".stripMargin
    )
    planCWFP_left should equal(
      planner.planBuilder()
        .produceResults("`COUNT(*)`")
        .aggregation(Seq(), Seq("count(*) AS `COUNT(*)`"))
        .filter("anon_0:Person")
        .expandAll("(anon_1)-[:WORKS_FOR]->(anon_0)")
        .nodeByLabelScan("anon_1", "Company")
        .build()
    )
  }

  test("plan RelationshipCountFromCountStore in COUNT-subquery when possible by end-node constraints") {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:WORKS_FOR]->(:Company)")
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 80)
      .setLabelCardinality("Company", 5)
      .setRelationshipCardinality("()-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("()-[:WORKS_FOR]->(:Company)", 100)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->(:Company)", 100)
      .build()

    val plan = planner.plan(
      """MATCH (p:Person)
        |RETURN p, COUNT {(:Person)-[:WORKS_FOR]->(:Company) } AS totalWorksFor
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("p", "totalWorksFor")
        .apply()
        .|.relationshipCountFromCountStore("totalWorksFor", None, Seq("WORKS_FOR"), None)
        .nodeByLabelScan("p", "Person")
        .build()
    )
  }

  test("plan RelationshipCountFromCountStore when possible by end-node constraints - disjunctive types") {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:WORKS_FOR]->(:Company)")
      .addRelationshipEndpointLabelConstraint("(:Person)-[:WORKED_FOR]->(:Company)")
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 80)
      .setLabelCardinality("Company", 5)
      .setRelationshipCardinality("()-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("()-[:WORKED_FOR]->()", 250)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("(:Person)-[:WORKED_FOR]->()", 250)
      .setRelationshipCardinality("()-[:WORKS_FOR]->(:Company)", 100)
      .setRelationshipCardinality("()-[:WORKED_FOR]->(:Company)", 250)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->(:Company)", 100)
      .setRelationshipCardinality("(:Person)-[:WORKED_FOR]->(:Company)", 250)
      .build()

    val plan = planner.plan(
      """MATCH (:Person)-[:WORKS_FOR | :WORKED_FOR]->(:Company)
        |RETURN COUNT(*)
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("`COUNT(*)`")
        .relationshipCountFromCountStore("COUNT(*)", None, Seq("WORKS_FOR", "WORKED_FOR"), None)
        .build()
    )
  }

  test("should not plan RelationshipCountFromCountStore when one of the types does not imply the labels") {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:WORKS_FOR]->(:Company)")
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 80)
      .setLabelCardinality("Company", 5)
      .setRelationshipCardinality("()-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("()-[:WORKED_FOR]->()", 250)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("(:Person)-[:WORKED_FOR]->()", 250)
      .setRelationshipCardinality("()-[:WORKS_FOR]->(:Company)", 100)
      .setRelationshipCardinality("()-[:WORKED_FOR]->(:Company)", 250)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->(:Company)", 100)
      .setRelationshipCardinality("(:Person)-[:WORKED_FOR]->(:Company)", 250)
      .build()

    val plan = planner.plan(
      """MATCH (:Person)-[:WORKS_FOR | :WORKED_FOR]->(:Company)
        |RETURN COUNT(*)
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("`COUNT(*)`")
        .aggregation(Seq(), Seq("count(*) AS `COUNT(*)`"))
        .filter("anon_0:Person")
        .expandAll("(anon_1)<-[:WORKS_FOR|WORKED_FOR]-(anon_0)")
        .nodeByLabelScan("anon_1", "Company")
        .build()
    )
  }

  test(
    "should plan RelationshipCountFromCountStore (type, destLabel) when the destination label cannot be implied by all types"
  ) {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:WORKS_FOR]->(:Company)")
      .addRelationshipEndpointLabelConstraint("(:Person)-[:WORKED_FOR]->()")
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 80)
      .setLabelCardinality("Company", 5)
      .setRelationshipCardinality("()-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("()-[:WORKED_FOR]->()", 250)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("(:Person)-[:WORKED_FOR]->()", 250)
      .setRelationshipCardinality("()-[:WORKS_FOR]->(:Company)", 100)
      .setRelationshipCardinality("()-[:WORKED_FOR]->(:Company)", 250)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->(:Company)", 100)
      .setRelationshipCardinality("(:Person)-[:WORKED_FOR]->(:Company)", 250)
      .build()

    val plan = planner.plan(
      """MATCH (:Person)-[:WORKS_FOR | :WORKED_FOR]->(:Company)
        |RETURN COUNT(*)
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("`COUNT(*)`")
        .relationshipCountFromCountStore("COUNT(*)", None, Seq("WORKS_FOR", "WORKED_FOR"), Some("Company"))
        .build()
    )
  }

  test(
    "should plan RelationshipCountFromCountStore (srcLabel, type) when the source label is not implied by all types"
  ) {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:WORKS_FOR]->(:Company)")
      .addRelationshipEndpointLabelConstraint("()-[:WORKED_FOR]->(:Company)")
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 80)
      .setLabelCardinality("Company", 5)
      .setRelationshipCardinality("()-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("()-[:WORKED_FOR]->()", 250)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->()", 100)
      .setRelationshipCardinality("(:Person)-[:WORKED_FOR]->()", 250)
      .setRelationshipCardinality("()-[:WORKS_FOR]->(:Company)", 100)
      .setRelationshipCardinality("()-[:WORKED_FOR]->(:Company)", 250)
      .setRelationshipCardinality("(:Person)-[:WORKS_FOR]->(:Company)", 100)
      .setRelationshipCardinality("(:Person)-[:WORKED_FOR]->(:Company)", 250)
      .build()

    val plan = planner.plan(
      """MATCH (:Person)-[:WORKS_FOR | :WORKED_FOR]->(:Company)
        |RETURN COUNT(*)
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("`COUNT(*)`")
        .relationshipCountFromCountStore("COUNT(*)", Some("Person"), Seq("WORKS_FOR", "WORKED_FOR"), None)
        .build()
    )
  }

  test(
    "should not plan RelationshipCountFromCountStore for undirected relationship even if labels are implied by end-point constraints"
  ) {
    val planner = undirectedRelationshipPlannerBase
      .addRelationshipEndpointLabelConstraint("(:Person)-[:LIKES]->(:Person)")
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 80)
      .setRelationshipCardinality("()-[:LIKES]->()", 100)
      .setRelationshipCardinality("(:Person)-[:LIKES]->()", 100)
      .setRelationshipCardinality("()-[:LIKES]->(:Person)", 100)
      .setRelationshipCardinality("(:Person)-[:LIKES]->(:Person)", 100)
      .build()

    val plan = planner.plan(
      """MATCH (:Person)-[:LIKES]-(:Person)
        |RETURN COUNT(*)
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("`COUNT(*)`")
        .aggregation(Seq(), Seq("count(*) AS `COUNT(*)`"))
        .expandAll("(anon_0)-[:LIKES]-()")
        .nodeByLabelScan("anon_0", "Person")
        .build()
    )
  }

  test(
    "should plan nodeCountFromCountStore for COUNT(*) over a single node with multiple labels when all labels are implied by a single label"
  ) {
    val planner = undirectedRelationshipPlannerBase
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 80)
      .setLabelCardinality("Actor", 5)
      .build()

    val plan = planner.plan(
      """MATCH (:Person&Actor)
        |RETURN COUNT(*)
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("`COUNT(*)`")
        .nodeCountFromCountStore("COUNT(*)", Seq(Some("Actor")))
        .build()
    )
  }

  test(
    "should plan nodeCountFromCountStore for COUNT on props implied by an implied label"
  ) {
    val planner = undirectedRelationshipPlannerBase
      .addNodeLabelConstraint(constrainedLabel = "Developer", impliedLabel = "Person")
      .addNodeExistenceConstraint(label = "Person", property = "name")
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 80)
      .setLabelCardinality("Developer", 5)
      .build()

    val plan = planner.plan(
      """MATCH (m:Person&Developer)
        |WHERE m.name IS NOT NULL
        |RETURN COUNT(m.name)
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("`COUNT(m.name)`")
        .nodeCountFromCountStore("COUNT(m.name)", Seq(Some("Developer")))
        .build()
    )
  }

  test(
    "should plan nodeCountFromCountStore for COUNT on props when labels and properties are implied"
  ) {
    val planner = undirectedRelationshipPlannerBase
      .addNodeLabelConstraint(constrainedLabel = "Developer", impliedLabel = "Person")
      .addNodeExistenceConstraint(label = "Person", property = "name")
      .addNodeExistenceConstraint(label = "Developer", property = "repositoryId")
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 80)
      .setLabelCardinality("Developer", 5)
      .build()

    val plan = planner.plan(
      """MATCH (m:Person&Developer)
        |WHERE m.repositoryId IS NOT NULL
        |RETURN COUNT(m.repositoryId)
        |""".stripMargin
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("`COUNT(m.repositoryId)`")
        .nodeCountFromCountStore("COUNT(m.repositoryId)", Seq(Some("Developer")))
        .build()
    )
  }

  // Cardinality estimation tests

  test("should not apply the selectivity of an implied label to estimate cardinality of a label scan") {
    val actors = 500

    val planner = plannerBuilder()
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", actors)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .setAllNodesCardinality(3000)
      .build()

    val query = "MATCH (n:Actor:Person) RETURN n"

    val actual = planner.planState(query)
    val expected = planner.planBuilder()
      .produceResults("n").withCardinality(actors)
      .nodeByLabelScan("n", "Actor").withCardinality(actors)

    actual should haveSamePlanAndCardinalitiesAsBuilder(expected)
  }

  test("should not apply the selectivity of implied labels to estimate cardinality of an intersection scan") {
    val actors = 500
    val others = 1500
    val total = 3000

    val planner = plannerBuilder()
      .setLabelCardinality("Entity", 1500)
      .setLabelCardinality("Person", 1000)
      .setLabelCardinality("Actor", actors)
      .setLabelCardinality("Other", others)
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Person")
      .addNodeLabelConstraint(constrainedLabel = "Actor", impliedLabel = "Entity")
      .setAllNodesCardinality(total)
      .build()

    val query = "MATCH (n:Actor:Person:Entity:Other) RETURN n"

    val cardinality = actors * (others.toDouble / total)

    val actual = planner.planState(query)
    val expected = planner.planBuilder()
      .produceResults("n").withCardinality(cardinality)
      .intersectionNodeByLabelsScan("n", Seq("Actor", "Other")).withCardinality(cardinality)

    actual should haveSamePlanAndCardinalitiesAsBuilder(expected)
  }

  test("should estimate cardinality of a label scan using implied label") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 10)
      .setAllRelationshipsCardinality(200)
      .setRelationshipCardinality("()-[:R]->()", 200)
      .setRelationshipCardinality("(:A)-[:R]->()", 200)
      .addRelationshipEndpointLabelConstraint("(:A)-[:R]->()")
      .build()

    val query =
      """
        |MATCH (a)-[r:R]->(b)
        |RETURN a
        |""".stripMargin

    val actual = planner.planState(query)
    val expected = planner.subPlanBuilder()
      .produceResults("a").withCardinality(200)
      .expandAll("(a)-[:R]->()").withCardinality(200)
      .nodeByLabelScan("a", "A").withCardinality(10)

    actual should haveSamePlanAndCardinalitiesAsBuilder(expected)
  }

  test("Prune implied label from node label constraints during cardinality estimation of relationship patterns") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("B", 1500)
      .setLabelCardinality("C", 90)
      .addNodeLabelConstraint(constrainedLabel = "A", impliedLabel = "B")
      .setAllNodesCardinality(10000)
      .setRelationshipCardinality("()-[:R]->()", 250)
      .setRelationshipCardinality("(:C)-[:R]->()", 220)
      .setRelationshipCardinality("()-[:R]->(:A)", 180)
      .setRelationshipCardinality("()-[:R]->(:B)", 190)
      .setRelationshipCardinality("(:C)-[:R]->(:A)", 180)
      .setRelationshipCardinality("(:C)-[:R]->(:B)", 190)
      .build()

    val query = "MATCH (:C)-[r:R]->(:A&B) RETURN r"

    // When we do not know that A implies B, we assume independence:
    // |(:C)-[:R]->(A)| * |(B)|/|(n)| = 180 * 1500/10000 = 27
    // |(:C)-[:R]->(B)| * |(A)|/|(n)| = 190 * 1000/10000 = 19
    //                  ^ independence assumption

    // Without realizing that B is implied by A, we would incorrectly apply the independence assumption.
    // Which would lead to underestimating the cardinality.

    planner.planState(query) should haveSamePlanAndCardinalitiesAsBuilder(
      planner.planBuilder()
        // Without pruning the implied label B, this would get the cardinality 19.
        .produceResults("r").withCardinality(180)
        .filter("anon_1:A").withCardinality(180)
        .expandAll("(anon_0)-[r:R]->(anon_1)").withCardinality(220)
        .nodeByLabelScan("anon_0", "C").withCardinality(90)
    )

    val queryWithoutImpliedB = "MATCH (:C)-[r:R]->(:A) RETURN r"
    planner.planState(queryWithoutImpliedB) should haveSamePlanAndCardinalitiesAsBuilder(
      planner.planBuilder()
        // Without pruning the implied label B, this would get the cardinality 19.
        .produceResults("r").withCardinality(180)
        .filter("anon_1:A").withCardinality(180)
        .expandAll("(anon_0)-[r:R]->(anon_1)").withCardinality(220)
        .nodeByLabelScan("anon_0", "C").withCardinality(90)
    )
  }

  // Cardinality estimation tests end
}
