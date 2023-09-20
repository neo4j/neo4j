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
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.setNodeProperty
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class CreateNodePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport with AstConstructionTestSupport {

  test("should plan single create") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(CreateNode("a", Seq.empty, None))
      .argument()
      .build()
  }

  test("should plan for multiple creates") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a), (b), (c)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        CreateNode("a", Seq.empty, None),
        CreateNode("b", Seq.empty, None),
        CreateNode("c", Seq.empty, None))
      .argument()
      .build()
  }

  test("should plan for multiple creates via multiple statements") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a) CREATE (b) CREATE (c)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        CreateNode("a", Seq.empty, None),
        CreateNode("b", Seq.empty, None),
        CreateNode("c", Seq.empty, None))
      .argument()
      .build()
  }

  test("should plan multiple creates via multiple operators if they have dependencies via exists expressions") {
    val planner = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = planner.plan(
      """CREATE (n)-[r:REL]->(m)
        |CREATE (o {p: exists( ()--() ) })""".stripMargin
    )

    val embeddedPlan = planner.subPlanBuilder()
      .expand("(anon_2)-[anon_3]-(anon_4)")
      .allNodeScan("anon_2")
      .build()

    val embeddedPlanExpression = NestedPlanExistsExpression(
      embeddedPlan,
      "exists((`anon_2`)-[`anon_3`]-(`anon_4`))"
    )(pos)

    val mapExpression = MapExpression(Seq(propName("p") -> embeddedPlanExpression))(pos)
    val secondCreateNode = CreateNode("o", Seq.empty, Some(mapExpression))

    plan shouldEqual planner.planBuilder()
      .produceResults()
      .emptyResult()
      .create(secondCreateNode)
      .create(Seq(createNode("n"), createNode("m")), Seq(createRelationship("r", "n", "REL", "m")))
      .argument()
      .build()
  }

  test("should plan single create with return") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a) return a").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .create(CreateNode("a", Seq.empty, None))
      .argument()
      .build()
  }

  test("should plan create with labels") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a:A:B)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(CreateNode("a", Seq(labelName("A"), labelName("B")), None))
      .argument()
      .build()
  }

  test("should plan create with properties") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a {prop: 42})").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(CreateNode("a", Seq.empty, Some(mapOfInt(("prop", 42)))))
      .argument()
      .build()
  }

  test("should plan match and create") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("MATCH (a) CREATE (b)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(CreateNode("b", Seq.empty, None))
      .allNodeScan("a")
      .build()
  }

  test("should plan create in tail") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("MATCH (a) CREATE (b) WITH * MATCH (c) CREATE (d)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(CreateNode("d", Seq.empty, None))
      .eager()
      .apply()
      .|.allNodeScan("c", "a", "b")
      .eager()
      .create(CreateNode("b", Seq.empty, None))
      .allNodeScan("a")
      .build()
  }

  test("should use correct arguments on RHS of ForeachApply so that Create has access to that variable") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(40)
      .setLabelCardinality("BenchmarkTool", 100)
      .setLabelCardinality("BenchmarkToolVersion", 100)
      .setLabelCardinality("Project", 100)
      .setRelationshipCardinality("()-[:VERSION_OF]->()", 40)
      .setRelationshipCardinality("(:BenchmarkToolVersion)-[:VERSION_OF]->()", 40)
      .build()

    val query =
      """
        |MERGE (benchmark_tool:BenchmarkTool {name: $tool_name})
        |  ON CREATE SET benchmark_tool.repository_name=$tool_repository_name
        |MERGE (benchmark_tool_version:BenchmarkToolVersion)-[:VERSION_OF]->(benchmark_tool)
        |CREATE
        |    (test_run:TestRun $test_run),
        |    (benchmark_config:BenchmarkConfig $benchmark_config),
        |    (neo4j_config:Neo4jConfig $neo4j_config),
        |    (test_run)-[:HAS_BENCHMARK_CONFIG]->(benchmark_config),
        |    (test_run)-[:HAS_CONFIG]->(neo4j_config),
        |    (test_run)-[:WITH_TOOL]->(benchmark_tool_version)
        |FOREACH (project IN $projects |
        |    MERGE (p:Project)
        |    CREATE (p)<-[:WITH_PROJECT]-(test_run)
        |)
        |""".stripMargin
    val plan = planner.plan(query).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .foreachApply("project", "$projects")
      .|.create(Seq(), Seq(createRelationship("anon_4", "p", "WITH_PROJECT", "test_run", INCOMING)))
      .|.apply()
      .|.|.merge(Seq(createNode("p", "Project")), Seq(), Seq(), Seq(), Set())
      .|.|.nodeByLabelScan("p", "Project")
      .|.argument(
        "anon_1",
        "benchmark_tool_version",
        "benchmark_config",
        "anon_2",
        "test_run",
        "neo4j_config",
        "anon_0",
        "benchmark_tool",
        "project",
        "anon_3"
      )
      .create(Seq(createNodeWithProperties("test_run", Seq("TestRun"), "$test_run"), createNodeWithProperties("benchmark_config", Seq("BenchmarkConfig"), "$benchmark_config"), createNodeWithProperties("neo4j_config", Seq("Neo4jConfig"), "$neo4j_config")), Seq(createRelationship("anon_1", "test_run", "HAS_BENCHMARK_CONFIG", "benchmark_config", OUTGOING), createRelationship("anon_2", "test_run", "HAS_CONFIG", "neo4j_config", OUTGOING), createRelationship("anon_3", "test_run", "WITH_TOOL", "benchmark_tool_version", OUTGOING)))
      .apply()
      .|.merge(
        Seq(createNode("benchmark_tool_version", "BenchmarkToolVersion")),
        Seq(createRelationship("anon_0", "benchmark_tool_version", "VERSION_OF", "benchmark_tool", OUTGOING)),
        lockNodes = Set("benchmark_tool")
      )
      .|.filter("benchmark_tool_version:BenchmarkToolVersion")
      .|.expandAll("(benchmark_tool)<-[anon_0:VERSION_OF]-(benchmark_tool_version)")
      .|.argument("benchmark_tool")
      .merge(
        Seq(createNodeWithProperties("benchmark_tool", Seq("BenchmarkTool"), "{name: $tool_name}")),
        onCreate = Seq(setNodeProperty("benchmark_tool", "repository_name", "$tool_repository_name"))
      )
      .filter("benchmark_tool.name = $tool_name")
      .nodeByLabelScan("benchmark_tool", "BenchmarkTool")
      .build()
  }
}
