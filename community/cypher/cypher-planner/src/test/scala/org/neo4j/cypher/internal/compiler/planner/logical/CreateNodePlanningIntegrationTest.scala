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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.pos
import org.neo4j.cypher.internal.logical.builder.Parser
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
}
