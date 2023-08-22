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
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.collection.immutable.ListSet

class CreateNodePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  test("should plan single create") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(createNode("a"))
      .argument()
      .build()
  }

  test("should plan for multiple creates") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a), (b), (c)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        createNode("a"),
        createNode("b"),
        createNode("c")
      )
      .argument()
      .build()
  }

  test("should plan for multiple creates via multiple statements") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a) CREATE (b) CREATE (c)").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(
        createNode("a"),
        createNode("b"),
        createNode("c")
      )
      .argument()
      .build()
  }

  test("should plan multiple creates via multiple operators if they have pattern-dependencies via IR expressions") {
    val planner = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = planner.plan(
      """CREATE (m)
        |CREATE (n {count: COUNT { MATCH () } })""".stripMargin
    )

    plan shouldEqual planner.planBuilder()
      .produceResults()
      .emptyResult()
      .create(createNodeWithProperties("n", Seq(), "{count: anon_1}"))
      .apply()
      .|.nodeCountFromCountStore("anon_1", Seq(None))
      .eager(ListSet(EagernessReason.Unknown))
      .create(createNode("m"))
      .argument()
      .build()
  }

  test("should plan multiple creates via multiple operators if they have identifier-dependencies via IR expressions") {
    val planner = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = planner.plan(
      """CREATE (a)-[r:T]->(b)
        |CREATE (c {p: COUNT { WITH r ORDER BY r } })""".stripMargin
    )

    plan shouldEqual planner.planBuilder()
      .produceResults()
      .emptyResult()
      .create(createNodeWithProperties("c", Seq(), "{p: anon_0}"))
      .apply()
      .|.aggregation(Seq(), Seq("count(*) AS anon_0"))
      .|.sort("r ASC")
      .|.argument("r")
      .create(createNode("a"), createNode("b"), createRelationship("r", "a", "T", "b"))
      .argument()
      .build()
  }

  test("should plan multiple creates via multiple operators if they have dependencies via exists expressions") {
    val planner = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = planner.plan(
      """CREATE (n)-[r:REL]->(m)
        |CREATE (o {p: exists( ()--() ) });""".stripMargin
    )

    plan shouldEqual planner.planBuilder()
      .produceResults()
      .emptyResult()
      .create(createNodeWithProperties("o", Seq(), "{p: anon_3}"))
      .letSemiApply("anon_3")
      .|.allRelationshipsScan("(anon_0)-[anon_1]-(anon_2)")
      .eager(ListSet(EagernessReason.Unknown))
      .create(createNode("n"), createNode("m"), createRelationship("r", "n", "REL", "m"))
      .argument()
      .build()
  }

  test("should plan single create with return") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a) return a").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .create(createNode("a"))
      .argument()
      .build()
  }

  test("should plan create with labels") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a:A:B)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(createNode("a", "A", "B"))
      .argument()
      .build()
  }

  test("should plan create with properties") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("CREATE (a {prop: 42})").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(createNodeWithProperties("a", Seq.empty, "{prop: 42}"))
      .argument()
      .build()
  }

  test("should plan match and create") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("MATCH (a) CREATE (b)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(createNode("b"))
      .allNodeScan("a")
      .build()
  }

  test("should plan create in tail") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("MATCH (a) CREATE (b) WITH * MATCH (c) CREATE (d)").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .emptyResult()
      .create(createNode("d"))
      .eager()
      .apply()
      .|.allNodeScan("c", "a", "b")
      .eager()
      .create(createNode("b"))
      .allNodeScan("a")
      .build()
  }

  test("when inside a FOREACH, a create with subquery should be planned with a foreach apply") {
    val cfg = plannerBuilder().setAllNodesCardinality(0).build()
    val plan = cfg.plan("FOREACH (i IN [1, 2] | CREATE (a {prop: EXISTS { MATCH () }}))").stripProduceResults

    val plannedCreateWithForeachApply = plan.folder.treeExists {
      case _: ForeachApply => true
    }

    withClue(plan) {
      plannedCreateWithForeachApply should be(true)
    }
  }
}
