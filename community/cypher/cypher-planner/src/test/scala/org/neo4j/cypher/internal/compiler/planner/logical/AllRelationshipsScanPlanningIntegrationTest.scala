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
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.UndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class AllRelationshipsScanPlanningIntegrationTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with BeLikeMatcher {

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .setAllRelationshipsCardinality(100)

  test("should plan undirected all relationships scan") {
    val planner = plannerBuilder().build()
    planner.plan("MATCH (a)-[r]-(b) RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .allRelationshipsScan("(a)-[r]-(b)")
        .build()
    )
  }

  test("should plan directed outgoing all relationships scan") {
    val planner = plannerBuilder().build()
    planner.plan("MATCH (a)-[r]->(b) RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .allRelationshipsScan("(a)-[r]->(b)")
        .build()
    )
  }

  test("should plan directed incoming all relationships scan") {
    val planner = plannerBuilder().build()
    planner.plan("MATCH (a)<-[r]-(b) RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .allRelationshipsScan("(a)<-[r]-(b)")
        .build()
    )
  }

  test("should plan undirected all relationships on the RHS of an Apply with correct arguments") {
    val planner = plannerBuilder().build()

    planner.plan(
      """MATCH (n)
        |CALL {
        |  WITH n
        |  MATCH (a)-[r]-(b)
        |  RETURN r
        |}
        |RETURN n, r
        |""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("n", "r")
        .apply()
        .|.allRelationshipsScan("(a)-[r]-(b)", "n")
        .allNodeScan("n")
        .build()
    )
  }

  test("should plan directed outgoing all relationships on the RHS of an Apply with correct arguments") {
    val planner = plannerBuilder().build()

    planner.plan(
      """MATCH (n)
        |CALL {
        |  WITH n
        |  MATCH (a)-[r]->(b)
        |  RETURN r
        |}
        |RETURN n, r
        |""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("n", "r")
        .apply()
        .|.allRelationshipsScan("(a)-[r]->(b)", "n")
        .allNodeScan("n")
        .build()
    )
  }

  test("should plan directed incoming all relationships on the RHS of an Apply with correct arguments") {
    val planner = plannerBuilder().build()

    planner.plan(
      """MATCH (n)
        |CALL {
        |  WITH n
        |  MATCH (a)<-[r]-(b)
        |  RETURN r
        |}
        |RETURN n, r
        |""".stripMargin
    ) should equal(
      planner.planBuilder()
        .produceResults("n", "r")
        .apply()
        .|.allRelationshipsScan("(a)<-[r]-(b)", "n")
        .allNodeScan("n")
        .build()
    )
  }

  test("should not plan all relationships scan for already bound relationship variable") {
    val planner = plannerBuilder().build()
    withClue("Did not expect an UndirectedAllRelationshipsScan to be planned") {
      planner.plan(
        """MATCH (a)-[r:REL]-(b) WITH r SKIP 0
          |MATCH (a2)-[r]-(b2) RETURN r""".stripMargin
      ).leaves.folder.treeExists {
        case _: UndirectedAllRelationshipsScan => true
      } should be(false)
    }
  }

  test("should not prefer all relationships scan over label scan if label scan is selective") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 10)
      .setRelationshipCardinality("(:A)-[]-()", 1)
      .build()
    planner.plan("MATCH (a:A)-[r]-() RETURN r").leaves should beLike {
      case Seq(_: NodeByLabelScan) => ()
    }
  }

  test("should prefer all relationships scan over label scan if label scan is not selective") {
    val planner = plannerBuilder()
      .setAllRelationshipsCardinality(10)
      .setLabelCardinality("A", 100)
      .setRelationshipCardinality("()-[:REL]->()", 10)
      .setRelationshipCardinality("(:A)-[]-()", 1)
      .build()
    planner.plan("MATCH (a:A)-[r]-() RETURN r").leaves should beLike {
      case Seq(_: UndirectedAllRelationshipsScan) => ()
    }
  }

  test("should plan all relationships scan for self-loops") {
    val planner = plannerBuilder().build()

    planner.plan(s"MATCH (a)-[r]-(a) RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .filter("a = anon_0")
        .allRelationshipsScan("(a)-[r]-(anon_0)")
        .build()
    )
  }
}
