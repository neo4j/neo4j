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
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RelationshipIndexSeekPlanningIntegrationTest extends CypherFunSuite
                                                   with LogicalPlanningIntegrationTestSupport
                                                   with AstConstructionTestSupport {

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .enablePlanningRelationshipIndexes()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .setRelationshipCardinality("()-[:REL2]-()", 50)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01)

  for ((pred, indexStr) <- Seq(
    "r.prop = 123"     -> "prop = 123",
    "r.prop > 123"     -> "prop > 123",
    "r.prop < 123"     -> "prop < 123",
    "r.prop IN [1, 2]" -> "prop = 1 OR 2",
  )) {

    test(s"should plan undirected relationship index seek for $pred") {
      val planner = plannerBuilder().build()

      planner.plan(s"MATCH (a)-[r:REL]-(b) WHERE $pred RETURN r") should equal(
        planner.planBuilder()
          .produceResults("r")
          .relationshipIndexOperator(s"(a)-[r:REL($indexStr)]-(b)")
          .build()
      )
    }

    test(s"should plan directed OUTGOING relationship index seek for $pred") {
      val planner = plannerBuilder().build()

      planner.plan(s"MATCH (a)-[r:REL]->(b) WHERE $pred RETURN r") should equal(
        planner.planBuilder()
          .produceResults("r")
          .relationshipIndexOperator(s"(a)-[r:REL($indexStr)]->(b)")
          .build()
      )
    }

    test(s"should plan directed INCOMING relationship index seek for $pred") {
      val planner = plannerBuilder().build()

      planner.plan(s"MATCH (a)<-[r:REL]-(b) WHERE $pred RETURN r") should equal(
        planner.planBuilder()
          .produceResults("r")
          .relationshipIndexOperator(s"(a)<-[r:REL($indexStr)]-(b)")
          .build()
      )
    }

    test(s"should plan undirected relationship index seek on the RHS of an Apply with correct arguments for $pred") {
      val planner = plannerBuilder().build()

      planner.plan(
        s"""
          |MATCH (n)
          |CALL {
          |  WITH n
          |  MATCH (a)-[r:REL]-(b)
          |  WHERE $pred AND b.prop = n.prop
          |  RETURN r
          |}
          |RETURN n, r
          |""".stripMargin
      ) should equal(
        planner.planBuilder()
          .produceResults("n", "r")
          .filter("b.prop = n.prop")
          .apply(fromSubquery = true)
          .|.relationshipIndexOperator(s"(a)-[r:REL($indexStr)]-(b)", argumentIds = Set("n"))
          .allNodeScan("n")
          .build()
      )
    }

    test(s"should plan undirected relationship index seek over NodeByLabelScan using a hint for $pred") {
      val planner = plannerBuilder()
        .setLabelCardinality("A", 10)
        .setRelationshipCardinality("(:A)-[:REL]-()", 10)
        .build()

      planner.plan(s"MATCH (a:A)-[r:REL]-(b) USING INDEX r:REL(prop) WHERE $pred RETURN r") should equal(
        planner.planBuilder()
          .produceResults("r")
          .filterExpression(hasLabels("a", "A")) // TODO change to .filter("a:A") after https://github.com/neo-technology/neo4j/pull/9823 is merged
          .relationshipIndexOperator(s"(a)-[r:REL($indexStr)]-(b)")
          .build()
      )
    }

    test(s"should not plan relationship index seek when not enabled for $pred") {
      val planner = plannerBuilder()
        .enablePlanningRelationshipIndexes(false)
        .build()

      withClue("Used relationship index even when not enabled:") {
        planner.plan(s"MATCH (a)-[r:REL]-(b) WHERE $pred RETURN r").treeExists {
          case _: UndirectedRelationshipIndexSeek => true
          case _: DirectedRelationshipIndexSeek => true
        } should be(false)
      }
    }

    test(s"should not (yet) plan relationship index seek with filter for already bound start node for $pred") {
      val planner = plannerBuilder().build()
      planner.plan(
        s"""MATCH (a) WITH a SKIP 0
          |MATCH (a)-[r:REL]-(b) WHERE $pred RETURN r""".stripMargin) should equal(
        planner.planBuilder()
          .produceResults("r")
          .filter(pred)
          .expandAll("(a)-[r:REL]-(b)")
          .skip(0)
          .allNodeScan("a")
          .build()
      )
    }

    test(s"should not (yet) plan relationship index seek with filter for already bound end node for $pred") {
      val planner = plannerBuilder().build()
      planner.plan(
        s"""MATCH (b) WITH b SKIP 0
          |MATCH (a)-[r:REL]-(b) WHERE $pred RETURN r""".stripMargin) should equal(
        planner.planBuilder()
          .produceResults("r")
          .filter(pred)
          .expandAll("(b)-[r:REL]-(a)")
          .skip(0)
          .allNodeScan("b")
          .build()
      )
    }

    test(s"should not plan relationship index seek for already bound relationship variable for $pred") {
      val planner = plannerBuilder().build()
      withClue("Did not expect an UndirectedRelationshipIndexSeek to be planned") {
        planner.plan(
          s"""MATCH (a)-[r:REL]-(b) WITH r SKIP 0
            |MATCH (a2)-[r:REL]-(b2) WHERE $pred RETURN r""".stripMargin).leaves.treeExists {
          case _: UndirectedRelationshipIndexSeek => true
        } should be(false)
      }
    }
  }
}
