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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

class RelationshipIndexSeekPlanningIntegrationTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .setRelationshipCardinality("()-[:REL2]-()", 50)
      .addRelationshipIndex("REL", Seq("prop"), 0.01, 0.001)

  private def plannerBuilderWithIndexUniqueness(isUnique: Boolean)
    : StatisticsBackedLogicalPlanningConfigurationBuilder = {
    if (isUnique) {
      plannerBuilder()
        .addRelationshipIndex("REL", Seq("prop"), 0.01, 0.001, isUnique = true)
    } else {
      plannerBuilder()
    }
  }

  for {
    (pred, indexStr) <- Seq(
      "r.prop = 123" -> "prop = 123",
      "r.prop > 123" -> "prop > 123",
      "r.prop < 123" -> "prop < 123",
      "123 < r.prop" -> "prop > 123",
      "10 <= r.prop < 123" -> "10 <= prop < 123",
      "123 >= r.prop > 10" -> "10 < prop <= 123",
      "r.prop IN [1, 2]" -> "prop = 1 OR 2"
    )
    isUnique <- Seq(true, false)
  } {

    test(s"should plan undirected relationship index seek for $pred (isUnique = $isUnique)") {
      val planner = plannerBuilderWithIndexUniqueness(isUnique).build()

      planner.plan(s"MATCH (a)-[r:REL]-(b) WHERE $pred RETURN r") should equal(
        planner.planBuilder()
          .produceResults("r")
          .relationshipIndexOperator(s"(a)-[r:REL($indexStr)]-(b)", indexType = IndexType.RANGE, unique = isUnique)
          .build()
      )
    }

    test(s"should plan directed OUTGOING relationship index seek for $pred (isUnique = $isUnique)") {
      val planner = plannerBuilderWithIndexUniqueness(isUnique).build()

      planner.plan(s"MATCH (a)-[r:REL]->(b) WHERE $pred RETURN r") should equal(
        planner.planBuilder()
          .produceResults("r")
          .relationshipIndexOperator(s"(a)-[r:REL($indexStr)]->(b)", indexType = IndexType.RANGE, unique = isUnique)
          .build()
      )
    }

    test(s"should plan directed INCOMING relationship index seek for $pred (isUnique = $isUnique)") {
      val planner = plannerBuilderWithIndexUniqueness(isUnique).build()

      planner.plan(s"MATCH (a)<-[r:REL]-(b) WHERE $pred RETURN r") should equal(
        planner.planBuilder()
          .produceResults("r")
          .relationshipIndexOperator(s"(a)<-[r:REL($indexStr)]-(b)", indexType = IndexType.RANGE, unique = isUnique)
          .build()
      )
    }

    test(
      s"should plan undirected relationship index seek on the RHS of an Apply with correct arguments for $pred (isUnique = $isUnique)"
    ) {
      val planner = plannerBuilderWithIndexUniqueness(isUnique).build()

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
          .apply()
          .|.relationshipIndexOperator(
            s"(a)-[r:REL($indexStr)]-(b)",
            argumentIds = Set("n"),
            indexType = IndexType.RANGE,
            unique = isUnique
          )
          .allNodeScan("n")
          .build()
      )
    }

    test(
      s"should plan undirected relationship index seek over NodeByLabelScan using a hint for $pred (isUnique = $isUnique)"
    ) {
      val planner = plannerBuilderWithIndexUniqueness(isUnique)
        .setLabelCardinality("A", 10)
        .setRelationshipCardinality("(:A)-[:REL]-()", 10)
        .build()

      planner.plan(s"MATCH (a:A)-[r:REL]-(b) USING INDEX r:REL(prop) WHERE $pred RETURN r") should equal(
        planner.planBuilder()
          .produceResults("r")
          .filterExpression(hasLabels("a", "A"))
          .relationshipIndexOperator(s"(a)-[r:REL($indexStr)]-(b)", indexType = IndexType.RANGE, unique = isUnique)
          .build()
      )
    }

    test(
      s"should plan relationship index seek with filter for already bound start node for $pred (isUnique = $isUnique)"
    ) {
      val planner = plannerBuilderWithIndexUniqueness(isUnique).build()
      planner.plan(
        s"""MATCH (a) WITH a SKIP 0
           |MATCH (a)-[r:REL]-(b) WHERE $pred RETURN r""".stripMargin
      ) should equal(
        planner.planBuilder()
          .produceResults("r")
          // the filter got pushed up through the apply
          .filter("a = anon_0")
          .apply()
          .|.relationshipIndexOperator(
            s"(anon_0)-[r:REL($indexStr)]-(b)",
            argumentIds = Set("a"),
            indexType = IndexType.RANGE,
            unique = isUnique
          )
          .skip(0)
          .allNodeScan("a")
          .build()
      )
    }

    test(
      s"should plan relationship index seek with filter for already bound end node for $pred (isUnique = $isUnique)"
    ) {
      val planner = plannerBuilderWithIndexUniqueness(isUnique).build()
      planner.plan(
        s"""MATCH (b) WITH b SKIP 0
           |MATCH (a)-[r:REL]-(b) WHERE $pred RETURN r""".stripMargin
      ) should equal(
        planner.planBuilder()
          .produceResults("r")
          // the filter got pushed up through the apply
          .filter("b = anon_0")
          .apply()
          .|.relationshipIndexOperator(
            s"(a)-[r:REL($indexStr)]-(anon_0)",
            argumentIds = Set("b"),
            indexType = IndexType.RANGE,
            unique = isUnique
          )
          .skip(0)
          .allNodeScan("b")
          .build()
      )
    }

    test(s"should plan relationship index seek with filter for already bound nodes for $pred (isUnique = $isUnique)") {
      val planner = plannerBuilderWithIndexUniqueness(isUnique).build()
      planner.plan(
        s"""MATCH (a), (b) WITH a, b SKIP 0
           |MATCH (a)-[r:REL]-(b) WHERE $pred RETURN r""".stripMargin
      ) should equal(
        planner.planBuilder()
          .produceResults("r")
          // the filter got pushed up through the apply
          .filter("a = anon_0", "b = anon_1")
          .apply()
          .|.relationshipIndexOperator(
            s"(anon_0)-[r:REL($indexStr)]-(anon_1)",
            argumentIds = Set("a", "b"),
            indexType = IndexType.RANGE,
            unique = isUnique
          )
          .skip(0)
          .cartesianProduct()
          .|.allNodeScan("b")
          .allNodeScan("a")
          .build()
      )
    }

    test(
      s"should not plan relationship index seek for already bound relationship variable for $pred (isUnique = $isUnique)"
    ) {
      val planner = plannerBuilderWithIndexUniqueness(isUnique).build()
      withClue("Did not expect an UndirectedRelationshipIndexSeek to be planned") {
        planner.plan(
          s"""MATCH (a)-[r:REL]-(b) WITH r SKIP 0
             |MATCH (a2)-[r:REL]-(b2) WHERE $pred RETURN r""".stripMargin
        ).leaves.folder.treeExists {
          case _: UndirectedRelationshipIndexSeek => true
        } should be(false)
      }
    }
  }

  test("should plan the leaf with the longest prefix if multiple STARTS WITH patterns") {
    val planner = plannerBuilder().build()
    // Add an uninteresting predicate using a parameter to stop autoparameterization from happening
    planner.plan(
      """MATCH (a)-[r:REL]->(b)
        |WHERE 43 = $apa
        |  AND r.prop STARTS WITH 'w'
        |  AND r.prop STARTS WITH 'www'
        |RETURN r
        |""".stripMargin
    ).leaves.head should equal(
      planner.subPlanBuilder()
        .relationshipIndexOperator(s"(a)-[r:REL(prop STARTS WITH 'www')]->(b)", indexType = IndexType.RANGE)
        .build()
    )
  }

  test("should not use index seek by range when rhs of > inequality depends on property") {
    val planner = plannerBuilder().build()
    withClue("Used relationship index when not expected:") {
      planner.plan("MATCH (a)-[r:REL]-(b) WHERE r.prop > a.prop RETURN count(r) as c").folder.treeExists {
        case _: UndirectedRelationshipIndexSeek => true
        case _: DirectedRelationshipIndexSeek   => true
      } should be(false)
    }
  }

  test("should not use index seek by range when rhs of <= inequality depends on property") {
    val planner = plannerBuilder().build()
    withClue("Used relationship index when not expected:") {
      planner.plan("MATCH (a)-[r:REL]-(b) WHERE r.prop <= a.prop RETURN count(r) as c").folder.treeExists {
        case _: UndirectedRelationshipIndexSeek => true
        case _: DirectedRelationshipIndexSeek   => true
      } should be(false)
    }
  }

  test("should use index seek by range when rhs of > inequality depends on variable in horizon") {
    val planner = plannerBuilder().build()
    planner.plan(
      """WITH 200 AS x
        |MATCH (a)-[r:REL]-(b) WHERE r.prop > x RETURN count(r) as c
        |""".stripMargin
    ).leaves(1) should equal(
      planner.subPlanBuilder()
        .relationshipIndexOperator(
          s"(a)-[r:REL(prop > ???)]-(b)",
          argumentIds = Set("x"),
          paramExpr = Some(v"x"),
          indexType = IndexType.RANGE
        )
        .build()
    )
  }

  test("should pick index with smaller selectivity") {
    val planner = super.plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1)
      .addRelationshipIndex("REL", Seq("prop2"), 0.9, 0.1)
      .build()
    planner.plan(
      """MATCH (a)-[r:REL]->(b)
        |WHERE r.prop > 1
        |  AND r.prop2 > 1
        |RETURN r""".stripMargin
    ).leaves.head should equal(
      planner.subPlanBuilder()
        .relationshipIndexOperator(s"(a)-[r:REL(prop > 1)]->(b)", indexType = IndexType.RANGE)
        .build()
    )
  }

  private def compositeRelIndexSeekConfig(): StatisticsBackedLogicalPlanningConfiguration = plannerBuilder()
    .addRelationshipIndex("REL", Seq("prop1", "prop2"), existsSelectivity = 0.5, uniqueSelectivity = 0.5)
    .build()

  test(
    "should plan composite relationship index seek when there is an index on two properties and both are in equality predicates"
  ) {
    val planner = compositeRelIndexSeekConfig()
    val query = "MATCH (a)-[r:REL]->(b) WHERE r.prop1 = 42 AND r.prop2 = 'foo' RETURN r"
    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .relationshipIndexOperator("(a)-[r:REL(prop1 = 42, prop2 = 'foo')]->(b)", indexType = IndexType.RANGE)
      .build()
  }

  test(
    "should plan composite relationship index seek when there is an index on two properties and both are in equality predicates regardless of predicate order"
  ) {
    val planner = compositeRelIndexSeekConfig()
    val query = "MATCH (a)-[r:REL]->(b) WHERE r.prop2 = 'foo' AND r.prop1 = 42 RETURN r"
    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .relationshipIndexOperator("(a)-[r:REL(prop1 = 42, prop2 = 'foo')]->(b)", indexType = IndexType.RANGE)
      .build()
  }

  test(
    "should plan composite relationship index seek and filter when there is an index on two properties and both are in equality predicates together with other predicates"
  ) {
    val planner = compositeRelIndexSeekConfig()
    val query = "MATCH (a)-[r:REL]->(b) WHERE r.prop2 = 'foo' AND r.name IS NOT NULL AND r.prop1 = 42 RETURN r"
    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .filter("r.name IS NOT NULL")
      .relationshipIndexOperator("(a)-[r:REL(prop1 = 42, prop2 = 'foo')]->(b)", indexType = IndexType.RANGE)
      .build()
  }

  test(
    "should plan composite relationship unique index seek when there is an index on two properties and both are in equality predicates"
  ) {
    val planner = plannerBuilder()
      .addRelationshipIndex(
        "REL",
        Seq("prop1", "prop2"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.5,
        isUnique = true
      )
      .build()

    val query = "MATCH (a)-[r:REL]->(b) WHERE r.prop1 = 42 AND r.prop2 = 'foo' RETURN r"
    val plan = planner.plan(query).stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .relationshipIndexOperator("(a)-[r:REL(prop1 = 42, prop2 = 'foo')]->(b)", unique = true)
      .build()
  }

  test("should plan relationship index seek for self-loops") {
    val planner = plannerBuilder().build()

    planner.plan(s"MATCH (a)-[r:REL {prop: 1}]-(a) RETURN r") should equal(
      planner.planBuilder()
        .produceResults("r")
        .filter("a = anon_0")
        .relationshipIndexOperator("(a)-[r:REL(prop = 1)]-(anon_0)", indexType = IndexType.RANGE)
        .build()
    )
  }
}
