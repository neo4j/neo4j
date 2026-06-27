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

import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.util.Foldable.FoldableAny

class PushSemiApplyAboveAggregationIntegrationTest extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport {

  // An LDBC-SNB-shaped slice sufficient to plan BI Q18 (friend recommendation). A Person(id) node
  // key (uniqueness + existence) is what lets the rewrite expose the person nodes through the
  // aggregation; a uniqueness constraint alone is not enough, since it permits NULL ids.
  private def ldbcPlanner = plannerBuilder()
    .setAllNodesCardinality(3_000_000)
    .setLabelCardinality("Person", 10_295)
    .setLabelCardinality("Tag", 16_080)
    .setRelationshipCardinality("()-[:KNOWS]->()", 346_028)
    .setRelationshipCardinality("(:Person)-[:KNOWS]->()", 346_028)
    .setRelationshipCardinality("()-[:KNOWS]->(:Person)", 346_028)
    .setRelationshipCardinality("(:Person)-[:KNOWS]->(:Person)", 346_028)
    .setRelationshipCardinality("()-[:HAS_INTEREST]->()", 238_052)
    .setRelationshipCardinality("(:Person)-[:HAS_INTEREST]->()", 238_052)
    .setRelationshipCardinality("(:Person)-[:HAS_INTEREST]->(:Tag)", 238_052)
    .setRelationshipCardinality("()-[:HAS_INTEREST]->(:Tag)", 238_052)
    .addNodeIndex("Person", List("id"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 10_295, isUnique = true)
    .addNodeIndex("Tag", List("name"), existsSelectivity = 1.0, uniqueSelectivity = 1.0 / 16_080)

  // Person(id) is unique AND present (a node key) — the rewrite may expose the person nodes.
  private val planner = ldbcPlanner.addNodeExistenceConstraint("Person", "id").build()

  // Person(id) is unique but may be NULL — the rewrite must not expose the person nodes.
  private val plannerUniqueOnly = ldbcPlanner.build()

  private val q18 =
    """MATCH (tag:Tag {name: 'Frank_Sinatra'})<-[:HAS_INTEREST]-(person1:Person)
      |      -[:KNOWS]-(mutualFriend:Person)-[:KNOWS]-(person2:Person)-[:HAS_INTEREST]->(tag)
      |WHERE person1 <> person2 AND NOT EXISTS { MATCH (person1)-[:KNOWS]-(person2) }
      |RETURN person1.id AS p1, person2.id AS p2, count(DISTINCT mutualFriend) AS cnt
      |ORDER BY cnt DESC, p1 ASC, p2 ASC LIMIT 20""".stripMargin

  test("pushes the NOT EXISTS anti-join above the count(DISTINCT) aggregation in BI Q18") {
    val plan = planner.plan(q18)

    // The anti-join is evaluated once per grouped (person1, person2) pair, i.e. it sits ABOVE the
    // aggregation: some AntiSemiApply has an Aggregation in its left subtree.
    withClue(plan) {
      plan.folder.findAllByClass[AntiSemiApply].exists(
        _.left.folder.treeExists { case _: Aggregation => true }
      ) shouldBe true
    }
  }

  test("the pushed-down aggregation groups by the person nodes (functional-dependency exposure)") {
    val plan = planner.plan(q18)

    // Q18 groups by person1.id / person2.id; to move the anti-join above, the rewrite adds the
    // person1 / person2 node variables to the grouping (a no-op on the groups under the id uniqueness
    // constraint) so the anti-join can still reach them.
    val groupsByPersonNodes = plan.folder.findAllByClass[Aggregation].exists { agg =>
      val keys = agg.groupingExpressions.keySet.map(_.name)
      keys.contains("person1") && keys.contains("person2")
    }
    withClue(plan) {
      groupsByPersonNodes shouldBe true
    }
  }

  test("does not fire when Person(id) is unique but not guaranteed present (NULL-soundness guard)") {
    val plan = plannerUniqueOnly.plan(q18)

    // With only a uniqueness constraint, id may be NULL, so person1.id does not determine person1.
    // The rewrite must not add the person nodes to the grouping, so the anti-join stays BELOW the
    // aggregation (no AntiSemiApply has an Aggregation in its left subtree).
    withClue(plan) {
      plan.folder.findAllByClass[AntiSemiApply].exists(
        _.left.folder.treeExists { case _: Aggregation => true }
      ) shouldBe false
    }
  }
}
