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
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.DatabaseFormat
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.StableLeafPlan
import org.neo4j.cypher.internal.planner.spi.LeafStability.MvccEmptyTx
import org.neo4j.cypher.internal.planner.spi.LeafStability.MvccNonEmptyTx
import org.neo4j.cypher.internal.planner.spi.LeafStability.NonMvcc

class StableLeafPlanningIntegrationTest extends CypherPlannerTestSuite
    with LogicalPlanningIntegrationTestSupport {

  private def mvccPlanner() =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("N", 50)
      .setDatabaseFormat(DatabaseFormat.Mvcc)

  test("marks MvccEmptyTx on the leftmost stable leaf for an MVCC write query with empty tx state") {
    val planner = mvccPlanner().build()

    val planState = planner.planState("MATCH (n:N) SET n.prop = 1")
    val leaf = planState.logicalPlan.leftmostLeaf

    leaf shouldBe a[StableLeafPlan]
    planState.planningAttributes.stableLeafPlans.get(leaf.id) shouldBe MvccEmptyTx
  }

  test("marks MvccNonEmptyTx on the leftmost stable leaf for an MVCC write query with dirty tx state") {
    val planner = mvccPlanner().setTxStateHasChanges(true).build()

    val planState = planner.planState("MATCH (n:N) SET n.prop = 1")
    val leaf = planState.logicalPlan.leftmostLeaf

    leaf shouldBe a[StableLeafPlan]
    planState.planningAttributes.stableLeafPlans.get(leaf.id) shouldBe MvccNonEmptyTx
  }

  test("leaves NonMvcc for a read-only MVCC query") {
    val planner = mvccPlanner().build()

    val planState = planner.planState("MATCH (n:N) RETURN n")
    val leaf = planState.logicalPlan.leftmostLeaf

    planState.planningAttributes.stableLeafPlans.get(leaf.id) shouldBe NonMvcc
  }

  test("leaves NonMvcc for a write query on a non-MVCC format") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(100)
        .setLabelCardinality("N", 50)
        .build()

    val planState = planner.planState("MATCH (n:N) SET n.prop = 1")
    val leaf = planState.logicalPlan.leftmostLeaf

    planState.planningAttributes.stableLeafPlans.get(leaf.id) shouldBe NonMvcc
  }

  test("leaves NonMvcc when the leftmost leaf of an MVCC write query is not a StableLeafPlan") {
    val planner = mvccPlanner().build()

    val planState = planner.planState("MATCH (n) WHERE id(n) = 0 SET n.prop = 1")
    val leaf = planState.logicalPlan.leftmostLeaf

    leaf shouldBe a[NodeByIdSeek]
    leaf should not(be(a[StableLeafPlan]))
    planState.planningAttributes.stableLeafPlans.get(leaf.id) shouldBe NonMvcc
  }

  test("the classification survives EagerRewriter inserting an Eager above the marked leaf") {
    val planner = mvccPlanner().build()

    val planState =
      planner.planState("MATCH (x) WITH x, 1 AS dummy MATCH (n:N) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS")
    val plan = planState.logicalPlan
    val leaf = plan.leftmostLeaf

    plan.folder.treeExists { case _: Eager => true } shouldBe true
    leaf shouldBe a[StableLeafPlan]
    planState.planningAttributes.stableLeafPlans.get(leaf.id) shouldBe MvccEmptyTx
  }
}
