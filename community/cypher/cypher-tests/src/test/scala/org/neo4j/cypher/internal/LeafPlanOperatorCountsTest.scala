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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.frontend.phases.LeafPlanOperatorMetricKey
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LeafPlanOperatorCountsTest extends CypherFunSuite {

  private def countsOf(plan: LogicalPlan): Map[LeafPlanOperatorMetricKey, Int] =
    LeafPlanOperatorCounts(plan)

  test("counts each node leaf operator category") {
    val plan = new LogicalPlanBuilder()
      .produceResults("a", "b", "c", "d")
      .cartesianProduct()
      .|.nodeIndexOperator("d:D(prop = 1)")
      .cartesianProduct()
      .|.nodeIndexOperator("c:C(prop)")
      .cartesianProduct()
      .|.nodeByLabelScan("b", "B")
      .allNodeScan("a")
      .build()

    countsOf(plan) shouldBe Map(
      LeafPlanOperatorMetricKey.ALL_NODES_SCAN -> 1,
      LeafPlanOperatorMetricKey.NODE_LABEL_SCAN -> 1,
      LeafPlanOperatorMetricKey.NODE_INDEX_SCAN -> 1,
      LeafPlanOperatorMetricKey.NODE_INDEX_SEEK -> 1
    )
  }

  test("counts each relationship leaf operator category") {
    val plan = new LogicalPlanBuilder()
      .produceResults("a", "b", "c")
      .cartesianProduct()
      .|.relationshipIndexOperator("(x)-[c:C(prop = 1)]->(y)")
      .cartesianProduct()
      .|.relationshipIndexOperator("(x)-[b:B(prop)]->(y)")
      .relationshipTypeScan("(x)-[a:A]->(y)")
      .build()

    countsOf(plan) shouldBe Map(
      LeafPlanOperatorMetricKey.RELATIONSHIP_TYPE_SCAN -> 1,
      LeafPlanOperatorMetricKey.RELATIONSHIP_INDEX_SCAN -> 1,
      LeafPlanOperatorMetricKey.RELATIONSHIP_INDEX_SEEK -> 1
    )
  }

  test("unique index seek counts as index seek") {
    val plan = new LogicalPlanBuilder()
      .produceResults("a")
      .nodeIndexOperator("a:A(prop = 1)", unique = true)
      .build()

    countsOf(plan) shouldBe Map(LeafPlanOperatorMetricKey.NODE_INDEX_SEEK -> 1)
  }

  test("partitioned variants fold into their base categories") {
    val plan = new LogicalPlanBuilder()
      .produceResults("a", "b", "c", "d", "e")
      .cartesianProduct()
      .|.partitionedRelationshipTypeScan("(x)-[e:E]->(y)")
      .cartesianProduct()
      .|.partitionedNodeIndexOperator("d:D(prop = 1)")
      .cartesianProduct()
      .|.partitionedNodeIndexOperator("c:C(prop)")
      .cartesianProduct()
      .|.partitionedNodeByLabelScan("b", "B")
      .partitionedAllNodeScan("a")
      .build()

    countsOf(plan) shouldBe Map(
      LeafPlanOperatorMetricKey.ALL_NODES_SCAN -> 1,
      LeafPlanOperatorMetricKey.NODE_LABEL_SCAN -> 1,
      LeafPlanOperatorMetricKey.NODE_INDEX_SCAN -> 1,
      LeafPlanOperatorMetricKey.NODE_INDEX_SEEK -> 1,
      LeafPlanOperatorMetricKey.RELATIONSHIP_TYPE_SCAN -> 1
    )
  }

  test("partitioned and non-partitioned variants of a category are counted together") {
    val plan = new LogicalPlanBuilder()
      .produceResults("a", "b")
      .cartesianProduct()
      .|.partitionedAllNodeScan("b")
      .allNodeScan("a")
      .build()

    countsOf(plan) shouldBe Map(LeafPlanOperatorMetricKey.ALL_NODES_SCAN -> 2)
  }

  test("counts every occurrence in nested branches") {
    val plan = new LogicalPlanBuilder()
      .produceResults("a", "b", "c")
      .apply()
      .|.union()
      .|.|.nodeByLabelScan("c", "L")
      .|.nodeByLabelScan("b", "L")
      .nodeByLabelScan("a", "L")
      .build()

    countsOf(plan) shouldBe Map(LeafPlanOperatorMetricKey.NODE_LABEL_SCAN -> 3)
  }

  test("ignores non-store-access and non-leaf operators") {
    val plan = new LogicalPlanBuilder()
      .produceResults("a", "b")
      .filter("a.prop > 1")
      .nodeHashJoin("a")
      .|.expandAll("(b)-[r]->(a)")
      .|.argument("b")
      .projection("a AS a")
      .argument("a")
      .build()

    countsOf(plan) shouldBe empty
  }
}
