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

import org.neo4j.cypher.internal.frontend.phases.LeafPlanOperatorMetricKey
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedAllNodesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipUniqueIndexSeek

/**
 * Classifies the store-access leaf operators of a logical plan tree into their
 * [[LeafPlanOperatorMetricKey]] categories and counts the occurrences of each.
 *
 * Partitioned variants fold into their base category and index contains/ends-with scans fold
 * into the index-seek category. Every occurrence anywhere in the tree, including nested/subquery
 * branches, is counted; non-leaf and non-store-access operators are ignored.
 */
object LeafPlanOperatorCounts {

  private val classify: PartialFunction[Any, LeafPlanOperatorMetricKey] = {
    case _: AllNodesScan | _: PartitionedAllNodesScan       => LeafPlanOperatorMetricKey.ALL_NODES_SCAN
    case _: NodeByLabelScan | _: PartitionedNodeByLabelScan => LeafPlanOperatorMetricKey.NODE_LABEL_SCAN
    case _: NodeIndexScan | _: PartitionedNodeIndexScan     => LeafPlanOperatorMetricKey.NODE_INDEX_SCAN
    case _: NodeIndexSeek | _: NodeUniqueIndexSeek | _: PartitionedNodeIndexSeek | _: NodeIndexContainsScan |
      _: NodeIndexEndsWithScan =>
      LeafPlanOperatorMetricKey.NODE_INDEX_SEEK
    case _: DirectedRelationshipTypeScan | _: UndirectedRelationshipTypeScan |
      _: PartitionedDirectedRelationshipTypeScan | _: PartitionedUndirectedRelationshipTypeScan =>
      LeafPlanOperatorMetricKey.RELATIONSHIP_TYPE_SCAN
    case _: DirectedRelationshipIndexScan | _: UndirectedRelationshipIndexScan |
      _: PartitionedDirectedRelationshipIndexScan | _: PartitionedUndirectedRelationshipIndexScan =>
      LeafPlanOperatorMetricKey.RELATIONSHIP_INDEX_SCAN
    case _: DirectedRelationshipIndexSeek | _: UndirectedRelationshipIndexSeek |
      _: DirectedRelationshipUniqueIndexSeek | _: UndirectedRelationshipUniqueIndexSeek |
      _: PartitionedDirectedRelationshipIndexSeek | _: PartitionedUndirectedRelationshipIndexSeek |
      _: DirectedRelationshipIndexContainsScan | _: UndirectedRelationshipIndexContainsScan |
      _: DirectedRelationshipIndexEndsWithScan | _: UndirectedRelationshipIndexEndsWithScan =>
      LeafPlanOperatorMetricKey.RELATIONSHIP_INDEX_SEEK
  }

  def apply(logicalPlan: LogicalPlan): Map[LeafPlanOperatorMetricKey, Int] =
    logicalPlan.folder
      .treeCollect(classify)
      .groupBy(identity)
      .map { case (category, occurrences) => category -> occurrences.size }
}
