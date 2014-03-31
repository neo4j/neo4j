/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import Metrics._

class SimpleCostModel(cardinality: CardinalityEstimator) extends CostModel {

  val HASH_TABLE_CONSTRUCTION_OVERHEAD_PER_ROW = 0.001
  val HASH_TABLE_LOOKUP_OVERHEAD_PER_ROW = 0.0005
  val EXPRESSION_PROJECTION_OVERHEAD_PER_ROW = 0.01
  val EXPRESSION_SELECTION_OVERHEAD_PER_ROW = EXPRESSION_PROJECTION_OVERHEAD_PER_ROW
  val INDEX_OVERHEAD_COST_PER_ROW = 3
  val LABEL_INDEX_OVERHEAD_COST_PER_ROW = 3

  def apply(plan: LogicalPlan): Int = plan match {
    case _: SingleRow =>
      cardinality(plan)

    case _: AllNodesScan =>
      cardinality(plan)

    case _: NodeIndexSeek =>
      cardinality(plan) * INDEX_OVERHEAD_COST_PER_ROW

    case _: NodeIndexUniqueSeek =>
      cardinality(plan) * INDEX_OVERHEAD_COST_PER_ROW

    case _: NodeByLabelScan =>
      cardinality(plan) * LABEL_INDEX_OVERHEAD_COST_PER_ROW

    case _: NodeByIdSeek =>
      cardinality(plan)

    case _: DirectedRelationshipByIdSeek =>
      cardinality(plan)

    case _: UndirectedRelationshipByIdSeek =>
      cardinality(plan)

    case projection: Projection => (
      cost(projection.left) +
      cardinality(projection.left) * EXPRESSION_PROJECTION_OVERHEAD_PER_ROW * projection.numExpressions
    ).toInt

    case selection: Selection => (
      cost(selection.left) +
      cardinality(selection.left) * EXPRESSION_SELECTION_OVERHEAD_PER_ROW * selection.numPredicates
    ).toInt

    case cartesian: CartesianProduct =>
      cost(cartesian.left) + cardinality(cartesian.left) * cost(cartesian.right)

    case expand: Expand =>
      cost(expand.left) + cardinality(expand)

    case join: NodeHashJoin => (
      cost(join.left) +
      cost(join.right) +
      cardinality(join.left) * HASH_TABLE_CONSTRUCTION_OVERHEAD_PER_ROW +
      cardinality(join.right) * HASH_TABLE_LOOKUP_OVERHEAD_PER_ROW
    ).toInt
  }

  private def cost(plan: LogicalPlan) = apply(plan)
}
