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

class SimpleCostModel(cardinality: CardinalityModel) extends CostModel {

  val HASH_TABLE_CONSTRUCTION_OVERHEAD_PER_ROW = 0.001
  val HASH_TABLE_LOOKUP_OVERHEAD_PER_ROW = 0.0005
  val EXPRESSION_PROJECTION_OVERHEAD_PER_ROW = 0.01
  val EXPRESSION_SELECTION_OVERHEAD_PER_ROW = EXPRESSION_PROJECTION_OVERHEAD_PER_ROW
  val INDEX_OVERHEAD_COST_PER_ROW = 3.0
  val LABEL_INDEX_OVERHEAD_COST_PER_ROW = 2.0
  val SORT_COST_PER_ROW = 0.01

  def apply(plan: LogicalPlan): Cost = plan match {
    case _: SingleRow =>
      Cost(0)

    case _: AllNodesScan =>
      Cost(cardinality(plan).amount)

    case _: NodeIndexSeek =>
      Cost(cardinality(plan).amount * INDEX_OVERHEAD_COST_PER_ROW)

    case _: NodeIndexUniqueSeek =>
      Cost(cardinality(plan).amount * INDEX_OVERHEAD_COST_PER_ROW)

    case _: NodeByLabelScan =>
      Cost(cardinality(plan).amount * LABEL_INDEX_OVERHEAD_COST_PER_ROW)

    case _: NodeByIdSeek =>
      Cost(cardinality(plan).amount)

    case _: DirectedRelationshipByIdSeek =>
      Cost(cardinality(plan).amount)

    case _: UndirectedRelationshipByIdSeek =>
      Cost(cardinality(plan).amount)

    case projection: Projection =>
      Cost(cost(projection.left) +
        cardinality(projection.left).amount * EXPRESSION_PROJECTION_OVERHEAD_PER_ROW * projection.numExpressions)

    case selection: Selection =>
      Cost(cost(selection.left) +
        cardinality(selection.left).amount * EXPRESSION_SELECTION_OVERHEAD_PER_ROW * selection.numPredicates)

    case cartesian: CartesianProduct =>
      Cost(cost(cartesian.left) + cardinality(cartesian.left).amount * cost(cartesian.right).gummyBears)

    case applyOp: Apply =>
      Cost(applyCost(outer = applyOp.left, inner = applyOp.right))

    case applyOp: SemiApply =>
      Cost(applyCost(outer = applyOp.left, inner = applyOp.right))

    case applyOp: LetSemiApply =>
      Cost(applyCost(outer = applyOp.left, inner = applyOp.right))

    case applyOp: AntiSemiApply =>
      Cost(applyCost(outer = applyOp.left, inner = applyOp.right))

    case applyOp: LetAntiSemiApply =>
      Cost(applyCost(outer = applyOp.left, inner = applyOp.right))

    case applyOp: SelectOrSemiApply =>
      Cost(selectOrSemiApplyCost(outer = applyOp.left, inner = applyOp.right))

    case applyOp: LetSelectOrSemiApply =>
      Cost(selectOrSemiApplyCost(outer = applyOp.left, inner = applyOp.right))

    case applyOp: SelectOrAntiSemiApply =>
      Cost(selectOrSemiApplyCost(outer = applyOp.left, inner = applyOp.right))

    case applyOp: LetSelectOrAntiSemiApply =>
      Cost(selectOrSemiApplyCost(outer = applyOp.left, inner = applyOp.right))

    case expand: Expand =>
      Cost(cost(expand.left) + cardinality(expand).amount)

    case expand: OptionalExpand =>
      Cost(cost(expand.left) + cardinality(expand).amount * expand.predicates.length * EXPRESSION_SELECTION_OVERHEAD_PER_ROW)

    case optional: Optional =>
      cost(optional.inputPlan)

    case join: NodeHashJoin =>
      Cost(cost(join.left) +
        cost(join.right) +
        cardinality(join.left).amount * HASH_TABLE_CONSTRUCTION_OVERHEAD_PER_ROW +
        cardinality(join.right).amount * HASH_TABLE_LOOKUP_OVERHEAD_PER_ROW)

    case outerJoin: OuterHashJoin =>
      Cost(cost(outerJoin.left) +
        cost(outerJoin.right) +
        cardinality(outerJoin.left).amount * HASH_TABLE_CONSTRUCTION_OVERHEAD_PER_ROW +
        cardinality(outerJoin.right).amount * HASH_TABLE_LOOKUP_OVERHEAD_PER_ROW)

    case s @ Sort(input, _) =>
      Cost(cost(input) + cardinality(s).amount * SORT_COST_PER_ROW)

    case s @ Skip(input, _) =>
      Cost(cost(input) + cardinality(s).amount)

    case l @ Limit(input, _) =>
      Cost(cost(input) + cardinality(l).amount)

    case s @ SortedLimit(input, _, _) =>
      Cost(cost(input) + cardinality(s).amount * SORT_COST_PER_ROW)
  }

  private def selectOrSemiApplyCost(outer: LogicalPlan, inner: LogicalPlan): Double =
    cost(outer) + cardinality(outer).amount * (cost(inner) + EXPRESSION_SELECTION_OVERHEAD_PER_ROW)

  private def applyCost(outer: LogicalPlan, inner: LogicalPlan): Double =
    cost(outer) + cardinality(outer).amount * cost(inner).gummyBears

  private def cost(plan: LogicalPlan) = apply(plan)
}
