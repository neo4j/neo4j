/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_1.ast.Collection
import org.neo4j.cypher.internal.compiler.v2_1.commands.{ManyQueryExpression, SingleQueryExpression}
import Metrics._

class SimpleCostModel(cardinality: CardinalityModel) extends CostModel {

  val HASH_TABLE_CONSTRUCTION_OVERHEAD_PER_ROW = CostPerRow(0.001)
  val HASH_TABLE_LOOKUP_OVERHEAD_PER_ROW       = CostPerRow(0.0005)
  val EXPRESSION_PROJECTION_OVERHEAD_PER_ROW   = CostPerRow(0.01)
  val EXPRESSION_SELECTION_OVERHEAD_PER_ROW    = EXPRESSION_PROJECTION_OVERHEAD_PER_ROW
  val INDEX_OVERHEAD_COST_PER_ROW              = CostPerRow(3.0)
  val LABEL_INDEX_OVERHEAD_COST_PER_ROW        = CostPerRow(2.0)
  val SORT_COST_PER_ROW                        = CostPerRow(0.01)
  val STORE_ACCESS_COST_PER_ROW                = CostPerRow(1)
  val DIJKSTRA_OVERHEAD                        = CostPerRow(0.05)

  def apply(plan: LogicalPlan): Cost = plan match {
    case _: SingleRow =>
      Cost(0)

    case _: AllNodesScan =>
      cardinality(plan) * STORE_ACCESS_COST_PER_ROW

    case NodeIndexSeek(_, _, _, SingleQueryExpression(_)) =>
      cardinality(plan) * INDEX_OVERHEAD_COST_PER_ROW

    case NodeIndexSeek(_, _, _, ManyQueryExpression(Collection(elements))) =>
      cardinality(plan) * INDEX_OVERHEAD_COST_PER_ROW * elements.size

    case NodeIndexSeek(_, _, _, ManyQueryExpression(_)) =>
      cardinality(plan) * INDEX_OVERHEAD_COST_PER_ROW * 10 // This is a wild guess.

    case NodeIndexUniqueSeek(_, _, _, SingleQueryExpression(_)) =>
      cardinality(plan) * INDEX_OVERHEAD_COST_PER_ROW

    case NodeIndexUniqueSeek(_, _, _, ManyQueryExpression(Collection(elements))) =>
      cardinality(plan) * INDEX_OVERHEAD_COST_PER_ROW * elements.size

    case NodeIndexUniqueSeek(_, _, _, ManyQueryExpression(_)) =>
      cardinality(plan) * INDEX_OVERHEAD_COST_PER_ROW * 10 // This is a wild guess.

    case _: NodeByLabelScan =>
      cardinality(plan) * LABEL_INDEX_OVERHEAD_COST_PER_ROW

    case _: NodeByIdSeek =>
      cardinality(plan) * STORE_ACCESS_COST_PER_ROW

    case _: DirectedRelationshipByIdSeek =>
      cardinality(plan) * STORE_ACCESS_COST_PER_ROW

    case _: UndirectedRelationshipByIdSeek =>
      cardinality(plan) * STORE_ACCESS_COST_PER_ROW

    case projection: Projection =>
      cost(projection.left) +
      cardinality(projection.left) * EXPRESSION_PROJECTION_OVERHEAD_PER_ROW * projection.numExpressions

    case selection: Selection =>
      cost(selection.left) +
      cardinality(selection.left) * EXPRESSION_SELECTION_OVERHEAD_PER_ROW * selection.numPredicates

    case cartesian: CartesianProduct =>
      cost(cartesian.left) +
      cardinality(cartesian.left) * cost(cartesian.right)

    case applyOp: Apply =>
      applyCost(outer = applyOp.left, inner = applyOp.right)

    case applyOp: SemiApply =>
      applyCost(outer = applyOp.left, inner = applyOp.right)

    case applyOp: LetSemiApply =>
      applyCost(outer = applyOp.left, inner = applyOp.right)

    case applyOp: AntiSemiApply =>
      applyCost(outer = applyOp.left, inner = applyOp.right)

    case applyOp: LetAntiSemiApply =>
      applyCost(outer = applyOp.left, inner = applyOp.right)

    case applyOp: SelectOrSemiApply =>
      selectOrSemiApplyCost(outer = applyOp.left, inner = applyOp.right)

    case applyOp: LetSelectOrSemiApply =>
      selectOrSemiApplyCost(outer = applyOp.left, inner = applyOp.right)

    case applyOp: SelectOrAntiSemiApply =>
      selectOrSemiApplyCost(outer = applyOp.left, inner = applyOp.right)

    case applyOp: LetSelectOrAntiSemiApply =>
      selectOrSemiApplyCost(outer = applyOp.left, inner = applyOp.right)

    case expand: Expand =>
      cost(expand.left) +
      cardinality(expand) * STORE_ACCESS_COST_PER_ROW

    case expand: OptionalExpand =>
      cost(expand.left) +
      cardinality(expand) * EXPRESSION_SELECTION_OVERHEAD_PER_ROW * expand.predicates.length

    case optional: Optional =>
      cost(optional.inputPlan)

    case join: NodeHashJoin =>
      cost(join.left) +
      cost(join.right) +
      cardinality(join.left) * HASH_TABLE_CONSTRUCTION_OVERHEAD_PER_ROW +
      cardinality(join.right) * HASH_TABLE_LOOKUP_OVERHEAD_PER_ROW

    case outerJoin: OuterHashJoin =>
      cost(outerJoin.left) +
      cost(outerJoin.right) +
      cardinality(outerJoin.left) * HASH_TABLE_CONSTRUCTION_OVERHEAD_PER_ROW +
      cardinality(outerJoin.right) * HASH_TABLE_LOOKUP_OVERHEAD_PER_ROW

    case shortestPath: FindShortestPaths =>
      // TODO: shortest path should take two childs
      val sqNodes = cardinality(shortestPath.left)
      val nodes = sqNodes.map(Math.sqrt)
      val edges = sqNodes * GuessingEstimation.DEFAULT_CONNECTIVITY_CHANCE
      val storeCost = (sqNodes + edges) * STORE_ACCESS_COST_PER_ROW
      val dijkstraCost = sqNodes * edges.map(Math.log) * DIJKSTRA_OVERHEAD
      cost(shortestPath.left) + dijkstraCost + storeCost

    case s@Sort(input, _) =>
      cost(input) +
      cardinality(s) * SORT_COST_PER_ROW

    case s@Skip(input, _) =>
      cost(input)

    case l@Limit(input, _) =>
      cost(input) // TODO: This is probably not correct. I think LIMIT should make a plan cheaper

    case s@SortedLimit(input, _, _) =>
      cost(input) +
      cardinality(s) * SORT_COST_PER_ROW
  }

  private def selectOrSemiApplyCost(outer: LogicalPlan, inner: LogicalPlan): Cost =
    cost(outer) + cardinality(outer) * (cost(inner) + EXPRESSION_SELECTION_OVERHEAD_PER_ROW)

  private def applyCost(outer: LogicalPlan, inner: LogicalPlan): Cost =
    cost(outer) + cardinality(outer) * cost(inner)

  private def cost(plan: LogicalPlan): Cost = apply(plan)
}
