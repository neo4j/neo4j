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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.ast.Collection
import org.neo4j.cypher.internal.compiler.v2_2.commands.ManyQueryExpression
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._

/*
A very simplistic cost model. Each row returned by an operator costs 1. That's it.
 */
case class CardinalityCostModel(cardinality: CardinalityModel) extends CostModel {

  val CPU_BOUND_PLAN_COST_PER_ROW = CostPerRow(0.1)
  val DB_ACCESS_BOUND_PLAN_COST_PER_ROW = CostPerRow(1.0)

  private def costPerRow(plan: LogicalPlan) = plan match {
    case _: NodeHashJoin |
      _: Aggregation |
      _: Apply |
      _: CartesianProduct |
      _: AbstractLetSemiApply |
      _: Limit |
      _: Optional |
      _: SingleRow |
      _: OuterHashJoin |
      _: AbstractSemiApply |
      _: Skip |
      _: Sort |
      _: SortedLimit |
      _: Union |
      _: UnwindCollection
    => CPU_BOUND_PLAN_COST_PER_ROW

    case NodeIndexSeek(_, _, _, ManyQueryExpression(Collection(elements)), _) =>
      DB_ACCESS_BOUND_PLAN_COST_PER_ROW * Multiplier(elements.size)

    case NodeIndexSeek(_, _, _, ManyQueryExpression(Collection(elements)), _) =>
      DB_ACCESS_BOUND_PLAN_COST_PER_ROW * Multiplier(10)

    case _ => DB_ACCESS_BOUND_PLAN_COST_PER_ROW
  }

  private def cardinalityForPlan(plan: LogicalPlan) = plan match {
    case Selection(_, left) => cardinality(left)
    case exp: Expand        => cardinality(exp)
    case _                  => plan.lhs.map(cardinality).getOrElse(cardinality(plan))
  }

  def apply(plan: LogicalPlan): Cost = {
    val totalCost = cardinalityForPlan(plan) * costPerRow(plan) +
    plan.lhs.map(this).getOrElse(Cost(0)) +
    plan.rhs.map(this).getOrElse(Cost(0))

    totalCost
  }
}
