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

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._

/*
A very simplistic cost model. Each row returned by an operator costs 1. That's it.
 */
case class CardinalityCostModel(cardinality: CardinalityModel) extends CostModel {

  val CPU_BOUND_PLAN_COST_PER_ROW: CostPerRow = 0.1
  val DB_ACCESS_BOUND_PLAN_COST_PER_ROW: CostPerRow = 1.0

  case class CostCoefficients(lhsCoefficient: CostPerRow, rhsCoefficient: CostPerRow, outputCoefficient: CostPerRow, constantCost: Cost)

  def costCoefficients(plan: LogicalPlan): CostCoefficients = plan match {

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
         _: UnwindCollection =>
      CostCoefficients(
        CPU_BOUND_PLAN_COST_PER_ROW,
        CPU_BOUND_PLAN_COST_PER_ROW,
        CPU_BOUND_PLAN_COST_PER_ROW,
        Cost(0))

    case _ =>
      CostCoefficients(
        DB_ACCESS_BOUND_PLAN_COST_PER_ROW,
        DB_ACCESS_BOUND_PLAN_COST_PER_ROW,
        DB_ACCESS_BOUND_PLAN_COST_PER_ROW,
        Cost(0))
  }

  def apply(plan: LogicalPlan): Cost = {
    val CostCoefficients(lhsCoefficient, rhsCoefficient, outputCoefficient, constantCost) = costCoefficients(plan)
    plan.lhs.map(cardinality).map(_ * lhsCoefficient).getOrElse(Cost(0)) +
      plan.rhs.map(cardinality).map(_ * lhsCoefficient).getOrElse(Cost(0)) +
      cardinality(plan) * outputCoefficient + constantCost
  }
}
