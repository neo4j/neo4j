/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_2.ast.{HasLabels, Property, Collection}
import org.neo4j.cypher.internal.compiler.v2_2.commands.ManyQueryExpression
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._

case class CardinalityCostModel(cardinality: CardinalityModel) extends CostModel {

  val CPU_BOUND: CostPerRow = 0.1
  val FAST_STORE: CostPerRow = 1.0
  val SLOW_STORE: CostPerRow = 10.0

  private def costPerRow(plan: LogicalPlan): CostPerRow = plan match {

    case _: AllNodesScan |
         _: DirectedRelationshipByIdSeek |
         _: UndirectedRelationshipByIdSeek |
         _: ProjectEndpoints |
         _: NodeByLabelScan
    => FAST_STORE

    // Filtering on labels and properties
    case Selection(predicates, _) if predicates.exists {
      case _:Property | _: HasLabels => true
      case _ => false
    }
    => FAST_STORE

    case _: NodeHashJoin |
         _: Aggregation |
         _: AbstractLetSemiApply |
         _: Limit |
         _: Optional |
         _: SingleRow |
         _: Argument |
         _: OuterHashJoin |
         _: AbstractSemiApply |
         _: Skip |
         _: Sort |
         _: SortedLimit |
         _: Union |
         _: Selection |
         _: UnwindCollection
    => CPU_BOUND

    case _: Expand |
         _: FindShortestPaths |
         _: LegacyIndexSeek |
         _: NodeByIdSeek |
         _: NodeIndexUniqueSeek |
         _: NodeIndexSeek |
         _: VarExpand
    => SLOW_STORE

    case NodeIndexSeek(_, _, _, ManyQueryExpression(Collection(elements)), _)
    => SLOW_STORE * Multiplier(elements.size)

  }

  private def cardinalityForPlan(plan: LogicalPlan, input: QueryGraphCardinalityInput): Cardinality = plan match {
    case Selection(_, left) => cardinality(left, input)
    case _ => plan.lhs.map(p => cardinality(p, input)).getOrElse(cardinality(plan, input))
  }

  def apply(plan: LogicalPlan, input: QueryGraphCardinalityInput): Cost = plan match {

    case CartesianProduct(lhs, rhs) =>
      apply(lhs, input) + cardinality(lhs, input) * apply(rhs, input)

    case ApplyVariants(lhs, rhs) =>
      val newInputCardinality = cardinality(lhs, input)
      val newInput = input.withCardinality(newInputCardinality)

      val lCost = apply(lhs, input)
      val rCost = apply(rhs, newInput)
      lCost + rCost

    case HashJoin(lhs, rhs) =>
      val lCost = apply(lhs, input)
      val rCost = apply(rhs, input)
      val PROBE_BUILD_COST = CPU_BOUND
      val PROBE_SEARCH_COST = CPU_BOUND * .5
      val lhsCardinality = cardinality(lhs, input)
      val rhsCardinality = cardinality(rhs, input)

      lCost + rCost +
        lhsCardinality * PROBE_BUILD_COST +
        rhsCardinality * PROBE_SEARCH_COST

    case _ =>
      val lhsCost = plan.lhs.map(p => apply(p, input)).getOrElse(Cost(0))
      val rhsCost = plan.rhs.map(p => apply(p, input)).getOrElse(Cost(0))
      val costForThisPlan = cardinalityForPlan(plan, input) * costPerRow(plan)
      val totalCost = costForThisPlan + lhsCost + rhsCost
      totalCost
  }

  object HashJoin {
    def unapply(x: Any): Option[(LogicalPlan, LogicalPlan)] = x match {
      case NodeHashJoin(_, l, r) => Some(l -> r)
      case OuterHashJoin(_, l, r) => Some(l -> r)
      case _ => None
    }
  }

  object ApplyVariants {
    def unapply(x: Any): Option[(LogicalPlan, LogicalPlan)] = x match {
      case Apply(l, r) => Some(l -> r)
      case p: AbstractLetSelectOrSemiApply => Some(p.lhs.get -> p.rhs.get)
      case p: AbstractSelectOrSemiApply => Some(p.lhs.get -> p.rhs.get)
      case _ => None
    }
  }
}
