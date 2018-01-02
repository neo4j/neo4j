/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.frontend.v2_3.ast.{HasLabels, Property}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._

object CardinalityCostModel extends CostModel {

  /*
   * These constants are approximations derived from test runs,
   * see ActualCostCalculationTest
   */
  private val CPU_BOUND: CostPerRow = 0.1
  private val FAST_STORE: CostPerRow = 1.0
  private val SLOW_STORE: CostPerRow = 12.0
  private val PROBE_BUILD_COST: CostPerRow = 3.1
  private val PROBE_SEARCH_COST: CostPerRow = 2.4
  private val EAGERNESS_MULTIPLIER: Multiplier = 2.0

  private def costPerRow(plan: LogicalPlan): CostPerRow = plan match {

    case  _: AllNodesScan |
         _: DirectedRelationshipByIdSeek |
         _: UndirectedRelationshipByIdSeek |
         _: ProjectEndpoints
    => FAST_STORE

    case _: NodeByLabelScan => 1.6

    case _: Expand |
         _: VarExpand  => 2.5

    // Filtering on labels and properties
    case Selection(predicates, _) if predicates.exists {
      case _: Property | _: HasLabels => true
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

    case _: FindShortestPaths |
         _: LegacyIndexSeek |
         _: NodeByIdSeek |
         _: NodeUniqueIndexSeek |
         _: NodeIndexSeek |
         _: NodeIndexScan
    => SLOW_STORE

    case _
    => CPU_BOUND
  }

  private def cardinalityForPlan(plan: LogicalPlan): Cardinality = plan match {
    case Selection(_, left) => left.solved.estimatedCardinality
    case _ => plan.lhs.map(p => p.solved.estimatedCardinality).getOrElse(plan.solved.estimatedCardinality)
  }

  def apply(plan: LogicalPlan, input: QueryGraphSolverInput): Cost = {
    val cost = plan match {
      case CartesianProduct(lhs, rhs) =>
        apply(lhs, input) + lhs.solved.estimatedCardinality * apply(rhs, input)

      case ApplyVariants(lhs, rhs) =>
        val lCost = apply(lhs, input)
        val rCost = apply(rhs, input)

        // the rCost has already been multiplied by the lhs cardinality
        lCost + rCost

      case HashJoin(lhs, rhs) =>
        val lCost = apply(lhs, input)
        val rCost = apply(rhs, input)

        val lhsCardinality = lhs.solved.estimatedCardinality
        val rhsCardinality = rhs.solved.estimatedCardinality

        lCost + rCost +
          lhsCardinality * PROBE_BUILD_COST +
          rhsCardinality * PROBE_SEARCH_COST

      case _ =>
        val lhsCost = plan.lhs.map(p => apply(p, input)).getOrElse(Cost(0))
        val rhsCost = plan.rhs.map(p => apply(p, input)).getOrElse(Cost(0))
        val planCardinality = cardinalityForPlan(plan)
        val rowCost = costPerRow(plan)
        val costForThisPlan = planCardinality * rowCost
        val totalCost = costForThisPlan + lhsCost + rhsCost
        totalCost
    }

    input.strictness match {
      case Some(LazyMode) if !LazyMode(plan) => cost * EAGERNESS_MULTIPLIER
      case _ => cost
    }
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
      case p: AbstractSemiApply => Some(p.lhs.get -> p.rhs.get)
      case _ => None
    }
  }
}
