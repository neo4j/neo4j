/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics._
import org.neo4j.cypher.internal.frontend.v3_3.ast.{HasLabels, Property}
import org.neo4j.cypher.internal.ir.v3_3._
import org.neo4j.cypher.internal.v3_3.logical.plans._

object CardinalityCostModel extends CostModel {
  def VERBOSE = java.lang.Boolean.getBoolean("CardinalityCostModel.VERBOSE")

  private val DEFAULT_COST_PER_ROW: CostPerRow = 0.1
  private val PROBE_BUILD_COST: CostPerRow = 3.1
  private val PROBE_SEARCH_COST: CostPerRow = 2.4
  private val EAGERNESS_MULTIPLIER: Multiplier = 2.0

  private def costPerRow(plan: LogicalPlan): CostPerRow = plan match {
    /*
     * These constants are approximations derived from test runs,
     * see ActualCostCalculationTest
     */

    case _: NodeByLabelScan |
         _: NodeIndexScan |
         _: ProjectEndpoints
    => 1.0

    // Filtering on labels and properties
    case Selection(predicates, _) =>
      val noOfStoreAccesses = predicates.treeCount {
        case _: Property | _: HasLabels => true
        case _ => false
      }
      if (noOfStoreAccesses > 0)
        CostPerRow(noOfStoreAccesses)
      else
        DEFAULT_COST_PER_ROW

    case _: AllNodesScan
    => 1.2

    case _: Expand |
         _: VarExpand
    => 1.5

    case _: NodeUniqueIndexSeek |
         _: NodeIndexSeek |
         _: NodeIndexContainsScan |
         _: NodeIndexEndsWithScan
    => 1.9

    case _: NodeByIdSeek |
         _: DirectedRelationshipByIdSeek |
         _: UndirectedRelationshipByIdSeek
    => 6.2

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
         _: Union |
         _: Selection |
         _: ValueHashJoin |
         _: UnwindCollection |
         _: ProcedureCall
    => DEFAULT_COST_PER_ROW

    case _: FindShortestPaths |
         _: DirectedRelationshipByIdSeek |
         _: UndirectedRelationshipByIdSeek
    => 12.0

    case _ // Default
    => DEFAULT_COST_PER_ROW
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
      case ValueHashJoin(l, r, _) => Some(l -> r)
      case _ => None
    }
  }

  object ApplyVariants {
    def unapply(x: Any): Option[(LogicalPlan, LogicalPlan)] = x match {
      case Apply(l, r) => Some(l -> r)
      case RollUpApply(l, r, _, _, _) => Some(l -> r)
      case ConditionalApply(l, r, _) => Some(l -> r)
      case AntiConditionalApply(l, r, _) => Some(l -> r)
      case ForeachApply(l, r, _, _) => Some(l -> r)
      case p: AbstractLetSelectOrSemiApply => Some(p.lhs.get -> p.rhs.get)
      case p: AbstractSelectOrSemiApply => Some(p.lhs.get -> p.rhs.get)
      case p: AbstractSemiApply => Some(p.lhs.get -> p.rhs.get)
      case p: AbstractLetSemiApply => Some(p.lhs.get -> p.rhs.get)
      case _ => None
    }
  }
}
