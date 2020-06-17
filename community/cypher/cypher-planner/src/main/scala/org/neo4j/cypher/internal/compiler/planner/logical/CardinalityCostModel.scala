/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CostModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.ir.LazyMode
import org.neo4j.cypher.internal.logical.plans.AbstractLetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractLetSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.AbstractSemiApply
import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.CostPerRow
import org.neo4j.cypher.internal.util.Multiplier

object CardinalityCostModel extends CostModel {

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
    case Selection(predicate, _) =>
      val noOfStoreAccesses = predicate.exprs.treeCount {
        case _: Property | _: HasLabels => true
        case _ => false
      }
      if (noOfStoreAccesses > 0)
        CostPerRow(noOfStoreAccesses)
      else
        DEFAULT_COST_PER_ROW

    case _: AllNodesScan
    => 1.2

    case e: Expand if e.mode == ExpandInto
    => 4.5

    case e: VarExpand if e.mode == ExpandInto
    => 4.5

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
         _: AggregatingPlan |
         _: AbstractLetSemiApply |
         _: Limit |
         _: Optional |
         _: Argument |
         _: LeftOuterHashJoin |
         _: RightOuterHashJoin |
         _: AbstractSemiApply |
         _: Skip |
         _: Sort |
         _: Union |
         _: Selection |
         _: ValueHashJoin |
         _: UnwindCollection |
         _: ProcedureCall
    => DEFAULT_COST_PER_ROW

    case _: FindShortestPaths
    => 12.0

    case _ // Default
    => DEFAULT_COST_PER_ROW
  }

  private def cardinalityForPlan(plan: LogicalPlan, cardinalities: Cardinalities): Cardinality = plan match {
    case _ => plan.lhs.map(p => cardinalities.get(p.id)).getOrElse(cardinalities.get(plan.id))
  }

  def apply(plan: LogicalPlan, input: QueryGraphSolverInput, cardinalities: Cardinalities): Cost = {
    val cost = plan match {
      case CartesianProduct(lhs, rhs) =>
        val lhsCardinality = Cardinality.max(Cardinality.SINGLE, cardinalities.get(lhs.id))
        apply(lhs, input, cardinalities) + lhsCardinality * apply(rhs, input, cardinalities)

      case ApplyVariants(lhs, rhs) =>
        val lCost = apply(lhs, input, cardinalities)
        val rCost = apply(rhs, input, cardinalities)

        // the rCost has already been multiplied by the lhs cardinality
        lCost + rCost

      case HashJoin(lhs, rhs) =>
        val lCost = apply(lhs, input, cardinalities)
        val rCost = apply(rhs, input, cardinalities)

        val lhsCardinality = cardinalities.get(lhs.id)
        val rhsCardinality = cardinalities.get(rhs.id)

        lCost + rCost +
          lhsCardinality * PROBE_BUILD_COST +
          rhsCardinality * PROBE_SEARCH_COST

      case _ =>
        val lhsCost = plan.lhs.map(p => apply(p, input, cardinalities)).getOrElse(Cost(0))
        val rhsCost = plan.rhs.map(p => apply(p, input, cardinalities)).getOrElse(Cost(0))
        val planCardinality = cardinalityForPlan(plan, cardinalities)
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
      case LeftOuterHashJoin(_, l, r) => Some(l -> r)
      case RightOuterHashJoin(_, l, r) => Some(l -> r)
      case ValueHashJoin(l, r, _) => Some(l -> r)
      case _ => None
    }
  }

  object ApplyVariants {
    def unapply(x: Any): Option[(LogicalPlan, LogicalPlan)] = x match {
      case Apply(l, r) => Some(l -> r)
      case RollUpApply(l, r, _, _) => Some(l -> r)
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
