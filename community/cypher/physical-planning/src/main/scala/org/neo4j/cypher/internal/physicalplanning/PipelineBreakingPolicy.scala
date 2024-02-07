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
package org.neo4j.cypher.internal.physicalplanning

import org.neo4j.cypher.internal.logical.plans.AggregatingPlan
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EagerLogicalPlan
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy.DiscardFromLhs
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy.DiscardFromRhs
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy.DiscardPolicy
import org.neo4j.cypher.internal.physicalplanning.PipelineBreakingPolicy.DoNotDiscard
import org.neo4j.cypher.internal.util.attribution.Id

/**
 * Policy that determines what parts of an operator tree belong together.
 *
 * One such part is called a Pipeline, and will have one shared slot configuration.
 */
trait PipelineBreakingPolicy {

  /**
   * True if the an operator should be the start of a new pipeline.
   */
  def breakOn(lp: LogicalPlan, applyPlans: PhysicalPlanningAttributes.ApplyPlans): Boolean

  def invoke(
    lp: LogicalPlan,
    slots: SlotConfiguration,
    argumentSlots: SlotConfiguration,
    applyPlans: PhysicalPlanningAttributes.ApplyPlans
  ): SlotConfiguration =
    if (breakOn(lp, applyPlans)) {
      lp match {
        case _: AggregatingPlan => argumentSlots.copy()
        case _                  => slots.copy()
      }
    } else slots

  def nestedPlanBreakingPolicy: PipelineBreakingPolicy = this

  /** Used to determine if an operator supports discarding */
  def discardPolicy(lp: LogicalPlan, applyPlans: PhysicalPlanningAttributes.ApplyPlans): DiscardPolicy = {
    if (breakOn(lp, applyPlans)) {
      // Only plans that break can allow discarding, discarding needs to be the last thing that happens to the data
      // before creating a new row.
      discardPolicyWhenBreaking(lp)
    } else {
      DoNotDiscard
    }
  }

  def canBeDiscardingPlan(lp: LogicalPlan): Boolean = discardPolicyWhenBreaking(lp) != DoNotDiscard

  private def discardPolicyWhenBreaking(lp: LogicalPlan): DiscardPolicy = {
    // The
    lp match {
      case eager: EagerLogicalPlan => eager match {
          case _: Eager | _: LeftOuterHashJoin | _: NodeHashJoin | _: RightOuterHashJoin | _: Sort | _: Top | _: ValueHashJoin =>
            DiscardFromLhs
          case _ => DoNotDiscard
        }
      case _: TransactionApply => DiscardFromRhs
      case _                   => DoNotDiscard
    }
  }
}

object BREAK_FOR_LEAFS extends PipelineBreakingPolicy {

  override def breakOn(lp: LogicalPlan, applyPlans: PhysicalPlanningAttributes.ApplyPlans): Boolean =
    lp.isInstanceOf[LogicalLeafPlan]
}

object PipelineBreakingPolicy {

  sealed trait DiscardPolicy
  object DoNotDiscard extends DiscardPolicy

  /** Operators that buffer the lhs have this discard policy */
  object DiscardFromLhs extends DiscardPolicy

  /** Operators that buffer the rhs have this discard policy */
  object DiscardFromRhs extends DiscardPolicy

  def breakFor(logicalPlans: LogicalPlan*): PipelineBreakingPolicy =
    new PipelineBreakingPolicy {

      override def breakOn(lp: LogicalPlan, applyPlans: PhysicalPlanningAttributes.ApplyPlans): Boolean =
        logicalPlans.contains(lp)
    }

  def breakForIds(ids: Id*): PipelineBreakingPolicy =
    new PipelineBreakingPolicy {

      override def breakOn(lp: LogicalPlan, applyPlans: PhysicalPlanningAttributes.ApplyPlans): Boolean =
        ids.contains(lp.id)
    }
}
