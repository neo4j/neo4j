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
import org.neo4j.cypher.internal.logical.plans.LogicalLeafPlan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
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
  def breakOn(lp: LogicalPlan, outerApplyPlanId: Id): Boolean

  def invoke(
    lp: LogicalPlan,
    slots: SlotConfiguration,
    argumentSlots: SlotConfiguration,
    outerApplyPlanId: Id
  ): SlotConfiguration =
    if (breakOn(lp, outerApplyPlanId)) {
      lp match {
        case _: AggregatingPlan => argumentSlots.copy()
        case _                  => slots.copy()
      }
    } else slots

  def nestedPlanBreakingPolicy: PipelineBreakingPolicy = this
}

object BREAK_FOR_LEAFS extends PipelineBreakingPolicy {
  override def breakOn(lp: LogicalPlan, outerApplyPlanId: Id): Boolean = lp.isInstanceOf[LogicalLeafPlan]
}

object PipelineBreakingPolicy {

  def breakFor(logicalPlans: LogicalPlan*): PipelineBreakingPolicy =
    new PipelineBreakingPolicy {
      override def breakOn(lp: LogicalPlan, outerApplyPlanId: Id): Boolean = logicalPlans.contains(lp)
    }

  def breakForIds(ids: Id*): PipelineBreakingPolicy =
    new PipelineBreakingPolicy {
      override def breakOn(lp: LogicalPlan, outerApplyPlanId: Id): Boolean = ids.contains(lp.id)
    }
}
