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

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration.Size
import org.neo4j.cypher.internal.util.attribution.Attribute
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.ImmutableAttribute

object PhysicalPlanningAttributes {

  class SlotConfigurations extends BuildsSlotConfigurations
  class ArgumentSizes extends Attribute[LogicalPlan, Size]

  class ApplyPlans extends Attribute[LogicalPlan, Id] {

    def isInOutermostScope(plan: LogicalPlan): Boolean = {
      val applyPlanId = apply(plan.id)
      if (applyPlanId == Id.INVALID_ID)
        true
      else if (applyPlanId == plan.id)
        apply(plan.leftmostLeaf.id) == Id.INVALID_ID
      else
        false
    }

    def getOuterApplyPlanId(plan: LogicalPlan): Id = {
      val id = apply(plan.id)
      if (id == plan.id)
        apply(plan.leftmostLeaf.id)
      else id
    }
  }

  class TransactionApplyPlans extends Attribute[LogicalPlan, Id] {

    def getOuterApplyPlanId(plan: LogicalPlan): Id = {
      val id = apply(plan.id)
      if (id == plan.id)
        apply(plan.leftmostLeaf.id)
      else id
    }
  }

  class TrailPlans extends Attribute[LogicalPlan, Id]

  class AcyclicPlans extends Attribute[LogicalPlan, Id]

  class NestedPlanArgumentConfigurations extends BuildsSlotConfigurations
  class LiveVariables extends Attribute[LogicalPlan, Set[String]]

  trait BuildsSlotConfigurations extends Attribute[LogicalPlan, SlotConfigurationBuilder] {

    def finalizeSlots(): ImmutableAttribute[SlotConfiguration] = {
      ImmutableAttribute.deduplicated(iterator.map { case (id, s) => id -> s.build() })
    }
  }
}
