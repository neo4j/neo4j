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

object PhysicalPlanningAttributes {
  class SlotConfigurations extends Attribute[LogicalPlan, SlotConfiguration]
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
  }

  class TrailPlans extends Attribute[LogicalPlan, Id]

  class NestedPlanArgumentConfigurations extends Attribute[LogicalPlan, SlotConfiguration]
  class LiveVariables extends Attribute[LogicalPlan, Set[String]]
}
