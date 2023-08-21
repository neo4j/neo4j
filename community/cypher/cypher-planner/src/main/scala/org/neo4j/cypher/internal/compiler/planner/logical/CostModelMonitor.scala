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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost

trait CostModelMonitor {
  def reportPlanCost(rootPlan: LogicalPlan, plan: LogicalPlan, cost: Cost): Unit

  def reportPlanEffectiveCardinality(rootPlan: LogicalPlan, plan: LogicalPlan, cardinality: Cardinality): Unit
}

object CostModelMonitor {

  val DEFAULT: CostModelMonitor = new CostModelMonitor {
    override def reportPlanCost(rootPlan: LogicalPlan, plan: LogicalPlan, cost: Cost): Unit = {}

    override def reportPlanEffectiveCardinality(
      rootPlan: LogicalPlan,
      plan: LogicalPlan,
      cardinality: Cardinality
    ): Unit = {}
  }
}
