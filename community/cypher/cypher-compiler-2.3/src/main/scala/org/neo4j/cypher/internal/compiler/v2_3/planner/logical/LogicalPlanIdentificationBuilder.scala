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

import org.neo4j.cypher.internal.compiler.v2_3.planDescription.Id
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.frontend.v2_3.IdentityMap

/*
The map of logical plan and ids is used to allow profiling to connect to the right part in the logical plan
to report db hits and rows passed through.
 */
object LogicalPlanIdentificationBuilder extends (LogicalPlan => Map[LogicalPlan, Id]) {
  def apply(plan: LogicalPlan): Map[LogicalPlan, Id] = {

    def build(input: Map[LogicalPlan, Id], plan: LogicalPlan): Map[LogicalPlan, Id] = {
      val l = plan.lhs.foldLeft(input)(build)
      val r = plan.rhs.foldLeft(l)(build)
      r + (plan -> new Id)
    }

    build(IdentityMap.empty, plan)
  }
}
