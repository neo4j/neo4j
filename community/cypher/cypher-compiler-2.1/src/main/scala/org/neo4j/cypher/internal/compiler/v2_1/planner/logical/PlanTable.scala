/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan

case class PlanTable(m: Map[Set[IdName], QueryPlan] = Map.empty) {
  def size = m.size

  def isEmpty = m.isEmpty

  def -(ids: Set[IdName]) = copy(m = m - ids)

  def +(newPlan: QueryPlan): PlanTable = {
    if (m.keys.exists(newPlan.isCoveredBy)) {
      this
    } else {
      val newMap = m.filter {
        case (_, existingPlan) => !newPlan.covers(existingPlan)
      }
      PlanTable(newMap + (newPlan.coveredIds -> newPlan))
    }
  }

  def plans: Seq[QueryPlan] = m.values.toSeq

  def uniquePlan: QueryPlan = {
    val allPlans = plans

    if (allPlans.size > 1)
      throw new InternalException(s"Expected the final plan table to have 0 or 1 plan (got ${allPlans.size})")

    allPlans.headOption.getOrElse(SingleRowPlan())
  }
}

object PlanTable {
  val empty = PlanTable()
}
