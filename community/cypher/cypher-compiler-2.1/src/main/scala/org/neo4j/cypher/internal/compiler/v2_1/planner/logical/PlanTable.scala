/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.InternalException
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._

case class PlanTable(m: Map[Set[IdName], QueryPlan] = Map.empty) {
  def size = m.size

  def isEmpty = m.isEmpty
  def nonEmpty = !isEmpty

  def -(ids: Set[IdName]) = copy(m = m - ids)

  def +(newPlan: QueryPlan): PlanTable = {
    val newSolved = newPlan.solved
    val newPlanCoveredByOldPlan = m.values.exists { p =>
      val solved = p.solved
      newSolved.graph.isCoveredBy(solved.graph) &&
      newSolved.isCoveredByHints(solved)
    }

    if (newPlanCoveredByOldPlan) {
      this
    } else {
      val oldPlansNotCoveredByNewPlan = m.filter {
        case (_, existingPlan) =>
          val solved = existingPlan.solved
          !(newSolved.graph.covers(solved.graph) &&
            solved.isCoveredByHints(newSolved))
      }
      PlanTable(oldPlansNotCoveredByNewPlan + (newPlan.availableSymbols -> newPlan))
    }
  }

  def plans: Seq[QueryPlan] = m.values.toSeq

  def uniquePlan: QueryPlan = {
    val allPlans = plans.toList

    if (allPlans.size > 1)
      throw new InternalException(s"Expected the final plan table to have 0 or 1 plan (got ${allPlans.size})")

    allPlans.headOption.getOrElse(planSingleRow())
  }
}

object PlanTable {
  val empty = PlanTable()
}
