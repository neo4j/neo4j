/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.InternalException
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._

import scala.collection.{immutable, mutable, Map}

trait PlanTable extends ((QueryGraph) => LogicalPlan) {
  def uniquePlan: LogicalPlan = {
    val allPlans = plans.toList

    if (allPlans.size > 1)
      throw new InternalException(s"Expected the final plan table to have 0 or 1 plan (got ${allPlans.size})")

    allPlans.headOption.getOrElse(planSingleRow())
  }

  def size: Int = m.size
  def isEmpty: Boolean = m.isEmpty
  def plans: Seq[LogicalPlan] = m.values.toSeq
  def get(queryGraph: QueryGraph): Option[LogicalPlan] = m.get(queryGraph)
  def apply(queryGraph: QueryGraph): LogicalPlan = m(queryGraph)
  override def toString() = m.toString()

  def +(plan: LogicalPlan): PlanTable
  def m: Map[QueryGraph, LogicalPlan]
}

object GreedyPlanTable {
  def empty: PlanTable = new GreedyPlanTable()
  def apply(plans: LogicalPlan*): PlanTable = plans.foldLeft(empty)(_ + _)

  private case class GreedyPlanTable(m: immutable.Map[QueryGraph, LogicalPlan] = immutable.Map.empty) extends PlanTable {
    override def +(newPlan: LogicalPlan): PlanTable = {
      val newSolved = newPlan.solved
      val newPlanCoveredByOldPlan = m.values.exists { p =>
        val solved = p.solved
        newSolved.graph.isCoveredBy(solved.graph) &&
          newSolved.isCoveredByHints(solved)
      }

      if (newPlanCoveredByOldPlan) {
        this
      } else {
        val oldPlansNotCoveredByNewPlan: immutable.Map[QueryGraph, LogicalPlan] = m.filter {
          case (_, existingPlan) =>
            val solved = existingPlan.solved
            !(newSolved.graph.covers(solved.graph) &&
              solved.isCoveredByHints(newSolved))
        }
        new GreedyPlanTable(oldPlansNotCoveredByNewPlan + (newPlan.solved.lastQueryGraph -> newPlan))
      }
    }

    override def toString(): String = s"PlanTable:\n${m.toString()}"
  }
}

object ExhaustivePlanTable {
  def empty: PlanTable = new ExhaustivePlanTable()
  def apply(plans: LogicalPlan*): PlanTable = plans.foldLeft(empty)(_ + _)

  private case class ExhaustivePlanTable(m: mutable.Map[QueryGraph, LogicalPlan] = mutable.Map.empty) extends PlanTable {
    override def +(newPlan: LogicalPlan): PlanTable = { m += (newPlan.solved.lastQueryGraph -> newPlan) ; this }
  }
}

