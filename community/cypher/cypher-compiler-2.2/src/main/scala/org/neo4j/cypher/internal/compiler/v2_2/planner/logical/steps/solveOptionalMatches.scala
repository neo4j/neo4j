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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.solveOptionalMatches.OptionalSolver
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{CandidateList, LogicalPlanningContext, LogicalPlanningFunction2, PlanTable}

import scala.annotation.tailrec

case class solveOptionalMatches(solvers: Seq[OptionalSolver]) {
  def apply(planTable: PlanTable, qg: QueryGraph)(implicit context: LogicalPlanningContext): PlanTable = {

    val p = if (planTable.isEmpty)
      PlanTable(planSingleRow())
    else
      planTable

    p.plans.foldLeft(planTable) {
      case (table: PlanTable, plan: LogicalPlan) =>
        val optionalQGs: Seq[QueryGraph] = findQGsToSolve(plan, table, qg.optionalMatches)
        val newPlan = optionalQGs.foldLeft(plan) {
          case (lhs: LogicalPlan, optionalQg: QueryGraph) =>
            val plans = solvers.flatMap(_.apply(optionalQg, lhs))
            CandidateList(plans).bestPlan(context.cost).get
        }
        table + newPlan
    }
  }

  private def findQGsToSolve(plan: LogicalPlan, table: PlanTable, graphs: Seq[QueryGraph]): Seq[QueryGraph] = {

    @tailrec
    def inner(in: Seq[QueryGraph], out: Seq[QueryGraph]): Seq[QueryGraph] = in match {
      case hd :: tl if isSolved(table, hd)  => inner(tl, out)
      case hd :: tl if applicable(plan, hd) => inner(tl, out :+ hd)
      case _                                => out
    }

    inner(graphs, Seq.empty)
  }

  private def isSolved(table: PlanTable, optionalQG: QueryGraph) =
    table.plans.exists(_.solved.lastQueryGraph.optionalMatches.contains(optionalQG))

  private def applicable(outerPlan: LogicalPlan, optionalQG: QueryGraph) =
    optionalQG.argumentIds.subsetOf(outerPlan.availableSymbols)
}

object solveOptionalMatches extends LogicalPlanningFunction2[PlanTable, QueryGraph, PlanTable] {

  type OptionalSolver = LogicalPlanningFunction2[QueryGraph, LogicalPlan, Option[LogicalPlan]]

  private val optionalStrategies: Seq[OptionalSolver] = Seq(
    applyOptional,
    outerHashJoin
  )

  private val instance = new solveOptionalMatches(optionalStrategies)

  def apply(pt: PlanTable, qg: QueryGraph)(implicit context: LogicalPlanningContext) =
    instance.apply(pt, qg)
}

case object applyOptional extends OptionalSolver {
  def apply(optionalQg: QueryGraph, lhs: LogicalPlan)(implicit context: LogicalPlanningContext) =
    Some(planApply(lhs, planOptional(context.strategy.plan(optionalQg))))
}

case object outerHashJoin extends OptionalSolver {
  def apply(optionalQg: QueryGraph, lhs: LogicalPlan)(implicit context: LogicalPlanningContext) = {
    val joinNodes = optionalQg.argumentIds
    val rhs = context.strategy.plan(optionalQg.withoutArguments())

    if (joinNodes.nonEmpty &&
      joinNodes.forall(lhs.availableSymbols) &&
      joinNodes.forall(optionalQg.patternNodes)) {
      Some(planOuterHashJoin(joinNodes, lhs, rhs))
    } else {
      None
    }
  }
}
