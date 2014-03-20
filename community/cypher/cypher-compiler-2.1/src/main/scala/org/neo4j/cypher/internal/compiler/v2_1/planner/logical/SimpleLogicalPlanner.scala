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

import org.neo4j.cypher.internal.compiler.v2_1.planner._
import scala.annotation.tailrec
import org.neo4j.cypher.{SyntaxException, CypherExecutionException}

trait NodeIdentifierInitialiser {
  def apply()(implicit context: LogicalPlanContext): PlanTable
}

object MainLoop {
  def converge[A](f: (A) => A)(seed: A): A = {
    @tailrec
    def recurse(a: A, b: A): A =
      if (a == b) a else recurse(b, f(a))
    recurse(seed, f(seed))
  }
}

trait MainLoop {
  self =>

  def apply(plan: PlanTable)(implicit context: LogicalPlanContext): PlanTable

  def andThen(other: MainLoop) = new MainLoop {
    override def apply(plan: PlanTable)(implicit context: LogicalPlanContext): PlanTable =
      other.apply(self.apply(plan))
  }
}

trait ProjectionApplicator {
  def apply(plan: LogicalPlan)(implicit context: LogicalPlanContext): LogicalPlan
}

trait SelectionApplicator {
  def apply(plan: LogicalPlan)(implicit context: LogicalPlanContext): LogicalPlan
}

class SimpleLogicalPlanner(startPointFinder: NodeIdentifierInitialiser = new initialiser(applySelections),
                           mainLoop: MainLoop = new expandAndJoinLoop(applySelections)
                                                andThen new cartesianProductLoop(applySelections),
                           projector: ProjectionApplicator = projectionPlanner) {
  def plan(implicit context: LogicalPlanContext): LogicalPlan = {
    val initialPlanTable = startPointFinder()

    val bestPlan = if (initialPlanTable.isEmpty)
      SingleRow()
    else {
      val planTable = mainLoop(initialPlanTable)
      if (planTable.size != 1)
        throw new SyntaxException("Expected the final plan table to have exactly 1 plan")

      val bestPlan = planTable.plans.head
      if (!context.queryGraph.selections.coveredBy(bestPlan.solvedPredicates))
        throw new CantHandleQueryException

      bestPlan
    }

    projector(bestPlan)
  }
}
