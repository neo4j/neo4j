/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.idp.joinSolverStep._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.{LogicalPlanningContext, LogicalPlanningSupport}
import org.neo4j.cypher.internal.ir.v3_5.{PatternRelationship, QueryGraph}
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan

object joinSolverStep {
  val VERBOSE = false
}

case class joinSolverStep(qg: QueryGraph) extends IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext] {

  import LogicalPlanningSupport._

  override def apply(registry: IdRegistry[PatternRelationship],
                     goal: Goal,
                     table: IDPCache[LogicalPlan],
                     context: LogicalPlanningContext, solveds: Solveds): Iterator[LogicalPlan] = {

    if (VERBOSE) {
      println(s"\n>>>> start solving ${show(goal, goalSymbols(goal, registry))}")
    }

    val goalSize = goal.size
    val arguments = qg.argumentIds
    val planProducer = context.logicalPlanProducer
    val builder = Vector.newBuilder[LogicalPlan]

    for (
      leftSize <- 1.until(goalSize);
      leftGoal <- goal.subsets(leftSize);
      rightSize <- 1.until(goalSize);
      rightGoal <- goal.subsets(rightSize) if (leftGoal != rightGoal) && ((leftGoal | rightGoal) == goal)
    ) {
      val optLhs = table(leftGoal)
      val optRhs = table(rightGoal)
      if (optLhs.isDefined && optRhs.isDefined) {
        val lhs = optLhs.get
        val rhs = optRhs.get
        val overlappingNodes = computeOverlappingNodes(lhs, rhs, solveds, arguments)
        if (overlappingNodes.nonEmpty) {
          val overlappingSymbols = computeOverlappingSymbols(lhs, rhs, arguments)
          if (overlappingSymbols == overlappingNodes) {
            if (VERBOSE) {
              println(s"${show(leftGoal, nodes(lhs, solveds))} overlap ${show(rightGoal, nodes(rhs, solveds))} on ${showNames(overlappingNodes)}")
            }
            // This loop is designed to find both LHS and RHS plans, so no need to generate them swapped here
            val matchingHints = qg.joinHints.filter(_.coveredBy(overlappingNodes))
            builder += planProducer.planNodeHashJoin(overlappingNodes, lhs, rhs, matchingHints, context)
          }
        }
      }
    }

    builder.result().iterator
  }

  private def computeOverlappingNodes(lhs: LogicalPlan, rhs: LogicalPlan, solveds: Solveds, arguments: Set[String]): Set[String] = {
    val leftNodes = nodes(lhs, solveds)
    val rightNodes = nodes(rhs, solveds)
    (leftNodes intersect rightNodes) -- arguments
  }

  private def computeOverlappingSymbols(lhs: LogicalPlan, rhs: LogicalPlan, arguments: Set[String]): Set[String] = {
    val leftSymbols = lhs.availableSymbols
    val rightSymbols = rhs.availableSymbols
    (leftSymbols intersect rightSymbols) -- arguments
  }

  private def nodes(plan: LogicalPlan, solveds: Solveds) =
    solveds.get(plan.id).queryGraph.patternNodes

  private def show(goal: Goal, symbols: Set[String]) =
    s"${showIds(goal.toSet)}: ${showNames(symbols)}"

  private def goalSymbols(goal: Goal, registry: IdRegistry[PatternRelationship]) =
    registry.explode(goal).flatMap(_.coveredIds)

  private def showIds(ids: Set[Int]) =
    ids.toIndexedSeq.sorted.mkString("{", ", ", "}")

  private def showNames(ids: Set[String]) =
    ids.toIndexedSeq.sorted.mkString("[", ", ", "]")
}
