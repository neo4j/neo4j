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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp.joinSolverStep._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LogicalPlan, PatternRelationship}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanningContext, LogicalPlanningSupport}

object joinSolverStep {
  val VERBOSE = false
}

case class joinSolverStep(qg: QueryGraph) extends IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext] {

  import LogicalPlanningSupport._

  override def apply(registry: IdRegistry[PatternRelationship], goal: Goal, table: IDPCache[LogicalPlan])
                    (implicit context: LogicalPlanningContext): Iterator[LogicalPlan] = {

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
        val overlappingNodes = computeOverlappingNodes(lhs, rhs, arguments)
        if (overlappingNodes.nonEmpty) {
          val overlappingSymbols = computeOverlappingSymbols(lhs, rhs, arguments)
          if (overlappingSymbols == overlappingNodes) {
            if (VERBOSE) {
              println(s"${show(leftGoal, nodes(lhs))} overlap ${show(rightGoal, nodes(rhs))} on ${showNames(overlappingNodes)}")
            }
            // This loop is designed to find both LHS and RHS plans, so no need to generate them swapped here
            val matchingHints = qg.joinHints.filter(_.coveredBy(overlappingNodes))
            builder += planProducer.planNodeHashJoin(overlappingNodes, lhs, rhs, matchingHints)
          }
        }
      }
    }

    builder.result().iterator
  }

  private def computeOverlappingNodes(lhs: LogicalPlan, rhs: LogicalPlan, arguments: Set[IdName]): Set[IdName] = {
    val leftNodes = nodes(lhs)
    val rightNodes = nodes(rhs)
    (leftNodes intersect rightNodes) -- arguments
  }

  private def computeOverlappingSymbols(lhs: LogicalPlan, rhs: LogicalPlan, arguments: Set[IdName]): Set[IdName] = {
    val leftSymbols = lhs.availableSymbols
    val rightSymbols = rhs.availableSymbols
    (leftSymbols intersect rightSymbols) -- arguments
  }

  private def nodes(plan: LogicalPlan) =
    plan.solved.graph.patternNodes

  private def show(goal: Goal, symbols: Set[IdName]) =
    s"${showIds(goal.toSet)}: ${showNames(symbols)}"

  private def goalSymbols(goal: Goal, registry: IdRegistry[PatternRelationship]) =
    registry.explode(goal).flatMap(_.coveredIds)

  private def showIds(ids: Set[Int]) =
    ids.toSeq.sorted.mkString("{", ", ", "}")

  private def showNames(ids: Set[IdName]) =
    ids.map(_.name).toSeq.sorted.mkString("[", ", ", "]")
}
