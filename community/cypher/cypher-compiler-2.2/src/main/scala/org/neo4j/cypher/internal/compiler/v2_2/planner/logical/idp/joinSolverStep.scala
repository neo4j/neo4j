/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.idp

import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{LogicalPlan, NodeHashJoin, PatternRelationship}

case class joinSolverStep(qg: QueryGraph) extends IDPSolverStep[PatternRelationship, LogicalPlan, LogicalPlanningContext] {

  override def apply(registry: IdRegistry[PatternRelationship], goal: Goal, table: IDPCache[LogicalPlan])
                    (implicit context: LogicalPlanningContext): Iterator[LogicalPlan] = {
    val goalSize = goal.size / 2

    val result: Iterator[Iterator[NodeHashJoin]] =
      for(
        leftGoal <- goal.subsets if leftGoal.size <= goalSize;
        lhs <- table(leftGoal);
        leftNodes = lhs.solved.graph.patternNodes -- qg.argumentIds;
        rightGoal = goal &~ leftGoal; // bit set -- operator
        rhs <- table(rightGoal);
        rightNodes = rhs.solved.graph.patternNodes;
        overlap = leftNodes intersect rightNodes if overlap.nonEmpty
      ) yield {
        Iterator(
          context.logicalPlanProducer.planNodeHashJoin(overlap, lhs, rhs),
          context.logicalPlanProducer.planNodeHashJoin(overlap, rhs, lhs)
        )
      }

    // This should be (and is) lazy
    result.flatten
  }
}

