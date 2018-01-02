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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy

import org.neo4j.cypher.internal.frontend.v2_3.ast.UsingJoinHint
import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{CandidateGenerator, LogicalPlanningContext}

import scala.collection.mutable.ArrayBuffer

object join extends CandidateGenerator[GreedyPlanTable] {

  import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.LogicalPlanningSupport._

  def apply(planTable: GreedyPlanTable, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {

    def isApplicable(id: IdName): Boolean =  queryGraph.patternNodes(id) && !queryGraph.argumentIds(id)

    val plans = planTable.plans
    val joinPlans = new ArrayBuffer[LogicalPlan]()
    (1 until plans.size).foreach { i =>
      val right = plans(i)
      (0 until i).foreach { j =>
        val left = plans(j)
        val shared = left.availableSymbols & right.availableSymbols

        if (shared.nonEmpty && shared.forall(isApplicable)) {

          val hints = queryGraph.hints.collect {
            case hint: UsingJoinHint if hint.coveredBy(shared) => hint
          }

          joinPlans += context.logicalPlanProducer.planNodeHashJoin(shared, left, right, hints)
          joinPlans += context.logicalPlanProducer.planNodeHashJoin(shared, right, left, hints)
        }
      }
    }

    joinPlans.toSeq
  }
}
