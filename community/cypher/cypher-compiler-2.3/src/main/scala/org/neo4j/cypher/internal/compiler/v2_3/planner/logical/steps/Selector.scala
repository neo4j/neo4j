/*
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{LogicalPlanningContext, CandidateSelector, LogicalPlanningFunction0, PlanTransformer}

import scala.annotation.tailrec

case class Selector(selectionSteps: Seq[PlanTransformer[QueryGraph]],
                    pickBestFactory: LogicalPlanningFunction0[CandidateSelector]) extends PlanTransformer[QueryGraph] {
  def apply(input: LogicalPlan, queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val pickBest = pickBestFactory(context)

    @tailrec
    def selectIt(plan: LogicalPlan): LogicalPlan = {
      val plans = selectionSteps.
        map(step => step(plan, queryGraph)).
        filterNot(p => p.solved == plan.solved)

      pickBest(plans) match {
        case Some(p) => selectIt(p)
        case None => plan
      }
    }

    selectIt(input)
  }
}