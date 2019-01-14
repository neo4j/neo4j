/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_4.planner.logical._
import org.neo4j.cypher.internal.ir.v3_4.QueryGraph
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

import scala.annotation.tailrec

case class Selector(pickBestFactory: CandidateSelectorFactory,
                    planGenerators: CandidateGenerator[LogicalPlan]*) extends PlanTransformer[QueryGraph] {
  def apply(input: LogicalPlan, queryGraph: QueryGraph, context: LogicalPlanningContext, solveds: Solveds, cardinalities: Cardinalities): LogicalPlan = {
    val pickBest = pickBestFactory(context, solveds, cardinalities)

    @tailrec
    def selectIt(plan: LogicalPlan): LogicalPlan = {
      val plans = planGenerators.
        flatMap(generator => generator(plan, queryGraph, context, solveds, cardinalities))

      pickBest(plans) match {
        case Some(p) => selectIt(p)
        case None => plan
      }
    }

    selectIt(input)
  }
}
