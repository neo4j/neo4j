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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{CandidateSelector, LogicalPlanningContext}
import org.neo4j.helpers.Clock
import org.neo4j.kernel.monitoring.Monitors

object pickBestPlan extends CandidateSelector {
  private final val VERBOSE = true

  def apply(plans: Seq[LogicalPlan])(implicit context: LogicalPlanningContext): Option[LogicalPlan] = {

    val costs = context.cost
    val comparePlans = (c: LogicalPlan) =>
      (-c.solved.numHints, costs(c, context.cardinalityInput), -c.availableSymbols.size)

    if (VERBOSE) {
      import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.PipeExecutionPlanBuilder
      import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.PipeExecutionBuilderContext
      val pipeBuilderContext = new PipeExecutionBuilderContext(context.cardinality, context.semanticTable)
      val pipeBuilder = new PipeExecutionPlanBuilder(Clock.SYSTEM_CLOCK, new v2_2.Monitors(new Monitors()))

      def printPlan(plan: LogicalPlan): Unit = {
        val pipe = pipeBuilder.build(plan)(pipeBuilderContext, context.planContext).pipe
        println("* " + plan.toString + s"\n${costs(plan, context.cardinalityInput)}\n")
        println(pipe.planDescription)
      }

      val sortedPlans = plans.sortBy(comparePlans)

      if (sortedPlans.size > 1) {
        println("Get best of:")
        for (plan <- sortedPlans) {
          printPlan(plan)
        }

        println("Best is:")
        println(sortedPlans.head.toString)
        println("--------\n")
      }

      sortedPlans.headOption
    } else {
      if (plans.isEmpty) None else Some(plans.minBy(comparePlans))
    }
  }
}
