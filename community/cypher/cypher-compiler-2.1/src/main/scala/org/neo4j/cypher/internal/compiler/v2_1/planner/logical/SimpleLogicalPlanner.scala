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

import org.neo4j.cypher.internal.helpers.{Function1WithImplicit1, Function0WithImplicit1}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SimpleLogicalPlanner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LogicalPlan

case class iterateUntilConverged(f: PlanTableTransformer) extends PlanTableTransformer {
  def apply(planTable: PlanTable)(implicit context: LogicalPlanContext): PlanTable = {
    val stream = Stream.iterate(planTable)(table => f(table))
    stream.sliding(2).collectFirst {
      case pair if pair(0) == pair(1) => pair(0)
    }.get
  }
}

object SimpleLogicalPlanner {
  type PlanTableGenerator = Function0WithImplicit1[PlanTable, LogicalPlanContext]
  type PlanTransformer = Function1WithImplicit1[LogicalPlan, LogicalPlan, LogicalPlanContext]
  type PlanTableTransformer = Function1WithImplicit1[PlanTable, PlanTable, LogicalPlanContext]
  type PlanCandidateGenerator = Function1WithImplicit1[PlanTable, CandidateList, LogicalPlanContext]
  type PlanProducer[A] = Function1WithImplicit1[A, LogicalPlan, LogicalPlanContext]
  type PlanTableProducer[A] = Function1WithImplicit1[A, PlanTable, LogicalPlanContext]
  type CandidateListTransformer = Function1WithImplicit1[CandidateList, CandidateList, LogicalPlanContext]
}

class SimpleLogicalPlanner {
  def plan(implicit context: LogicalPlanContext): LogicalPlan = (
    generateLeafPlans andThen
    applySelectionsToPlanTable andThen
    iterateUntilConverged(new PlanTableTransformer {
      def apply(planTable: PlanTable)(implicit context: LogicalPlanContext): PlanTable = {
        val expanded = expand(planTable)
        val joined = join(planTable)
        val allCandidates = expanded ++ joined
        (applySelectionsToCandidateList andThen includeBestPlan(planTable))(allCandidates)
      }
    }) andThen
    iterateUntilConverged(new PlanTableTransformer {
      def apply(planTable: PlanTable)(implicit context: LogicalPlanContext): PlanTable =
        (cartesianProduct andThen applySelectionsToCandidateList andThen includeBestPlan(planTable))(planTable)
    }) andThen
    extractBestPlan andThen
    project
  )()
}
