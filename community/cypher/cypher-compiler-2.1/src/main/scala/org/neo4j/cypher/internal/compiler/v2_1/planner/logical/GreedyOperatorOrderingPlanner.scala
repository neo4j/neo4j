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

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LogicalPlan

import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged

class GreedyOperatorOrderingPlanner {
  def plan(implicit context: LogicalPlanContext): LogicalPlan = (
    (applySelections(_: PlanTable)) andThen
    iterateUntilConverged[PlanTable](planTable => (
      (applySelections(_: CandidateList)) andThen
      (includeBestPlan(planTable)(_))
    )(expand(planTable) ++ join(planTable))) andThen
    iterateUntilConverged[PlanTable](planTable => (
      (cartesianProduct(_)) andThen
      (applySelections(_: CandidateList)) andThen
      (includeBestPlan(planTable)(_))
    )(planTable)) andThen
    (extractBestPlan(_)) andThen
    (project(_))
  )(generateLeafPlans())
}
