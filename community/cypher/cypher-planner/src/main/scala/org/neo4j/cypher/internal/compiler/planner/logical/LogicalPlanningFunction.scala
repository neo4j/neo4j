/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.macros.AssertMacros

trait PlanSelector {

  def apply(
    plan: LogicalPlan,
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): LogicalPlan
}

trait PlanTransformer {
  def apply(plan: LogicalPlan, query: SinglePlannerQuery, context: LogicalPlanningContext): LogicalPlan
}

trait CandidateSelector extends ProjectingSelector[LogicalPlan]

trait LeafPlanner {

  def apply(
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan]
}

/**
 * Finds the best sorted and unsorted plan for every unique set of available symbols.
 */
trait LeafPlanFinder {

  def apply(
    config: QueryPlannerConfiguration,
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Map[Set[LogicalVariable], BestPlans]

  def apply(
    leafPlanCandidates: Set[LogicalPlan],
    config: QueryPlannerConfiguration,
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Map[Set[LogicalVariable], BestPlans]
}

sealed trait LeafPlanRestrictions {
  def symbolsThatShouldOnlyUseIndexSeekLeafPlanners: Set[LogicalVariable]
}

object LeafPlanRestrictions {

  case object NoRestrictions extends LeafPlanRestrictions {
    override def symbolsThatShouldOnlyUseIndexSeekLeafPlanners: Set[LogicalVariable] = Set.empty
  }

  /**
   * For `variable`, only plan IndexSeek, IndexContainsScan and IndexEndsWithScan.
   */
  case class OnlyIndexSeekPlansFor(variable: LogicalVariable, dependencies: Set[LogicalVariable])
      extends LeafPlanRestrictions {
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(dependencies.nonEmpty, "Dependencies must not be empty")
    override def symbolsThatShouldOnlyUseIndexSeekLeafPlanners: Set[LogicalVariable] = Set(variable)
  }

}
