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
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

/**
 * @param groupedPlans for each unique combination of available symbols, all plans that solve these symbols.
 */
case class PlansPerAvailableSymbols(groupedPlans: Iterable[Seq[LogicalPlan]])

trait LeafPlannerIterable {

  def candidates(
    qg: QueryGraph,
    f: (LogicalPlan, QueryGraph) => LogicalPlan = (plan, _) => plan,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan]
}

case class LeafPlannerList(leafPlanners: IndexedSeq[LeafPlanner]) extends LeafPlannerIterable {

  override def candidates(
    qg: QueryGraph,
    f: (LogicalPlan, QueryGraph) => LogicalPlan = (plan, _) => plan,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    leafPlanners.flatMap(_.apply(qg, interestingOrderConfig, context)).map(f(_, qg)).toSet
  }
}

case class PriorityLeafPlannerList(priority: LeafPlannerIterable, fallback: LeafPlannerIterable)
    extends LeafPlannerIterable {

  override def candidates(
    qg: QueryGraph,
    f: (LogicalPlan, QueryGraph) => LogicalPlan,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    val priorityPlans = priority.candidates(qg, f, interestingOrderConfig, context)
    if (priorityPlans.nonEmpty) priorityPlans
    else fallback.candidates(qg, f, interestingOrderConfig, context)
  }
}
