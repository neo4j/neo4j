/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._

trait LeafPlannerIterable {
  def candidates(qg: QueryGraph, f: (LogicalPlan, QueryGraph) => LogicalPlan = (plan, _) => plan )(implicit context: LogicalPlanningContext): Iterable[Seq[LogicalPlan]]
}

case class LeafPlannerList(leafPlanners: LeafPlanner*) extends LeafPlannerIterable {
  def candidates(qg: QueryGraph, f: (LogicalPlan, QueryGraph) => LogicalPlan = (plan, _) => plan )(implicit context: LogicalPlanningContext): Iterable[Seq[LogicalPlan]] = {
    val logicalPlans = leafPlanners.flatMap(_(qg)).map(f(_,qg))
    logicalPlans.groupBy(_.availableSymbols).values
  }
}

case class PriorityLeafPlannerList(priority: LeafPlannerIterable, fallback: LeafPlannerIterable) extends LeafPlannerIterable {

  override def candidates(qg: QueryGraph, f: (LogicalPlan, QueryGraph) => LogicalPlan)
                         (implicit context: LogicalPlanningContext): Iterable[Seq[LogicalPlan]] = {
    val priorityPlans = priority.candidates(qg, f)
    if (priorityPlans.nonEmpty) priorityPlans
    else fallback.candidates(qg, f)
  }
}
