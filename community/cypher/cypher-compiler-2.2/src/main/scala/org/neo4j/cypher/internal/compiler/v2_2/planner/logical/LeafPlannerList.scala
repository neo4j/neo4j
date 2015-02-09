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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan

case class LeafPlannerList(leafPlanners: LeafPlanner*) {
  def candidates(qg: QueryGraph, f: (LogicalPlan, QueryGraph) => LogicalPlan)(implicit context: LogicalPlanningContext): Iterable[Seq[LogicalPlan]] = {
    val logicalPlans = leafPlanners.flatMap(_(qg)).map(f(_,qg))

    /*
     * The filterNot here is needed since we introduce new variables (ending with '$$$_') when using projectEndPoints
     * in order to  enforce equality with nodes already in scope.  The problem here is that if we do not filter out
     * those '$$$_-variables' when grouping, plans which differ only for such '$$$_'-variables are not grouped together
     * and so not compared by our pickBestPlan causing troubles when planning.  Particularly, we have seen better plan
     * to be discarded due to the fact that they were not grouped together with plans containing extra '$$$_'-variables
     * and so not properly compared with each other and worse later wrongly discarded since they were covering 'less'
     * (no '$$$_'-variables) wrt the other plans.
     *
     * This is definitely a sub-optimal solution but needed until projectEndPoint is changed to not introduce such
     * offending '$$$_'-variables.
     */
    logicalPlans.groupBy(_.availableSymbols.filterNot(_.name.endsWith("$$$_"))).values
  }
}
