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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.ShortestPathPattern
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression
import QueryPlanProducer._

object findShortestPaths extends CandidateGenerator[PlanTable] {
  def apply(input: PlanTable, qg: QueryGraph)(implicit context: LogicalPlanningContext, subQueriesLookupTable: Map[PatternExpression, QueryGraph]): CandidateList = {

    val patterns = qg.shortestPathPatterns
    val plans = patterns.flatMap { (shortestPath: ShortestPathPattern) =>
        input.plans.collect {
          case plan if shortestPath.isFindableFrom(plan.availableSymbols) =>
            planShortestPaths(plan, shortestPath)
        }
      }
      CandidateList(plans.toSeq)
  }
}
