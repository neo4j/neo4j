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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.greedy

import org.neo4j.cypher.internal.compiler.v3_0.planner.{Predicate, QueryGraph}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.{LogicalPlan, ShortestPathPattern}
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.{CandidateGenerator, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v3_0.ast.Expression

object findShortestPaths extends CandidateGenerator[GreedyPlanTable] {
  def apply(input: GreedyPlanTable, qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] =
    qg.shortestPathPatterns.flatMap { (shortestPath: ShortestPathPattern) =>
      input.plans.collect {
        case plan if shortestPath.isFindableFrom(plan.availableSymbols) =>
          val pathVariables = Set(shortestPath.name, Some(shortestPath.rel.name)).flatten
          val pathPredicates = qg.selections.predicates.collect {
            case Predicate(dependencies, expr: Expression) if (dependencies intersect pathVariables).nonEmpty => expr
          }.toSeq
          context.logicalPlanProducer.planShortestPaths(plan, shortestPath, pathPredicates, pathVariables)
      }
    }.toSeq
}
