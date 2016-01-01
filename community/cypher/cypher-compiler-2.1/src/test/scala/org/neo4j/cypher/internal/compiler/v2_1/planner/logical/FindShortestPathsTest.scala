/**
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryGraph, LogicalPlanningTestSupport2}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.PatternRelationship
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.ShortestPathPattern
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression
import QueryPlanProducer._

class FindShortestPathsTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("finds single shortest path") {
    val shortestPath = ShortestPathPattern(
      None,
      PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
      single = true
    )(null)

    new given {
      qg = QueryGraph
           .empty
           .addPatternNodes("a", "b")
           .addShortestPath(shortestPath)
      withLogicalPlanningContext { (ctx: LogicalPlanningContext, table: Map[PatternExpression, QueryGraph]) =>
        val left = planCartesianProduct(planAllNodesScan("a"), planAllNodesScan("b"))
        val candidates = findShortestPaths(PlanTable(Map(Set[IdName]("a", "b") -> left)), qg)(ctx, table)
        candidates should equal(Candidates(
          planShortestPaths(left, shortestPath)
        ))
      }
    }
  }
  test("finds single named shortest path") {
    val shortestPath = ShortestPathPattern(
      Some("p"),
      PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
      single = true
    )(null)

    new given {
      qg = QueryGraph
           .empty
           .addPatternNodes("a", "b")
           .addShortestPath(shortestPath)

      withLogicalPlanningContext { (ctx: LogicalPlanningContext, table: Map[PatternExpression, QueryGraph]) =>
        val left = planCartesianProduct(planAllNodesScan("a"), planAllNodesScan("b"))
        val candidates = findShortestPaths(PlanTable(Map(Set[IdName]("a", "b") -> left)), qg)(ctx, table)
        candidates should equal(Candidates(
          planShortestPaths(left, shortestPath)
        ))
      }
    }
  }

  test("finds all shortest path") {
    val shortestPath = ShortestPathPattern(
      None,
      PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength),
      single = false
    )(null)

    new given {
      qg = QueryGraph
           .empty
           .addPatternNodes("a", "b")
           .addShortestPath(shortestPath)
      withLogicalPlanningContext { (ctx: LogicalPlanningContext, table: Map[PatternExpression, QueryGraph]) =>
        val left = planCartesianProduct(planAllNodesScan("a"), planAllNodesScan("b"))
        val candidates = findShortestPaths(PlanTable(Map(Set[IdName]("a", "b") -> left)), qg)(ctx, table)
        candidates should equal(Candidates(
          planShortestPaths(left, shortestPath)
        ))
      }
    }
  }
}
