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

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{PatternRelationship, ShortestPathPattern, _}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps.LogicalPlanProducer._
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport2, QueryGraph}
import org.neo4j.graphdb.Direction

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
      withLogicalPlanningContext { (ctx: LogicalPlanningContext) =>
        implicit val x = ctx
        val left = planCartesianProduct(planAllNodesScan("a", Set.empty), planAllNodesScan("b", Set.empty))
        val candidates = findShortestPaths(planTableWith(left), qg)
        candidates should equal(Seq(
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

      withLogicalPlanningContext { (ctx: LogicalPlanningContext) =>
        implicit val x = ctx
        val left = planCartesianProduct(planAllNodesScan("a", Set.empty), planAllNodesScan("b", Set.empty))
        val candidates = findShortestPaths(planTableWith(left), qg)
        candidates should equal(Seq(
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
      withLogicalPlanningContext { (ctx: LogicalPlanningContext) =>
        implicit val x = ctx
        val left = planCartesianProduct(planAllNodesScan("a", Set.empty), planAllNodesScan("b", Set.empty))
        val candidates = findShortestPaths(planTableWith(left), qg)
        candidates should equal(Seq(
          planShortestPaths(left, shortestPath)
        ))
      }
    }
  }
}
