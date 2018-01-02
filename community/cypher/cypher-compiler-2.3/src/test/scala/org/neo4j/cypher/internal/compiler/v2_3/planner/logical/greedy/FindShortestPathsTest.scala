/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{PatternRelationship, ShortestPathPattern, _}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport2, QueryGraph}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class FindShortestPathsTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("finds single shortest path") {
    val shortestPath = ShortestPathPattern(
      None,
      PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
      single = true
    )(null)

    new given {
      qg = QueryGraph
        .empty
        .addPatternNodes("a", "b")
        .addShortestPath(shortestPath)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val _ = ctx
      val left = CartesianProduct(AllNodesScan("a", Set.empty)(solved), AllNodesScan("b", Set.empty)(solved))(solved)
      val candidates = findShortestPaths(greedyPlanTableWith(left), cfg.qg)
      candidates should equal(Seq(
        FindShortestPaths(left, shortestPath)(solved)
      ))
    }

  }
  test("finds single named shortest path") {
    val shortestPath = ShortestPathPattern(
      Some("p"),
      PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
      single = true
    )(null)

    new given {
      qg = QueryGraph
        .empty
        .addPatternNodes("a", "b")
        .addShortestPath(shortestPath)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val _ = ctx
      val left = CartesianProduct(AllNodesScan("a", Set.empty)(solved), AllNodesScan("b", Set.empty)(solved))(solved)
      val candidates = findShortestPaths(greedyPlanTableWith(left), cfg.qg)
      candidates should equal(Seq(
        FindShortestPaths(left, shortestPath)(solved)
      ))
    }

  }

  test("finds all shortest path") {
    val shortestPath = ShortestPathPattern(
      None,
      PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
      single = false
    )(null)

    new given {
      qg = QueryGraph
        .empty
        .addPatternNodes("a", "b")
        .addShortestPath(shortestPath)
    }.withLogicalPlanningContext { (cfg, ctx) =>
      implicit val _ = ctx
      val left = CartesianProduct(AllNodesScan("a", Set.empty)(solved), AllNodesScan("b", Set.empty)(solved))(solved)
      val candidates = findShortestPaths(greedyPlanTableWith(left), cfg.qg)
      candidates should equal(Seq(
        FindShortestPaths(left, shortestPath)(solved)
      ))
    }

  }
}
