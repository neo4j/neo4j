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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}

class MergeNodeAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  // TODO: Reflect something like this in the TCK
  test("multiple merges after each other") {
    1 to 100 foreach { prop =>
      val result = updateWithBothPlannersAndCompatibilityMode(s"merge (a:Label {prop: $prop}) return a.prop")
      assertStats(result, nodesCreated = 1, propertiesWritten = 1, labelsAdded = 1)
    }
  }

  test("Merging with self loop and relationship uniqueness") {
    graph.execute("CREATE (a) CREATE (a)-[:X]->(a)")
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (a)-[:X]->(b)-[:X]->(c) RETURN 42")
    assertStats(result, relationshipsCreated = 2, nodesCreated = 3)
  }

  test("Merging with self loop and relationship uniqueness - no stats") {
    graph.execute("CREATE (a) CREATE (a)-[:X]->(a)")
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (a)-[r1:X]->(b)-[r2:X]->(c) RETURN id(r1) = id(r2) as sameEdge")
    result.columnAs[Boolean]("sameEdge").toList should equal(List(false))
  }

  test("Merging with self loop and relationship uniqueness - no stats - reverse direction") {
    graph.execute("CREATE (a) CREATE (a)-[:X]->(a)")
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (a)-[r1:X]->(b)<-[r2:X]-(c) RETURN id(r1) = id(r2) as sameEdge")
    result.columnAs[Boolean]("sameEdge").toList should equal(List(false))
  }

  test("Merging with non-self-loop but require relationship uniqueness") {
    val a = createLabeledNode(Map("name" -> "a"), "A")
    val b = createLabeledNode(Map("name" -> "b"), "B")
    relate(a, b, "X")
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (a)-[r1:X]->(b)<-[r2:X]-(c) RETURN id(r1) = id(r2) as sameEdge, c.name as name")
    result.toList should equal(List(Map("sameEdge" -> false, "name" -> null)))
  }
}
