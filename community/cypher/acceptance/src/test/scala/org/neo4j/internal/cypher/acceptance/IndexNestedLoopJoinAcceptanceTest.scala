/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.Node

class IndexNestedLoopJoinAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {
  test("test that index seek is planned on the RHS using information from the LHS") {
    // given

    def relateToThreeNodes(n: Node) = {
      relate(n, createNode("id" -> 1))
      relate(n, createNode("id" -> 2))
      relate(n, createNode("id" -> 3))
    }

    (0 to 100) foreach (i => relateToThreeNodes(createLabeledNode(Map("id" -> i), "A")))
    (0 to 100) foreach (i => createLabeledNode(Map("id" -> i), "C"))

    graph.createIndex("A", "id")
    graph.createIndex("C", "id")

    // when
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:A)-->(b), (c:C) WHERE b.id = c.id AND a.id = 42 RETURN count(*)")

    result.toList should equal(List(Map("count(*)" -> 3)))

    // then
    result should use("Apply", "NodeIndexSeek")
    result should not(use("ValueHashJoin", "CartesianProduct", "NodeByLabelScan", "Filter"))
  }

  test("index seek planned in the presence of optional matches") {
    // given

    def relateToThreeNodes(n: Node) = {
      relate(n, createNode("id" -> 1))
      relate(n, createNode("id" -> 2))
      relate(n, createNode("id" -> 3))
    }

    (0 to 100) foreach (i => relateToThreeNodes(createLabeledNode(Map("id" -> i), "A")))
    (0 to 100) foreach (i => createLabeledNode(Map("id" -> i), "C"))

    graph.createIndex("A", "id")
    graph.createIndex("C", "id")

    // when
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:A)-->(b), (c:C) WHERE b.id = c.id AND a.id = 42 OPTIONAL MATCH (a)-[:T]->() RETURN count(*)")

    result.toList should equal(List(Map("count(*)" -> 3)))

    // then
    result should use("Apply", "NodeIndexSeek")
    result should not(use("ValueHashJoin", "CartesianProduct", "NodeByLabelScan", "Filter"))
  }
}
