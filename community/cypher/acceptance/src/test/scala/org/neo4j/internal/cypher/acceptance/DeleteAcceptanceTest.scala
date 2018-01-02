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

import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}

class DeleteAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {
  test("should be able to delete nodes") {
    createNode()

    val result = execute(
      s"match (n) delete n"
    )

    assertStats(result, nodesDeleted = 1)
  }

  test("should be able to delete nodes with relationships still there") {
    createNode("foo" -> "bar")

    val result = execute(
      s"match (n) detach delete n"
    )

    assertStats(result, nodesDeleted = 1)
  }

  test("should be able to hard delete nodes and their relationships") {
    val x = createLabeledNode("X")

    relate(x, createNode())
    relate(x, createNode())
    relate(x, createNode())

    val result = execute(
      s"match (n:X) detach delete n"
    )

    assertStats(result, nodesDeleted = 1, relationshipsDeleted = 3)
  }

  test("should handle force deleting paths") {
    val x = createLabeledNode("X")
    val n1 = createNode()
    val n2 = createNode()
    val n3 = createNode()
    relate(x, n1)
    relate(n1, n2)
    relate(n2, n3)

    val result = execute(
      s"match p = (:X)-->()-->()-->() detach delete p"
    )

    assertStats(result, nodesDeleted = 4, relationshipsDeleted = 3)
  }
}