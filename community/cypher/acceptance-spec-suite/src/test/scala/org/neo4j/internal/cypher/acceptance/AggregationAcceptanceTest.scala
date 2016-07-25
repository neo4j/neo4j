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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

// TODO: Move to openCypher
class AggregationAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should aggregate using as grouping key expressions using variables in scope and nothing else") {
    val userId = createLabeledNode(Map("userId" -> 11), "User")
    relate(userId, createNode(), "FRIEND", Map("propFive" -> 1))
    relate(userId, createNode(), "FRIEND", Map("propFive" -> 3))
    relate(createNode(), userId, "FRIEND", Map("propFive" -> 2))
    relate(createNode(), userId, "FRIEND", Map("propFive" -> 4))

    val query1 = """MATCH (user:User {userId: 11})-[friendship:FRIEND]-()
                   |WITH user, collect(friendship)[toInt(rand() * count(friendship))] AS selectedFriendship
                   |RETURN id(selectedFriendship) AS friendshipId, selectedFriendship.propFive AS propertyValue""".stripMargin
    val query2 = """MATCH (user:User {userId: 11})-[friendship:FRIEND]-()
                   |WITH user, collect(friendship) AS friendships
                   |WITH user, friendships[toInt(rand() * size(friendships))] AS selectedFriendship
                   |RETURN id(selectedFriendship) AS friendshipId, selectedFriendship.propFive AS propertyValue""".stripMargin

    // TODO: this can be executed with the compatibility mode when we'll depend on the 2.3.4 cypher-compiler
    val result1 = executeWithCostPlannerOnly(query1).toList
    val result2 = executeWithCostPlannerOnly(query2).toList

    result1.size should equal(result2.size)
  }

  test("max() should aggregate strings") {
    val query = "UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i RETURN max(i)"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("max(i)" -> "b")))
  }

  test("min() should aggregate strings") {
    val query = "UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i RETURN min(i)"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("min(i)" -> "B")))
  }
}
