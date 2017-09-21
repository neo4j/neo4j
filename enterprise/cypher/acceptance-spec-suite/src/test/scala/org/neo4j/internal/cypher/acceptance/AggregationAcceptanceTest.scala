/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Versions.{V2_3, V3_2}

class AggregationAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  // Non-deterministic query -- needs TCK design
  test("should aggregate using as grouping key expressions using variables in scope and nothing else") {
    val userId = createLabeledNode(Map("userId" -> 11), "User")
    relate(userId, createNode(), "FRIEND", Map("propFive" -> 1))
    relate(userId, createNode(), "FRIEND", Map("propFive" -> 3))
    relate(createNode(), userId, "FRIEND", Map("propFive" -> 2))
    relate(createNode(), userId, "FRIEND", Map("propFive" -> 4))

    val query1 = """MATCH (user:User {userId: 11})-[friendship:FRIEND]-()
                   |WITH user, collect(friendship)[toInt({param} * count(friendship))] AS selectedFriendship
                   |RETURN id(selectedFriendship) AS friendshipId, selectedFriendship.propFive AS propertyValue""".stripMargin
    val query2 = """MATCH (user:User {userId: 11})-[friendship:FRIEND]-()
                   |WITH user, collect(friendship) AS friendships
                   |WITH user, friendships[toInt({param} * size(friendships))] AS selectedFriendship
                   |RETURN id(selectedFriendship) AS friendshipId, selectedFriendship.propFive AS propertyValue""".stripMargin
    val params = Map("param" -> 3)

    val result1 = executeWith(Configs.Interpreted, query1, params = params).toList
    val result2 = executeWith(Configs.Interpreted, query2, params = params).toList

    result1.size should equal(result2.size)
  }

  test("distinct aggregation on single node") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1, node2)
    relate(node2, node1)
    val result = executeWith(Configs.All, "MATCH (a)--() RETURN DISTINCT a")

    result.toList should equal(List(Map("a" -> node1), Map("a" -> node2)))
  }

  test("distinct aggregation on array property") {
    createNode("prop"-> Array(42))
    createNode("prop"-> Array(42))
    createNode("prop"-> Array(1337))
    val result = executeWith(Configs.All, "MATCH (a) RETURN DISTINCT a.prop")

    result.toComparableResult.toSet should equal(Set(Map("a.prop" -> List(1337)), Map("a.prop" -> List(42))))
  }

  test("Node count from count store plan should work with labeled nodes") {
    val node1 = createLabeledNode("Person")
    val node2 = createLabeledNode("Person")
    val node3 = createNode()
    // CountStore not supported by sloted
    val result = executeWith(Configs.AllExceptSlotted, "MATCH (a:Person) WITH count(a) as c RETURN c")
    result.toList should equal(List(Map("c" -> 2L)))
  }

  test("Count should work with projected node variable") {
    val node1 = createLabeledNode("Person")
    val node2 = createLabeledNode("Person")
    val node3 = createNode()
    // This does not use countstore
    val result = executeWith(Configs.All, "MATCH (a:Person) WITH a as b WITH count(b) as c RETURN c")
    result.toList should equal(List(Map("c" -> 2L)))
  }

  test("Count should work with projected relationship variable") {
    val node1 = createLabeledNode("Person")
    val node2 = createNode()
    val node3 = createNode()
    val r1 = relate(node1, node2)
    val r2 = relate(node1, node3)

    val result = executeWith(Configs.All, "MATCH (a:Person)-[r]->() WITH r as s WITH count(s) as c RETURN c")
    result.toList should equal(List(Map("c" -> 2L)))
  }

  test("combine grouping and aggregation with sorting") {
    val node1 = createNode(Map("prop" -> 1))
    val node2 = createNode(Map("prop" -> 2))
    val r1 = relate(node1, node2)

    val result = executeWith(Configs.All, "MATCH (a)--(b) RETURN a.prop, count(a) ORDER BY a.prop")
    result.toList should equal(List(Map("a.prop" -> 1, "count(a)" -> 1), Map("a.prop" -> 2, "count(a)" -> 1)))
  }

  test("combine simple aggregation on projection with sorting") {
    val node1 = createNode()
    val node2 = createNode()
    val result = executeWith(Configs.All, "MATCH (a) WITH a as b RETURN count(b) ORDER BY count(b)")
    result.toList should equal(List(Map("count(b)" -> 2)))
  }

  test("combine simple aggregation with sorting (cannot use count store)") {
    val node1 = createNode(Map("prop" -> 1))
    val node2 = createNode(Map("prop" -> 2))
    val result = executeWith(Configs.All, "MATCH (a) RETURN count(a.prop) ORDER BY count(a.prop)")
    result.toList should equal(List(Map("count(a.prop)" -> 2)))
  }

  test("combine simple aggregation with sorting (can use node count store)") {
    val node1 = createNode()
    val node2 = createNode()
    val result = executeWith(Configs.AllExceptSlotted, "MATCH (a) RETURN count(a) ORDER BY count(a)")
    result.toList should equal(List(Map("count(a)" -> 2)))
  }

  test("combine simple aggregation with sorting (can use relationship count store)") {
    val node1 = createNode()
    val node2 = createNode()
    val r1 = relate(node1, node2)
    val result = executeWith(Configs.All, "MATCH (a)-[r]-(b) RETURN count(r) ORDER BY count(r)")
    result.toList should equal(List(Map("count(r)" -> 2)))
  }

  test("should support DISTINCT followed by LIMIT and SKIP") {
    val node1 = createNode(Map("prop" -> 1))
    val node2 = createNode(Map("prop" -> 2))
    val query = "MATCH (a) RETURN DISTINCT a ORDER BY a.prop SKIP 1 LIMIT 1"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("a" -> node2)))
  }

  test("should support DISTINCT projection followed by LIMIT and SKIP") {
    val node1 = createNode(Map("prop" -> 1))
    val node2 = createNode(Map("prop" -> 2))
    val query = "MATCH (a) RETURN DISTINCT a.prop ORDER BY a.prop SKIP 1 LIMIT 1"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("a.prop" -> 2)))
  }

  test("should support DISTINCT projection followed by SKIP") {
    val node1 = createNode(Map("prop" -> 1))
    val node2 = createNode(Map("prop" -> 2))
    val query = "MATCH (a) RETURN DISTINCT a.prop ORDER BY a.prop SKIP 1"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("a.prop" -> 2)))
  }

  test("should support DISTINCT projection followed by LIMIT") {
    val node1 = createNode(Map("prop" -> 1))
    val node2 = createNode(Map("prop" -> 2))
    val query = "MATCH (a) RETURN DISTINCT a.prop ORDER BY a.prop LIMIT 1"

    val result = executeWith(Configs.All, query)

    result.toList should equal(List(Map("a.prop" -> 1)))
  }

  test("should support DISTINCT followed by LIMIT and SKIP with no ORDER BY") {
    val node1 = createNode(Map("prop" -> 1))
    val node2 = createNode(Map("prop" -> 2))
    val query = "MATCH (a) WITH DISTINCT a SKIP 1 LIMIT 1 RETURN count(a)"

    val result = executeWith(Configs.Interpreted, query)

    result.toList should equal(List(Map("count(a)" -> 1)))
  }
}
