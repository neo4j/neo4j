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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{NewPlannerTestSupport, ExecutionEngineFunSuite, QueryStatisticsTestSupport, SyntaxException}
import org.neo4j.graphdb.{Node, Relationship}

class CreateAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("create a single node") {
    val result = updateWithBothPlanners("create ()")
    assertStats(result, nodesCreated = 1)
    // then
    result.toList shouldBe empty
  }

  test("create a single node with single label") {
    val result = updateWithBothPlanners("create (:A)")
    assertStats(result, nodesCreated = 1, labelsAdded = 1)
    // then
    result.toList shouldBe empty
  }

  test("create a single node with multiple labels") {
    val result = updateWithBothPlanners("create (n:A:B:C:D)")
    assertStats(result, nodesCreated = 1, labelsAdded = 4)
    // then
    result.toList shouldBe empty
  }

  test("combine match and create") {
    createNode()
    createNode()
    val result = updateWithBothPlanners("match (n) create ()")
    assertStats(result, nodesCreated = 2)
    // then
    result.toList shouldBe empty
  }

  test("combine match, with, and create") {
    createNode()
    createNode()
    val result = updateWithBothPlanners("match (n) create (n1) with * match(p) create (n2)")
    assertStats(result, nodesCreated = 10)
    // then
    result.toList shouldBe empty
  }


  test("should not see updates created by itself") {
    createNode()

    val result = updateWithBothPlanners("match (n) create ()")
    assertStats(result, nodesCreated = 1)
  }

  test("create a single node with properties") {
    val result = updateWithBothPlanners("create (n {prop: 'foo'}) return n.prop as p")
    assertStats(result, nodesCreated = 1, propertiesSet = 1)
    // then
    result.toList should equal(List(Map("p" -> "foo")))
  }

  test("using an undirected relationship pattern should fail on create") {
      intercept[SyntaxException](executeScalar[Relationship]("create (a {id: 2})-[r:KNOWS]-(b {id: 1}) RETURN r"))
  }

  test("create node using null properties should just ignore those properties") {
    // when
    val result = updateWithBothPlanners("create (n {id: 12, property: null}) return n.id as id")
    assertStats(result, nodesCreated = 1, propertiesSet = 1)

    // then
   result.toList should equal(List(Map("id" -> 12)))
  }

  test("create relationship using null properties should just ignore those properties") {
    // when
    val result = executeWithRulePlanner("create ()-[r:X {id: 12, property: null}]->() return r")
    val relationship = result.columnAs[Relationship]("r").next()
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesSet = 1)

    // then
    graph.inTx {
      relationship.getProperty("id") should equal(12)
    }
  }

  test("single create after with") {
    //given
    createNode()
    createNode()

    //when
    val query = "MATCH (n) CREATE() WITH * CREATE ()"
    val result = updateWithBothPlanners(query)

    //then
    assertStats(result, nodesCreated = 4)
    result should not(use("Apply"))
  }
}
