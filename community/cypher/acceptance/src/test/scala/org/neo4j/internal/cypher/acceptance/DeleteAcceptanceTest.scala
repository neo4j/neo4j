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

import org.neo4j.cypher._
import org.scalatest.concurrent.TimeLimitedTests
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import scala.language.postfixOps

class DeleteAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport with TimeLimitedTests {
  test("should be able to delete nodes") {
    createNode()

    val result = updateWithBothPlannersAndCompatibilityMode(
      s"match (n) delete n"
    )

    assertStats(result, nodesDeleted = 1)
  }

  test("should be able to delete relationships") {
    relate(createNode(), createNode())
    relate(createNode(), createNode())
    relate(createNode(), createNode())

    val result = updateWithBothPlannersAndCompatibilityMode(
      s"match ()-[r]-() delete r"
    )

    assertStats(result, relationshipsDeleted = 3)
  }

  test("should be able to detach delete node") {
    createNode("foo" -> "bar")

    val result = updateWithBothPlannersAndCompatibilityMode(
      s"match (n) detach delete n"
    )

    assertStats(result, nodesDeleted = 1)
  }

  test("should not be able to delete nodes when connected") {
    val x = createLabeledNode("X")

    relate(x, createNode())
    relate(x, createNode())
    relate(x, createNode())

    a [ConstraintValidationException] should be thrownBy
      executeWithCostPlannerOnly(s"match (n:X) delete n")
  }

  test("should be able to detach delete nodes and their relationships") {
    val x = createLabeledNode("X")

    relate(x, createNode())
    relate(x, createNode())
    relate(x, createNode())

    val result = updateWithBothPlannersAndCompatibilityMode(
      s"match (n:X) detach delete n"
    )

    assertStats(result, nodesDeleted = 1, relationshipsDeleted = 3)
  }

  test("should handle detach delete paths") {
    val x = createLabeledNode("X")
    val n1 = createNode()
    val n2 = createNode()
    val n3 = createNode()
    relate(x, n1)
    relate(n1, n2)
    relate(n2, n3)

    val result = updateWithBothPlannersAndCompatibilityMode(
      s"match p = (:X)-->()-->()-->() detach delete p"
    )

    assertStats(result, nodesDeleted = 4, relationshipsDeleted = 3)
  }

  test("undirected expand followed by delete and count") {
    relate(createNode(), createNode())

    val result = updateWithBothPlanners(s"MATCH (a)-[r]-(b) DELETE r,a,b RETURN count(*) AS c")

    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 1)

    result.toList should equal(List(Map("c" -> 2)))
  }

  test("undirected variable length expand followed by delete and count") {
    val node1 = createNode()
    val node2 = createNode()
    val node3 = createNode()
    relate(node1, node2)
    relate(node2, node3)

    val result = executeWithCostPlannerOnly(s"MATCH (a)-[*]-(b) DETACH DELETE a,b RETURN count(*) AS c")
    assertStats(result, nodesDeleted = 3, relationshipsDeleted = 2)

    //(1)-->(2), (2)<--(1), (2)-->(3), (3)<--(2), (1)-*->(3), (3)<-*-(1)
    result.toList should equal(List(Map("c" -> 6)))
  }

  test("should be possible to create and delete in one statement") {
    createNode()

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH () CREATE (n) DELETE n")

    assertStats(result, nodesCreated = 1, nodesDeleted = 1)
  }

  test("should be able to delete on optional match relationship") {
    createNode()

    val query = "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r"
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesDeleted = 1)
  }

  test("should be able to handle detach deleting null node") {
    val query = "OPTIONAL MATCH (n) DETACH DELETE n"
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesDeleted = 0)
  }

  test("should be able to handle deleting null node") {
    val query = "OPTIONAL MATCH (n) DELETE n"
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesDeleted = 0)
  }

  test("should be able to handle deleting null path") {
    val query = "OPTIONAL MATCH p = (n)-->() DETACH DELETE p"
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesDeleted = 0)
  }

  test("should be able to delete a node from a collection") {
    // Given
    val user = createLabeledNode("User")
    (0 to 3).foreach(_ => relate(user, createNode(), "FRIEND"))

    // When
    val result = updateWithBothPlannersAndCompatibilityMode(
      """ MATCH (:User)-[:FRIEND]->(n)
        | WITH collect(n) AS friends
        | DETACH DELETE friends[{friendIndex}]""".stripMargin,
      "friendIndex" -> 1)

    // Then (no exception, and correct result)
    assertStats(result, nodesDeleted = 1, relationshipsDeleted = 1)
  }

  test("should be able to delete a relationship from a collection") {
    // Given
    val user = createLabeledNode("User")
    (0 to 3).foreach(_ => relate(user, createNode(), "FRIEND"))

    // When
    val result = updateWithBothPlannersAndCompatibilityMode(
      """ MATCH (:User)-[r:FRIEND]->()
        | WITH collect(r) AS friendships
        | DELETE friendships[{friendIndex}]""".stripMargin,
      "friendIndex" -> 1)

    // Then (no exception, and correct result)
    assertStats(result, nodesDeleted = 0, relationshipsDeleted = 1)
  }

  test("should be able to delete nodes from a map") {
    // Given
    createLabeledNode("User")
    createLabeledNode("User")

    // When
    val result = updateWithBothPlannersAndCompatibilityMode(
      """ MATCH (u:User)
        | WITH {key: u} AS nodes
        | DELETE nodes.key""".stripMargin)

    // Then (no exception, and correct result)
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 0)
  }

  test("should be able to delete relationships from a map") {
    // Given
    val a = createLabeledNode("User")
    val b = createLabeledNode("User")
    relate(a, b)
    relate(b, a)

    // When
    val result = updateWithBothPlannersAndCompatibilityMode(
      """ MATCH (:User)-[r]->(:User)
        | WITH {key: r} AS rels
        | DELETE rels.key""".stripMargin)

    // Then (no exception, and correct result)
    assertStats(result, nodesDeleted = 0, relationshipsDeleted = 2)
  }

  test("should be able to detach delete nodes from a nested map/collection") {
    // Given
    val a = createLabeledNode("User")
    val b = createLabeledNode("User")
    relate(a, b)
    relate(b, a)

    // When
    val result = updateWithBothPlannersAndCompatibilityMode(
      """ MATCH (u:User)
        | WITH {key: collect(u)} AS nodeMap
        | DETACH DELETE nodeMap.key[0]""".stripMargin)

    // Then (no exception, and correct result)
    assertStats(result, nodesDeleted = 1, relationshipsDeleted = 2)
  }

  test("should be able to delete relationships from a nested map/collection") {
    // Given
    val a = createLabeledNode("User")
    val b = createLabeledNode("User")
    relate(a, b)
    relate(b, a)

    // When
    val result = updateWithBothPlannersAndCompatibilityMode(
      """ MATCH (:User)-[r]->(:User)
        | WITH {key: {key: collect(r)}} AS rels
        | DELETE rels.key.key[0]""".stripMargin)

    // Then (no exception, and correct result)
    assertStats(result, nodesDeleted = 0, relationshipsDeleted = 1)
  }

  test("should be able to delete paths from a nested map/collection") {
    // Given
    val a = createLabeledNode("User")
    val b = createLabeledNode("User")
    relate(a, b)
    relate(b, a)

    // When
    val result = updateWithBothPlannersAndCompatibilityMode(
      """ MATCH p=(:User)-[r]->(:User)
        | WITH {key: collect(p)} AS pathColls
        | DELETE pathColls.key[0], pathColls.key[1]""".stripMargin)

    // Then (no exception, and correct result)
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 2)
  }

  //https://github.com/neo4j/neo4j-java-driver/issues/212
  test("should handle bidirectional match and relationship types") {
    // GIVEN
    val relId = relate(createNode(), createNode(), "T").getId

    // WHEN
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH ()-[r:T]-() WHERE ID(r) = {id} DELETE r", "id" -> relId)

    // THEN
    assertStats(result, relationshipsDeleted = 1)
  }

  override def timeLimit: Span = 10 seconds
}
