/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.collector

import org.neo4j.cypher._

class DataCollectorGraphCountsAcceptanceTest extends ExecutionEngineFunSuite with GraphIcing with SampleGraphs {

  test("retrieve empty") {
    // when
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')").single

    // then
    res("section") should be("GRAPH COUNTS")
    list(res("data"), "nodes") should contain only Map("count" -> 0)
    list(res("data"), "relationships") should contain only Map("count" -> 0)
    list(res("data"), "indexes") should be(Nil)
    list(res("data"), "constraints") should be(Nil)
  }

  test("retrieve nodes") {
    // given
    graph.inTx {
      createNode()
      createLabeledNode("User")
      createLabeledNode("User")
      createLabeledNode("Donkey")
    }

    // when
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')").single

    // then
    list(res("data"), "nodes") should contain only(
      Map("count" -> 4),
      Map("count" -> 2, "label" -> "User"),
      Map("count" -> 1, "label" -> "Donkey")
    )
  }

  test("retrieve relationships") {
    // given
    graph.inTx {
      val n1 = createNode()
      val n2 = createLabeledNode("User")
      relate(n1, n1, "R")
      relate(n1, n2, "R")
      relate(n2, n1, "R2")
      relate(n2, n2, "R")
    }

    // when
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')").single

    // then
    list(res("data"), "relationships") should contain only(
      Map("count" -> 4),
      Map("count" -> 3, "relationshipType" -> "R"),
      Map("count" -> 1, "relationshipType" -> "R", "startLabel" -> "User"),
      Map("count" -> 2, "relationshipType" -> "R", "endLabel" -> "User"),
      Map("count" -> 1, "relationshipType" -> "R2"),
      Map("count" -> 1, "relationshipType" -> "R2", "startLabel" -> "User")
    )
  }

  test("retrieve complex graph") {
    // given
    createSteelfaceGraph()

    // when
    val res = execute("CALL db.stats.retrieve('GRAPH COUNTS')").single

    // then
    assertSteelfaceGraphCounts(res, TokenNames("User",
                                               "Car",
                                               "Room",
                                               "OWNS",
                                               "STAYS_IN",
                                               "email",
                                               "lastName",
                                               "firstName",
                                               "number",
                                               "hotel"))
  }

  test("retrieve anonymized complex graph") {
    // given
    createSteelfaceGraph()

    // when
    val res = execute("CALL db.stats.retrieveAllAnonymized('myGraphToken')").toList.filter(row => row("section") == "GRAPH COUNTS").head

    // then
    assertSteelfaceGraphCounts(res, TokenNames("L0",
                                               "L1",
                                               "L2",
                                               "R0",
                                               "R1",
                                               "p0",
                                               "p1",
                                               "p2",
                                               "p3",
                                               "p4"))
  }

  case class TokenNames(User: String,
                        Car: String,
                        Room: String,
                        OWNS: String,
                        STAYS_IN: String,
                        email: String,
                        lastName: String,
                        firstName: String,
                        number: String,
                        hotel: String)

  private def assertSteelfaceGraphCounts(res: Map[String, AnyRef], tokenNames: TokenNames): Unit = {
    import tokenNames._

    res("section") should be("GRAPH COUNTS")
    list(res("data"), "nodes") should contain only(
      Map("count" -> 1278),
      Map("label" -> User, "count" -> 1000),
      Map("label" -> Car, "count" -> 128),
      Map("label" -> Room, "count" -> 150)
    )
    list(res("data"), "relationships") should contain only(
      Map("count" -> 320),
      Map("relationshipType" -> OWNS, "count" -> 170),
      Map("relationshipType" -> OWNS, "startLabel" -> User, "count" -> 170),
      Map("relationshipType" -> OWNS, "endLabel" -> Car, "count" -> 100),
      Map("relationshipType" -> OWNS, "endLabel" -> Room, "count" -> 70),
      Map("relationshipType" -> STAYS_IN, "count" -> 150),
      Map("relationshipType" -> STAYS_IN, "startLabel" -> User, "count" -> 150),
      Map("relationshipType" -> STAYS_IN, "endLabel" -> Room, "count" -> 150)
    )
    list(res("data"), "indexes") should contain only(
      Map("labels" -> List(User), "properties" -> List(email), "totalSize" -> 1000, "estimatedUniqueSize" -> 1000, "updatesSinceEstimation" -> 0),
      Map("labels" -> List(User), "properties" -> List(lastName), "totalSize" -> 500, "estimatedUniqueSize" -> 500, "updatesSinceEstimation" -> 0),
      Map("labels" -> List(User), "properties" -> List(firstName, lastName), "totalSize" -> 300, "estimatedUniqueSize" -> 300, "updatesSinceEstimation" -> 0),
      Map("labels" -> List(Room), "properties" -> List(hotel, number), "totalSize" -> 150, "estimatedUniqueSize" -> 50, "updatesSinceEstimation" -> 0),
      Map("labels" -> List(Car), "properties" -> List(number), "totalSize" -> 120, "estimatedUniqueSize" -> 120, "updatesSinceEstimation" -> 8)
    )
    list(res("data"), "constraints") should contain only
      Map("label" -> User, "properties" -> List(email), "type" -> "Uniqueness constraint")
  }

  private def list(map: AnyRef, key: String): IndexedSeq[AnyRef] =
    map.asInstanceOf[Map[String, AnyRef]](key).asInstanceOf[IndexedSeq[AnyRef]]
}
