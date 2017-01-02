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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}

class RemoveAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {
  test("should ignore nulls") {
    val n = createNode("apa" -> 42)

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) OPTIONAL MATCH (n)-[r]->() REMOVE r.apa RETURN n")
    result.toList should equal(List(Map("n" -> n)))
  }

  test("remove a single label") {
    // GIVEN
    createLabeledNode(Map("prop" -> 42), "L")

    // WHEN
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) REMOVE n:L RETURN n.prop")

    //THEN
    assertStats(result, labelsRemoved = 1)
    result.toList should equal(List(Map("n.prop" -> 42)))
  }

  test("remove multiple labels") {
    // GIVEN
    createLabeledNode(Map("prop" -> 42), "L1", "L2", "L3")

    // WHEN
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) REMOVE n:L1:L3 RETURN labels(n)")

    //THEN
    assertStats(result, labelsRemoved = 2)
    result.toList should equal(List(Map("labels(n)" -> List("L2"))))
  }

  test("remove a single node property") {
    // GIVEN
    createLabeledNode(Map("prop" -> 42), "L")

    // WHEN
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) REMOVE n.prop RETURN exists(n.prop) as still_there")

    //THEN
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("still_there" -> false)))
  }

  test("remove multiple node properties") {
    // GIVEN
    createLabeledNode(Map("prop" -> 42, "a" -> "a", "b" -> "B"), "L")

    // WHEN
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) REMOVE n.prop, n.a RETURN size(keys(n)) as props")

    //THEN
    assertStats(result, propertiesWritten = 2)
    result.toList should equal(List(Map("props" -> 1)))
  }

  test("remove a single relationship property") {
    // GIVEN
    relate(createNode(), createNode(), "X", Map("prop" -> 42))

    // WHEN
    val result = updateWithBothPlanners("MATCH ()-[r]->() REMOVE r.prop RETURN exists(r.prop) as still_there")

    //THEN
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("still_there" -> false)))
  }

  test("remove multiple relationship properties") {
    // GIVEN
    relate(createNode(), createNode(), "X", Map("prop" -> 42, "a" -> "a", "b" -> "B"))

    // WHEN
    val result = updateWithBothPlanners("MATCH ()-[r]->() REMOVE r.prop, r.a RETURN size(keys(r)) as props")

    //THEN
    assertStats(result, propertiesWritten = 2)
    result.toList should equal(List(Map("props" -> 1)))
  }

  test("removing an missing property is a valid operation") {
    // GIVEN
    createNode()
    createNode()
    createNode()

    // WHEN
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) REMOVE n.prop RETURN sum(size(keys(n))) as totalNumberOfProps")

    //THEN
    assertStats(result, propertiesWritten = 0)
    result.toList should equal(List(Map("totalNumberOfProps" -> 0)))
  }

  test("removing property when not sure if it is a node or relationship should still work - NODE") {
    val n = createNode("name" -> "Anders")

    updateWithBothPlannersAndCompatibilityMode("WITH {p} as p SET p.lastname = p.name REMOVE p.name", "p" -> n)

    graph.inTx {
      n.getProperty("lastname") should equal("Anders")
      n.hasProperty("name") should equal(false)
    }
  }

  test("removing property when not sure if it is a node or relationship should still work - REL") {
    val r = relate(createNode(), createNode(), "name" -> "Anders")

    updateWithBothPlannersAndCompatibilityMode("WITH {p} as p SET p.lastname = p.name REMOVE p.name", "p" -> r)

    graph.inTx {
      r.getProperty("lastname") should equal("Anders")
      r.hasProperty("name") should equal(false)
    }
  }
}
