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

import org.neo4j.cypher.{QueryStatisticsTestSupport, NewPlannerTestSupport, ExecutionEngineFunSuite}

class RemoveAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {
  test("should ignore nulls") {
    val n = createNode("apa" -> 42)

    val result = executeWithRulePlanner("MATCH (n) OPTIONAL MATCH (n)-[r]->() REMOVE r.apa RETURN n")
    result.toList should equal(List(Map("n" -> n)))
  }

  test("remove a single label") {
    // GIVEN
    createLabeledNode(Map("prop" -> 42), "L")

    // WHEN
    val result = updateWithBothPlanners("MATCH (n) REMOVE n:L RETURN n.prop")

    //THEN
    assertStats(result, labelsRemoved = 1)
    result.toList should equal(List(Map("n.prop" -> 42)))
  }

  test("remove multiple labels") {
    // GIVEN
    createLabeledNode(Map("prop" -> 42), "L1", "L2", "L3")

    // WHEN
    val result = updateWithBothPlanners("MATCH (n) REMOVE n:L1:L3 RETURN labels(n)")

    //THEN
    assertStats(result, labelsRemoved = 2)
    result.toList should equal(List(Map("labels(n)" -> List("L2"))))
  }
}
