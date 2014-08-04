/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher

class EagerizationAcceptanceTest extends ExecutionEngineFunSuite {
  val EagerRegEx = "Eager".r

  test("should not introduce eagerness for MATCH nodes and CREATE relationships") {
    val result = execute("MATCH a, b CREATE (a)-[:KNOWS]->(b)")

    assertNumberOfEagerness(result, 0)
  }

  test("should introduce eagerness when doing first matching and then creating nodes") {
    val result = execute("MATCH a CREATE (b)")

    assertNumberOfEagerness(result, 1)
  }

  test("should not introduce eagerness for MATCH nodes and CREATE UNIQUE relationships") {
    val result = execute("MATCH a, b CREATE UNIQUE (a)-[r:KNOWS]->(b)")

    assertNumberOfEagerness(result, 0)
  }

  ignore("should not introduce eagerness for MATCH nodes and MERGE relationships") {
    val result = execute("MATCH a, b MERGE (a)-[r:KNOWS]->(b)")

    assertNumberOfEagerness(result, 0)
  }

  ignore("should not add eagerness when not writing to nodes") {
    val result = execute("MATCH a, b CREATE (a)-[r:KNOWS]->(b) SET r = { key: 42 }")

    assertNumberOfEagerness(result, 0)
  }

  private def assertNumberOfEagerness(r: ExecutionResult, expectedEagerCount: Int) {
    val plan = r.executionPlanDescription().toString
    val length = EagerRegEx.findAllIn(plan).length
    assert(length == expectedEagerCount * 2, plan)
  }
}
