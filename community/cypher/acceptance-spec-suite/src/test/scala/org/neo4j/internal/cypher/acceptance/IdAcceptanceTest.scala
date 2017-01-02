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
import org.neo4j.graphdb.Relationship

class IdAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("id on a node should work in both runtimes")  {
    // GIVEN
    val expected = createNode().getId

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n) RETURN id(n)")

    // THEN
    result.toList should equal(List(Map("id(n)" -> expected)))

  }

  test("id on a rel should work in both runtimes")  {
    // GIVEN
    val expected = relate(createNode(), createNode()).getId

    // WHEN
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()-[r]->() RETURN id(r)")

    // THEN
    result.toList should equal(List(Map("id(r)" -> expected)))
  }

  test("deprecated functions still work") {
    val r = relate(createNode(), createNode())

    executeWithAllPlannersAndCompatibilityMode("RETURN toInt('1') AS one").columnAs[Long]("one").next should equal(1L)
    executeWithAllPlannersAndCompatibilityMode("RETURN upper('abc') AS a").columnAs[String]("a").next should equal("ABC")
    executeWithAllPlannersAndCompatibilityMode("RETURN lower('ABC') AS a").columnAs[String]("a").next should equal("abc")
    executeWithAllPlannersAndCompatibilityMode("MATCH p = ()-->() RETURN rels(p) AS r").columnAs[List[Relationship]]("r").next should equal(List(r))
  }

}
