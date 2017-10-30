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

import org.neo4j.cypher.internal.compiler.v3_1.test_helpers.CreateTempFileTestSupport
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}

class CreateAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport
  with CreateTempFileTestSupport {

  test("handle null value in property map from parameter for create node") {
    val query = "CREATE (a {props}) RETURN a.foo, a.bar"

    val result = updateWithBothPlanners(query, "props" -> Map("foo" -> null, "bar" -> "baz"))

    result.toSet should equal(Set(Map("a.foo" -> null, "a.bar" -> "baz")))
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
  }

  test("handle null value in property map from parameter for create node with SET") {
    createNode(("foo", 42), ("bar", "fu"))
    val query = "MATCH (a) SET a = {props} RETURN a.foo, a.bar"

    val result = updateWithBothPlanners(query, "props" -> Map("foo" -> null, "bar" -> "baz"))

    result.toSet should equal(Set(Map("a.foo" -> null, "a.bar" -> "baz")))
    assertStats(result, propertiesWritten = 2)
  }

  test("handle null value in property map from parameter for create relationship") {
    val query = "CREATE (a)-[r:REL {props}]->() RETURN r.foo, r.bar"

    val result = updateWithBothPlanners(query, "props" -> Map("foo" -> null, "bar" -> "baz"))

    result.toSet should equal(Set(Map("r.foo" -> null, "r.bar" -> "baz")))
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 1)
  }

  test("handle null value in property map from parameter") {
    val query = "CREATE (a {props})-[r:REL {props}]->() RETURN a.foo, a.bar, r.foo, r.bar"

    val result = updateWithBothPlanners(query, "props" -> Map("foo" -> null, "bar" -> "baz"))

    result.toSet should equal(Set(Map("a.foo" -> null, "a.bar" -> "baz", "r.foo" -> null, "r.bar" -> "baz")))
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 2)
  }

  //Not TCK material
  test("should have bound node recognized after projection with WITH + LOAD CSV") {
    val url = createCSVTempFileURL( writer => writer.println("Foo") )

    val query = s"CREATE (a) WITH a LOAD CSV FROM '$url' AS line CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  //Not TCK material
  test("should have bound node recognized after projection with WITH + CALL") {
    val query = "CREATE (a:L) WITH a CALL db.labels() YIELD label CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = executeWithCostPlannerOnly(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, labelsAdded = 1)
  }

  //Not TCK material
  test("should have bound node recognized after projection with WITH + FOREACH") {
    val query = "CREATE (a) WITH a FOREACH (i in [] | SET a.prop = 1) CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

}
