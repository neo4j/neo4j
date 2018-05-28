/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_2.test_helpers.CreateTempFileTestSupport
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}

class CreateAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport
  with CreateTempFileTestSupport {

  // Version 3.1 = rule planner will work first after the next patch release 3.1.8
  test("handle null value in property map from parameter for create node") {
    val query = "CREATE (a {props}) RETURN a.foo, a.bar"

    val result = updateWithCostPlannerOnly(query, "props" -> Map("foo" -> null, "bar" -> "baz"))

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

  // Version 3.1 = rule planner will work first after the next patch release 3.1.8
  test("handle null value in property map from parameter for create relationship") {
    val query = "CREATE (a)-[r:REL {props}]->() RETURN r.foo, r.bar"

    val result = updateWithCostPlannerOnly(query, "props" -> Map("foo" -> null, "bar" -> "baz"))

    result.toSet should equal(Set(Map("r.foo" -> null, "r.bar" -> "baz")))
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 1)
  }

  // Version 3.1 = rule planner will work first after the next patch release 3.1.8
  test("handle null value in property map from parameter") {
    val query = "CREATE (a {props})-[r:REL {props}]->() RETURN a.foo, a.bar, r.foo, r.bar"

    val result = updateWithCostPlannerOnly(query, "props" -> Map("foo" -> null, "bar" -> "baz"))

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

    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, labelsAdded = 1)
  }

  //Not TCK material
  test("should have bound node recognized after projection with WITH + FOREACH") {
    val query = "CREATE (a) WITH a FOREACH (i in [] | SET a.prop = 1) CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  //Not TCK material
  test("should handle pathological create query") {

    val query = "CREATE" + List.fill(500)("(:Bar)-[:FOO]->(:Baz)").mkString(", ")

    val result = updateWithCostPlannerOnly(query)

    assertStats(result, nodesCreated = 1000, relationshipsCreated = 500, labelsAdded = 1000)

    // Should not get StackOverflowException
    result.executionPlanDescription()
  }

}
