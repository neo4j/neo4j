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

import org.neo4j.cypher.internal.compiler.v3_3.test_helpers.CreateTempFileTestSupport
import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class CreateAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport
  with CreateTempFileTestSupport {

  //Not TCK material
  test("should have bound node recognized after projection with WITH + LOAD CSV") {
    val url = createCSVTempFileURL(writer => writer.println("Foo"))

    val query = s"CREATE (a) WITH a LOAD CSV FROM '$url' AS line CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = executeWith(Configs.CommunityInterpreted - Configs.Cost2_3, query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  //Not TCK material
  test("should have bound node recognized after projection with WITH + CALL") {
    val query = "CREATE (a:L) WITH a CALL db.labels() YIELD label CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = executeWith(Configs.CommunityInterpreted - Configs.Version2_3 - Configs.AllRulePlanners, query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, labelsAdded = 1)
  }

  //Not TCK material
  test("should have bound node recognized after projection with WITH + FOREACH") {
    val query = "CREATE (a) WITH a FOREACH (i in [] | SET a.prop = 1) CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = executeWith(Configs.CommunityInterpreted - Configs.Cost2_3, query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  //Not TCK material
  test("should handle pathological create query") {

    val amount = 200

    val query = "CREATE" + List.fill(amount)("(:Bar)-[:FOO]->(:Baz)").mkString(", ")

    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query)

    assertStats(result, nodesCreated = 2 * amount, relationshipsCreated = amount, labelsAdded = 2 * amount)

    // Should not get StackOverflowException
    result.executionPlanDescription()
  }

  test("should allow create, delete and return in one go (relationship)") {
    val typ = "ThisIsTheRelationshipType"
    val query = s"CREATE ()-[r:$typ]->() DELETE r RETURN type(r)"
    val result = executeWith(Configs.CommunityInterpreted - Configs.Cost2_3, query)
    result.toList should equal(List(Map("type(r)" -> typ)))
  }

  test("should create nodes with label and property with slotted runtime") {
    //TODO: Remove this test once we can create relationships in slotted runtime
    val createdNumber = 1

    val query = "CREATE" + List.fill(createdNumber)("(:Bar{prop: 1})").mkString(", ")

    val result = executeWith(Configs.All - Configs.Compiled - Configs.Cost2_3, query)

    assertStats(result, nodesCreated = createdNumber, labelsAdded = createdNumber, propertiesWritten = createdNumber)

    // Should not get StackOverflowException
    result.executionPlanDescription()
  }

}
