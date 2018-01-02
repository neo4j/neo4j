/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

/**
 * These tests are testing the actual index implementation, thus they should all check the actual result.
 * If you only want to verify that plans using indexes are actually planned, please use
 * [[org.neo4j.cypher.internal.compiler.v2_3.planner.logical.LeafPlanningIntegrationTest]]
 */
class NodeIndexSeekAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport{

  test("should not forget predicates") {
    setUpDatabaseForTests()

    // When
    val result = executeWithAllPlanners("MATCH (n:Crew) WHERE n.name = 'Neo' AND n.name = 'Morpheus' RETURN n")

    // Then
    result should (use("NodeIndexSeek") and be(empty))
  }


  test("should be able to use value coming from UNWIND for index seek") {
    // Given
    graph.createIndex("Prop", "id")
    val n1 = createLabeledNode(Map("id" -> 1), "Prop")
    val n2 = createLabeledNode(Map("id" -> 2), "Prop")
    val n3 = createLabeledNode(Map("id" -> 3), "Prop")
    for (i <- 4 to 30) createLabeledNode(Map("id" -> i), "Prop")

    // When
    val result = executeWithAllPlanners("unwind [1,2,3] as x match (n:Prop) where n.id = x return n;")

    // Then
    val expected = List(Map("n" -> n1), Map("n" -> n2), Map("n" -> n3))
    result should (use("NodeIndexSeek") and evaluateTo(expected))
  }

  test("should use index selectivity when planning") {
    // Given
    graph.inTx{
      val ls = (1 to 100).map { i =>
        createLabeledNode(Map("l" -> i), "L")
      }

      val rs = (1 to 100).map { i =>
        createLabeledNode(Map("r" -> 23), "R")
      }

      for (l <- ls ; r <- rs) {
        relate(l, r, "REL")
      }
    }

    // note: creating index after the nodes makes sure that we have statistics when the indexes come online
    graph.createIndex("L", "l")
    graph.createIndex("R", "r")

    val result = executeWithAllPlanners("MATCH (l:L {l: 9})-[:REL]->(r:R {r: 23}) RETURN l, r")
    result should (use("NodeIndexSeek") and have size 100)
  }

  test("should handle nulls in index lookup") {
    // Given
    val cat = createLabeledNode("Cat")
    val dog = createLabeledNode("Dog")
    relate(cat, dog, "FRIEND_OF")

    // create many nodes with label 'Place' to make sure index seek is planned
    (1 to 100).foreach(i => createLabeledNode(Map("name" -> s"Area $i"), "Place"))

    graph.createIndex("Place", "name")

    // When
    val result = executeWithCostPlannerOnly(
      """
        |MATCH ()-[f:FRIEND_OF]->()
        |WITH f.placeName AS placeName
        |OPTIONAL MATCH (p:Place)
        |WHERE p.name = placeName
        |RETURN p, placeName
      """.stripMargin)

    // Then
    result should (use("NodeIndexSeek") and evaluateTo(List(Map("p" -> null, "placeName" -> null))))
  }

  test("should not use indexes when RHS of property comparison depends on the node searched for (equality)") {
    // Given
    val n1 = createLabeledNode(Map("a" -> 1), "MyNodes")
    val n2 = createLabeledNode(Map("a" -> 0), "MyNodes")
    val n3 = createLabeledNode(Map("a" -> 1, "b" -> 1), "MyNodes")
    val n4 = createLabeledNode(Map("a" -> 1, "b" -> 5), "MyNodes")

    graph.createIndex("MyNodes", "a")

    val query =
      """|MATCH (m:MyNodes)
        |WHERE m.a = coalesce(m.b, 0)
        |RETURN m""".stripMargin

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result.toList should equal(List(
      Map("m" -> n2),
      Map("m" -> n3)
    ))
    result.executionPlanDescription().toString shouldNot include("Index")
  }

  test("should not use indexes when RHS of property comparison depends on the node searched for (range query)") {
    // Given
    val n1 = createLabeledNode(Map("a" -> 1), "MyNodes")
    val n2 = createLabeledNode(Map("a" -> 0), "MyNodes")
    val n3 = createLabeledNode(Map("a" -> 1, "b" -> 1), "MyNodes")
    val n4 = createLabeledNode(Map("a" -> 5, "b" -> 1), "MyNodes")

    graph.createIndex("MyNodes", "a")

    val query =
      """|MATCH (m:MyNodes)
        |WHERE m.a > coalesce(m.b, 0)
        |RETURN m""".stripMargin

    // When
    val result = executeWithAllPlanners(query)

    // Then
    result.toList should equal(List(
      Map("m" -> n1),
      Map("m" -> n4)
    ))
    result.executionPlanDescription().toString shouldNot include("Index")
  }

  private def setUpDatabaseForTests() {
    executeWithRulePlanner(
      """CREATE (architect:Matrix { name:'The Architect' }),
        |       (smith:Matrix { name:'Agent Smith' }),
        |       (cypher:Matrix:Crew { name:'Cypher' }),
        |       (trinity:Crew { name:'Trinity' }),
        |       (morpheus:Crew { name:'Morpheus' }),
        |       (neo:Crew { name:'Neo' }),
        |       smith-[:CODED_BY]->architect,
        |       cypher-[:KNOWS]->smith,
        |       morpheus-[:KNOWS]->trinity,
        |       morpheus-[:KNOWS]->cypher,
        |       neo-[:KNOWS]->morpheus,
        |       neo-[:LOVES]->trinity""".stripMargin)

    for (i <- 1 to 10) createLabeledNode(Map("name" -> ("Joe" + i)), "Crew")

    for (i <- 1 to 10) createLabeledNode(Map("name" -> ("Smith" + i)), "Matrix")

    graph.createIndex("Crew", "name")
  }

}
