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
package org.neo4j.cypher

import org.neo4j.cypher.internal.spi.v3_0.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.compiler.v3_0.IndexDescriptor

class UniqueIndexUsageAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {
  test("should be able to use indexes") {
    given()

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Crew) WHERE n.name = 'Neo' RETURN n")

    // Then
    result.executionPlanDescription().toString should include("NodeUniqueIndexSeek")
    result should have size 1
    assertNoLockingHappened
  }

  test("should not forget predicates") {
    given()

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Crew) WHERE n.name = 'Neo' AND n.name = 'Morpheus' RETURN n")

    // Then
    result shouldBe empty
    result.executionPlanDescription().toString should include("NodeUniqueIndexSeek")
    assertNoLockingHappened
  }

  test("should use index when there are multiple labels on the node") {
    given()

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Matrix:Crew) WHERE n.name = 'Cypher' RETURN n")

    // Then
    result.executionPlanDescription().toString should include("NodeUniqueIndexSeek")
    result should have size 1
    assertNoLockingHappened
  }

  test("should be able to use value coming from UNWIND for index seek") {
    // Given
    graph.createConstraint("Prop", "id")
    val n1 = createLabeledNode(Map("id" -> 1), "Prop")
    val n2 = createLabeledNode(Map("id" -> 2), "Prop")
    val n3 = createLabeledNode(Map("id" -> 3), "Prop")
    for (i <- 4 to 30) createLabeledNode(Map("id" -> i), "Prop")

    // When
    val result = executeWithAllPlannersAndCompatibilityMode("unwind [1,2,3] as x match (n:Prop) where n.id = x return n;")

    // Then
    result.toList should equal(List(Map("n" -> n1), Map("n" -> n2), Map("n" -> n3)))
    result.executionPlanDescription().toString should include("NodeUniqueIndexSeek")
    assertNoLockingHappened
  }

  test("should handle nulls in index lookup") {
    // Given
    val cat = createLabeledNode("Cat")
    val dog = createLabeledNode("Dog")
    relate(cat, dog, "FRIEND_OF")

    // create many nodes with label 'Place' to make sure index seek is planned
    (1 to 100).foreach(i => createLabeledNode(Map("name" -> s"Area $i"), "Place"))

    graph.createConstraint("Place", "name")

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
    result.toList should equal(List(Map("p" -> null, "placeName" -> null)))
    assertNoLockingHappened
  }

  test("should not use indexes when RHS of property comparison depends on the node searched for (equality)") {
    // Given
    val n1 = createLabeledNode(Map("a" -> 0, "b" -> 1), "MyNodes")
    val n2 = createLabeledNode(Map("a" -> 1, "b" -> 1), "MyNodes")
    val n3 = createLabeledNode(Map("a" -> 2, "b" -> 2), "MyNodes")
    val n4 = createLabeledNode(Map("a" -> 3, "b" -> 5), "MyNodes")

    graph.createConstraint("MyNodes", "a")

    val query =
      """|MATCH (m:MyNodes)
        |WHERE m.a = m.b
        |RETURN m""".stripMargin

    // When
    val result = executeWithAllPlannersAndCompatibilityMode(query)

    // Then
    result.toList should equal(List(
      Map("m" -> n2),
      Map("m" -> n3)
    ))
    result.executionPlanDescription().toString shouldNot include("Index")
    assertNoLockingHappened
  }

  var lockingIndexSearchCalled = false

  override protected def initTest(): Unit = {
    super.initTest()
    lockingIndexSearchCalled = false

    val assertReadOnlyMonitorListener = new IndexSearchMonitor {
      override def indexSeek(index: IndexDescriptor, value: Any): Unit = {}

      override def lockingUniqueIndexSeek(index: IndexDescriptor, value: Any): Unit = {
        lockingIndexSearchCalled = true
      }
    }
    kernelMonitors.addMonitorListener(assertReadOnlyMonitorListener)
  }

  private def given() {
    graph.execute(
      """CREATE (architect:Matrix { name:'The Architect' }),
        |       (smith:Matrix { name:'Agent Smith' }),
        |       (cypher:Matrix:Crew { name:'Cypher' }),
        |       (trinity:Crew { name:'Trinity' }),
        |       (morpheus:Crew { name:'Morpheus' }),
        |       (neo:Crew { name:'Neo' }),
        |       (smith)-[:CODED_BY]->(architect),
        |       (cypher)-[:KNOWS]->(smith),
        |       (morpheus)-[:KNOWS]->(trinity),
        |       (morpheus)-[:KNOWS]->(cypher),
        |       (neo)-[:KNOWS]->(morpheus),
        |       (neo)-[:LOVES]->(trinity)""".stripMargin)

    for (i <- 1 to 10) createLabeledNode(Map("name" -> ("Joe" + i)), "Crew")

    for (i <- 1 to 10) createLabeledNode(Map("name" -> ("Smith" + i)), "Matrix")

    graph.createConstraint("Crew", "name")
  }

  private def assertNoLockingHappened: Unit = {
    withClue("Should not lock indexes: ") { lockingIndexSearchCalled should equal(false) }
  }

}
