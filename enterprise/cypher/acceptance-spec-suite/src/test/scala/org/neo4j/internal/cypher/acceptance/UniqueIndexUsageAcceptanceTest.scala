/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.{ComparePlansWithAssertion, Configs}
import org.neo4j.internal.kernel.api.IndexReference

class UniqueIndexUsageAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {
  val expectPlansToFail = Configs.AllRulePlanners

  test("should be able to use indexes") {
    given()

    // When
    val result = executeWith(Configs.All, "MATCH (n:Crew) WHERE n.name = 'Neo' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion((planDescription) => {
        planDescription.toString should include("NodeUniqueIndexSeek")
      }, expectPlansToFail))

    // Then
    result should have size 1
    assertNoLockingHappened
  }

  test("should not forget predicates") {
    given()

    // When
    val result = executeWith(Configs.All, "MATCH (n:Crew) WHERE n.name = 'Neo' AND n.name = 'Morpheus' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion((planDescription) => {
        planDescription.toString should include("NodeUniqueIndexSeek")
      }, expectPlansToFail))

    // Then
    result shouldBe empty
    assertNoLockingHappened
  }

  test("should use index when there are multiple labels on the node") {
    given()

    // When
    val result = executeWith(Configs.All, "MATCH (n:Matrix:Crew) WHERE n.name = 'Cypher' RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion((planDescription) => {
        planDescription.toString should include("NodeUniqueIndexSeek")
      }, expectPlansToFail))

    // Then
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
    val result = executeWith(Configs.All, "unwind [1,2,3] as x match (n:Prop) where n.id = x return n;",
      planComparisonStrategy = ComparePlansWithAssertion((planDescription) => {
        planDescription.toString should include("NodeUniqueIndexSeek")
      }, expectPlansToFail))

    // Then
    result.toList should equal(List(Map("n" -> n1), Map("n" -> n2), Map("n" -> n3)))
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
    val result = executeWith(Configs.All - Configs.Compiled,
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
    val result = executeWith(Configs.Interpreted, query)

    // Then
    result.toList should equal(List(
      Map("m" -> n2),
      Map("m" -> n3)
    ))
    assertNoLockingHappened
  }

  var lockingIndexSearchCalled = false

  override protected def initTest(): Unit = {
    super.initTest()
    lockingIndexSearchCalled = false

    val assertReadOnlyMonitorListener = new IndexSearchMonitor {
      override def indexSeek(index: IndexReference, value: Seq[Any]): Unit = {}

      override def lockingUniqueIndexSeek(index: IndexReference, values: Seq[Any]): Unit = {
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

  private def assertNoLockingHappened(): Unit = {
    withClue("Should not lock indexes: ") { lockingIndexSearchCalled should equal(false) }
  }

}
