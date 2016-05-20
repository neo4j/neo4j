/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.impl.storageengine.impl.recordstorage.RecordStorageEngine

class LabelsAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  // TCK'd
  test("Adding single label") {
    createNode()

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) SET n:Foo RETURN labels(n)")

    assertStats(result, labelsAdded = 1)
    result.toList should equal(List(Map("labels(n)" -> List("Foo"))))
  }

  // TCK'd
  test("should ignore space before colon") {
    createNode()

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) SET n :Foo RETURN labels(n)")

    assertStats(result, labelsAdded = 1)
    result.toList should equal(List(Map("labels(n)" -> List("Foo"))))
  }

  // TCK'd
  test("Adding multiple labels") {
    createNode()

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) SET n:Foo:Bar RETURN labels(n)")

    assertStats(result, labelsAdded = 2)
    result.toList should equal(List(Map("labels(n)" -> List("Foo", "Bar"))))
  }

  // TCK'd
  test("should ignore intermediate whitespace 1") {
    createNode()

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) SET n :Foo :Bar RETURN labels(n)")

    assertStats(result, labelsAdded = 2)
    result.toList should equal(List(Map("labels(n)" -> List("Foo", "Bar"))))
  }

  // TCK'd
  test("should ignore intermediate whitespace 2") {
    createNode()

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) SET n :Foo:Bar RETURN labels(n)")

    assertStats(result, labelsAdded = 2)
    result.toList should equal(List(Map("labels(n)" -> List("Foo", "Bar"))))
  }

  // TCK'd
  test("create node without label") {
    val query = "CREATE (node) RETURN labels(node)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 1)
    result.toList should equal(List(Map("labels(node)" -> List.empty)))
  }

  // TCK'd
  test("create node with two labels") {
    val query = "CREATE (node:Foo:Bar {name: 'Mattias'}) RETURN labels(node)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 1, labelsAdded = 2, propertiesWritten = 1)
    result.toList should equal(List(Map("labels(node)" -> List("Foo", "Bar"))))
  }

  // TCK'd
  test("ignore space when creating node with labels") {
    val query = "CREATE (node :Foo:Bar) RETURN labels(node)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 1, labelsAdded = 2)
    result.toList should equal(List(Map("labels(node)" -> List("Foo", "Bar"))))
  }

  // TCK'd
  test("create node with label in pattern") {
    val query = "CREATE (n:Person)-[:OWNS]->(x:Dog) RETURN labels(n)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, labelsAdded = 2, relationshipsCreated = 1)
    result.toList should equal(List(Map("labels(n)" -> List("Person"))))
  }

  // TCK'd
  test("should fail when adding new label predicate on already bound node 1") {
    val query = "CREATE (n: Foo)-[:T1]->(b), (n: Bar)-[:T2]->(c)"

    a [SyntaxException] shouldBe thrownBy {
      updateWithBothPlannersAndCompatibilityMode(query)
    }
  }

  // TCK'd
  test("should fail when adding new label predicate on already bound node 2") {
    val query = "CREATE (c)<-[:T2]-(n: Foo), (n: Bar)<-[:T1]-(b)"

    a [SyntaxException] shouldBe thrownBy {
      updateWithBothPlannersAndCompatibilityMode(query)
    }
  }

  // TCK'd
  test("should fail when adding new label predicate on already bound node 3") {
    val query = "CREATE (n :Foo) CREATE (n :Bar)-[:OWNS]->(x:Dog)"

    a [SyntaxException] shouldBe thrownBy {
      updateWithBothPlannersAndCompatibilityMode(query)
    }
  }

  // TCK'd
  test("should fail when adding new label predicate on already bound node 4") {
    val query = "CREATE (n {}) CREATE (n :Bar)-[:OWNS]->(x:Dog)"

    a [SyntaxException] shouldBe thrownBy {
      updateWithBothPlannersAndCompatibilityMode(query)
    }
  }

  // TCK'd
  test("should fail when adding new label predicate on already bound node 5") {
    val query = "CREATE (n :Foo) CREATE (n {})-[:OWNS]->(x:Dog)"

    a [SyntaxException] shouldBe thrownBy {
      updateWithBothPlannersAndCompatibilityMode(query)
    }
  }

  // TCK'd
  test("Add labels to nodes in a foreach") {
    val query = "CREATE (a), (b), (c) WITH [a, b, c] AS nodes FOREACH(n IN nodes | SET n :Foo:Bar)"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 3, labelsAdded = 6)
    graph.execute("MATCH (n) WHERE NOT(n:Foo AND n:Bar) RETURN n").hasNext shouldBe false
  }

  // TCK'd
  test("Using labels() in RETURN clauses") {
    createNode()

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) WHERE id(n) = 0 RETURN labels(n)")

    result.toList should equal(List(Map("labels(n)" -> List.empty)))
  }

  // TCK'd
  test("Removing a label") {
    createLabeledNode("Foo", "Bar")

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) REMOVE n:Foo RETURN labels(n)")

    assertStats(result, labelsRemoved = 1)
    result.toList should equal(List(Map("labels(n)" -> List("Bar"))))
  }

  // TCK'd
  test("Removing non-existent label") {
    createLabeledNode("Foo")

    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n) REMOVE n:Bar RETURN labels(n)")

    assertStats(result, labelsRemoved = 0)
    result.toList should equal(List(Map("labels(n)" -> List("Foo"))))
  }

  // Not TCK material
  test("should not create labels id when trying to delete non-existing labels") {
    createNode()

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) REMOVE n:BAR RETURN id(n) AS id")

    assertStats(result, labelsRemoved = 0)
    result.toList should equal(List(Map("id" -> 0)))

    graph.inTx {
      graph.getDependencyResolver.resolveDependency(classOf[RecordStorageEngine]).testAccessNeoStores().getLabelTokenStore.getHighId should equal(0)
    }
  }
}
