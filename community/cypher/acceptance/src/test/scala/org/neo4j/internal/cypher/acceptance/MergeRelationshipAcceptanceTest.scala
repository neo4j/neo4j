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

import org.neo4j.cypher.{CypherExecutionException, ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport, SyntaxException}
import org.neo4j.graphdb.{Path, Relationship, Result}

class MergeRelationshipAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  // TCK'd
  test("should be able to create relationship") {
    // given
    createNode("A")
    createNode("B")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a {name: 'A'}), (b {name: 'B'}) MERGE (a)-[r:TYPE]->(b) RETURN count(*)")

    // then
    assertStats(result, relationshipsCreated = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
    executeWithAllPlannersAndCompatibilityMode("MATCH (a {name: 'A'})-[:TYPE]->(b {name: 'B'}) RETURN a.name").toList should have size 1
  }

  // TCK'd
  test("should be able to find a relationship") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    relate(a, b, "TYPE")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a {name: 'A'}), (b {name: 'B'}) MERGE (a)-[r:TYPE]->(b) RETURN count(r)")

    // then
    assertStats(result, relationshipsCreated = 0)
    result.toList should equal(List(Map("count(r)" -> 1)))
  }

  // TCK'd
  test("should be able to find two existing relationships") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    relate(a, b, "TYPE")
    relate(a, b, "TYPE")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a {name: 'A'}), (b {name: 'B'}) MERGE (a)-[r:TYPE]->(b) RETURN count(r)")

    // then
    assertStats(result, relationshipsCreated = 0)
    result.toList should equal(List(Map("count(r)" -> 2)))
  }

  // TCK'd
  test("should be able to filter out relationships") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    relate(a, b, "TYPE", "r1")
    val r = relate(a, b, "TYPE", "r2")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a {name: 'A'}), (b {name: 'B'}) MERGE (a)-[r:TYPE {name: 'r2'}]->(b) RETURN id(r)")

    // then
    assertStats(result, relationshipsCreated = 0)
    result.toList should equal(List(Map("id(r)" -> r.getId)))
  }

  // TCK'd
  test("should be able to create when nothing matches") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    relate(a, b, "TYPE", "r1")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a {name: 'A'}), (b {name: 'B'}) MERGE (a)-[r:TYPE {name: 'r2'}]->(b) RETURN count(r)")

    // then
    assertStats(result, relationshipsCreated = 1, propertiesWritten = 1)
    result.toList should equal(List(Map("count(r)" -> 1)))
    executeWithAllPlannersAndCompatibilityMode("MATCH (a {name: 'A'})-[r:TYPE {name: 'r2'}]->(b {name: 'B'}) RETURN a.name, b.name, r.name")
      .toList should equal(
      List(Map("a.name" -> "A", "b.name" -> "B", "r.name" -> "r2"))
    )
  }

  // TCK'd
  test("should not be fooled by direction") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    val r = relate(b, a, "TYPE")
    relate(a, b, "TYPE")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a {name: 'A'}), (b {name: 'B'}) MERGE (a)<-[r:TYPE]-(b) RETURN id(r)")

    // then
    assertStats(result, relationshipsCreated = 0)
    result.toList should equal(List(Map("id(r)" -> r.getId)))
  }

  // TCK'd
  test("should create relationship with property") {
    // given
    val a = createNode("A")
    val b = createNode("B")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a {name: 'A'}), (b {name: 'B'}) MERGE (a)-[r:TYPE {name: 'Lola'}]->(b) RETURN count(r)")

    // then
    assertStats(result, relationshipsCreated = 1, propertiesWritten = 1)
    result.toList should equal(List(Map("count(r)" -> 1)))
    executeWithAllPlannersAndCompatibilityMode("MATCH (a {name:'A'})-[r:TYPE {name:'Lola'}]->(b {name:'B'}) RETURN a, b").toList should equal(
      List(Map("a" -> a, "b" -> b)))
  }

  // TCK'd
  test("should handle on create") {
    // given
    createNode("A")
    createNode("B")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a {name: 'A'}), (b {name: 'B'}) MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name = 'Lola' RETURN count(r)")

    // then

    assertStats(result, relationshipsCreated = 1, propertiesWritten = 1)
    result.toList should equal(List(Map("count(r)" -> 1)))
    executeWithAllPlannersAndCompatibilityMode("MATCH (a {name:'A'})-[r]->(b) RETURN r.name").toList should equal(List(Map("r.name" -> "Lola")))
  }

  // TCK'd
  test("should handle on match") {
    // given
    val a = createNode("A")
    val b = createNode("B")
    relate(a, b, "TYPE")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a {name: 'A'}), (b {name: 'B'}) MERGE (a)-[r:TYPE]->(b) ON MATCH SET r.name = 'Lola' RETURN count(r)")

    // then
    assertStats(result, relationshipsCreated = 0, propertiesWritten = 1)
    result.toList should equal(List(Map("count(r)" -> 1)))
    executeScalarWithAllPlannersAndCompatibilityMode[String]("MATCH (a {name: 'A'})-[r:TYPE]->(b {name: 'B'}) RETURN r.name") should equal("Lola")
  }

  // TCK'd
  test("should handle on create and on match") {
    // given
    relate(createNode("name" -> "A", "id" -> 1), createNode("name" -> "B", "id" -> 2), "TYPE")
    createNode("name" -> "A", "id" -> 3)
    createNode("name" -> "B", "id" -> 4)

    // when
    val result = updateWithBothPlannersAndCompatibilityMode(
      "MATCH (a {name: 'A'}), (b {name: 'B'}) MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name = 'Lola' ON MATCH SET r.name = 'RUN' RETURN count(r)")

    // then
    assertStats(result, relationshipsCreated = 3, propertiesWritten = 4)
    result.toList should equal(List(Map("count(r)" -> 4)))
    executeWithAllPlannersAndCompatibilityMode("MATCH (a {name: 'A'})-[r]->(b) RETURN a.id, r.name, b.id").toSet should equal(Set(
      Map("a.id" -> 1, "b.id" -> 2, "r.name" -> "RUN"),
      Map("a.id" -> 3, "b.id" -> 2, "r.name" -> "Lola"),
      Map("a.id" -> 1, "b.id" -> 4, "r.name" -> "Lola"),
      Map("a.id" -> 3, "b.id" -> 4, "r.name" -> "Lola")
    ))
  }

  // TCK'd
  test("should work with single bound node") {
    // given
    val a = createNode("A")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a {name: 'A'}) MERGE (a)-[r:TYPE]->() RETURN count(r)")

    // then
    assertStats(result, relationshipsCreated = 1, nodesCreated = 1)
    result.toList should equal(List(Map("count(r)" -> 1)))
    executeWithAllPlannersAndCompatibilityMode("MATCH (a)-[r:TYPE]->() RETURN a").toList should equal(List(Map("a" -> a)))
  }

  // TCK'd
  test("should handle longer patterns") {
    // given
    val a = createNode("A")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a {name: 'A'}) MERGE (a)-[r:TYPE]->()<-[:TYPE]-() RETURN count(r)")

    // then
    assertStats(result, relationshipsCreated = 2, nodesCreated = 2)
    result.toList should equal(List(Map("count(r)" -> 1)))
    executeWithAllPlannersAndCompatibilityMode("MATCH (a {name: 'A'})-[r:TYPE]->()<-[:TYPE]-(b) RETURN a").toList should equal(List(Map("a" -> a)))
  }

  // TCK'd
  test("should handle nodes bound in the middle") {
    // given
    createNode("B")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (b {name: 'B'}) MERGE (a)-[r1:TYPE]->(b)<-[r2:TYPE]-(c) RETURN type(r1), type(r2)")

    // then
    assertStats(result, relationshipsCreated = 2, nodesCreated = 2)
    result.toList should equal(List(Map("type(r1)" -> "TYPE", "type(r2)" -> "TYPE")))
  }

  // TCK'd
  test("should handle nodes bound in the middle when half pattern is matching") {
    // given
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")
    relate(a, b, "TYPE")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (b:B) MERGE (a:A)-[r1:TYPE]->(b)<-[r2:TYPE]-(c:C) RETURN type(r1), type(r2)")

    // then
    assertStats(result, relationshipsCreated = 2, nodesCreated = 2, labelsAdded = 2)
    result.toList should equal(List(Map("type(r1)"-> "TYPE", "type(r2)" -> "TYPE")))
  }

  // TCK'd
  test("should handle first declaring nodes and then creating relationships between them") {
    // given
    createLabeledNode("A")
    createLabeledNode("B")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (a:A) MERGE (b:B) MERGE (a)-[:FOO]->(b)")

    // then
    assertStats(result, relationshipsCreated = 1)
    result shouldBe empty
  }

  // TCK'd
  test("should handle building links mixing create with merge pattern") {
    // given

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("CREATE (a:A) MERGE (a)-[:KNOWS]->(b:B) CREATE (b)-[:KNOWS]->(c:C) RETURN count(*)")

    // then
    assertStats(result, relationshipsCreated = 2, nodesCreated = 3, labelsAdded = 3)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  // TCK'd
  test("when merging a pattern that includes a unique node constraint violation fail") {
    // given
    graph.createConstraint("Person", "id")
    createLabeledNode(Map("id" -> 666), "Person")

    // when then fails
    a [CypherExecutionException] should be thrownBy {
      updateWithBothPlannersAndCompatibilityMode("CREATE (a:A) MERGE (a)-[:KNOWS]->(b:Person {id:666})")
    }
  }

  // TCK'd
  test("should work well inside foreach") {
    val a = createLabeledNode("S")
    relate(a, createNode("prop" -> 2), "FOO")

    val result = updateWithBothPlannersAndCompatibilityMode(
      """MATCH (a:S)
        |FOREACH(x IN [1, 2, 3] |
        |  MERGE (a)-[:FOO]->({prop: x})
        |)
        """.stripMargin)

    assertStats(result, nodesCreated = 2, propertiesWritten = 2, relationshipsCreated = 2)
    result shouldBe empty
  }

  // TCK'd
  test("should handle two merges inside foreach") {
    val a = createLabeledNode("S")
    val b = createLabeledNode(Map("prop" -> 42), "End")

    val result = updateWithBothPlanners(
      """MATCH (a:S)
        |FOREACH(x IN [42] |
        |  MERGE (b:End {prop: x})
        |  MERGE (a)-[:FOO]->(b)
        |)""".stripMargin)

    assertStats(result, nodesCreated = 0, propertiesWritten = 0, relationshipsCreated = 1)
    graph.inTx {
      val rel = a.getRelationships.iterator().next()
      rel.getStartNode should equal(a)
      rel.getEndNode should equal(b)
    }
  }

  // TCK'd
  test("should handle two merges inside bare foreach") {
    createNode("x" -> 1)

    val result = updateWithBothPlanners(
      """FOREACH(v IN [1, 2] |
        |  MERGE (a {x: v})
        |  MERGE (b {y: v})
        |  MERGE (a)-[:FOO]->(b)
        |)
        """.stripMargin)

    assertStats(result, nodesCreated = 3, propertiesWritten = 3, relationshipsCreated = 2)
    result shouldBe empty
  }

  // TCK'd
  test("should handle two merges inside foreach after with") {
    val result = updateWithBothPlanners(
      """WITH 3 AS y
        |FOREACH(x IN [1, 2] |
        |  MERGE (a {x: x, y: y})
        |  MERGE (b {x: x + 1, y: y})
        |  MERGE (a)-[:FOO]->(b)
        |)
        """.stripMargin)

    assertStats(result, nodesCreated = 3, propertiesWritten = 6, relationshipsCreated = 2)
    result shouldBe empty
  }

  // TCK'd
  test("should introduce named paths1") {
    // updateWithBothPlanners doesn't work when returning elements created in the same statement
    val result = executeWithCostPlannerOnly("MERGE (a) MERGE p = (a)-[:R]->() RETURN p")

    assertStats(result, relationshipsCreated = 1, nodesCreated = 2)
    val resultList = result.toList
    result should have size 1
    resultList.head.head._2.isInstanceOf[Path] should be(true)
  }

  // TCK'd
  test("should introduce named paths2") {
    // updateWithBothPlanners doesn't work when returning elements created in the same statement
    val result = executeWithCostPlannerOnly("MERGE (a {x: 1}) MERGE (b {x: 2}) MERGE p = (a)-[:R]->(b) RETURN p")

    assertStats(result, relationshipsCreated = 1, nodesCreated = 2, propertiesWritten = 2)
    val resultList = result.toList
    result should have size 1
    resultList.head.head._2.isInstanceOf[Path] should be(true)
  }

  // TCK'd
  test("should introduce named paths3") {
    // updateWithBothPlanners doesn't work when returning elements created in the same statement
    val result = executeWithCostPlannerOnly("MERGE p = (a {x: 1}) RETURN p")

    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
    val resultList = result.toList
    result should have size 1
    resultList.head.head._2.isInstanceOf[Path] should be(true)
  }

  // TCK'd
  test("should handle foreach in foreach game of life ftw") {

    /* creates a grid 4 nodes wide and 4 nodes deep.
     o-o-o-o
     | | | |
     o-o-o-o
     | | | |
     o-o-o-o
     | | | |
     o-o-o-o
     */

    val result = updateWithBothPlanners(
      """FOREACH(x IN [0, 1, 2] |
        |  FOREACH(y IN [0, 1, 2] |
        |    MERGE (a {x: x, y: y})
        |    MERGE (b {x: x + 1, y: y})
        |    MERGE (c {x: x, y: y + 1})
        |    MERGE (d {x: x + 1, y: y + 1})
        |    MERGE (a)-[:R]->(b)
        |    MERGE (a)-[:R]->(c)
        |    MERGE (b)-[:R]->(d)
        |    MERGE (c)-[:R]->(d)
        |  )
        |)
        """.stripMargin)

    assertStats(result, nodesCreated = 16, relationshipsCreated = 24, propertiesWritten = 16 * 2)
    result shouldBe empty
  }

  // TCK'd
  test("should handle merge with no known points") {
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE ({name: 'Andres'})-[:R]->({name: 'Emil'})")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 2)
  }

  // TCK'd
  test("should handle foreach in foreach game without known points") {

    /* creates a grid 4 nodes wide and 4 nodes deep.
     o-o o-o o-o
     | | | | | |
     o-o o-o o-o
     | | | | | |
     o-o o-o o-o
     | | | | | |
     o-o o-o o-o
     */

    val result = updateWithBothPlanners(
      """FOREACH(x IN [0, 1, 2] |
        |  FOREACH(y IN [0, 1, 2] |
        |    MERGE (a {x: x, y: y})-[:R]->(b {x: x + 1, y: y})
        |    MERGE (c {x: x, y: y + 1})-[:R]->(d {x: x + 1, y: y + 1})
        |    MERGE (a)-[:R]->(c) MERGE (b)-[:R]->(d)
        |  )
        |)
        """.stripMargin)

    assertStats(result, nodesCreated = 6*4, relationshipsCreated = 3*4+6*3, propertiesWritten = 6*4*2)
    result shouldBe empty
  }

  // TCK'd
  test("should handle on create on created nodes") {
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (a)-[:KNOWS]->(b) ON CREATE SET b.created = timestamp()")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 1)
    result shouldBe empty
  }

  // TCK'd
  test("should handle on match on created nodes") {
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (a)-[:KNOWS]->(b) ON MATCH SET b.created = timestamp()")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 0)
    result shouldBe empty
  }

  // TCK'd
  test("should handle on match on created rels") {
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (a)-[r:KNOWS]->(b) ON MATCH SET r.created = timestamp()")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 0)
    result shouldBe empty
  }

  // TCK'd
  test("should use left to right direction when creating based on pattern with undirected relationship") {
    // updateWithBothPlanners doesn't work when returning elements created in the same statement
    val result = executeWithCostPlannerOnly("MERGE (a {id: 2})-[r:KNOWS]-(b {id: 1}) RETURN startNode(r).id AS s, endNode(r).id AS e")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 2)
    result.toList should equal(List(Map("s" -> 2, "e" -> 1)))
  }

  // TCK'd
  test("should find existing right to left relationship when matching with undirected relationship") {
    val r = relate(createNode("id" -> 1), createNode("id" -> 2), "KNOWS")

    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (a {id: 2})-[r:KNOWS]-(b {id: 1}) RETURN r")

    result.columnAs[Relationship]("r").next() should equal(r)
    result shouldBe empty
  }

  // TCK'd
  test("should find existing left to right relationship when matching with undirected relationship") {
    val r = relate(createNode("id" -> 2), createNode("id" -> 1), "KNOWS")
    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (a {id: 2})-[r:KNOWS]-(b {id: 1}) RETURN r")

    result.columnAs[Relationship]("r").next() should equal(r)
    result shouldBe empty
  }

  // TCK'd
  test("should find existing relationships when matching with undirected relationship") {
    val r1 = relate(createNode("id" -> 2), createNode("id" -> 1), "KNOWS")
    val r2 = relate(createNode("id" -> 1), createNode("id" -> 2), "KNOWS")

    val result = updateWithBothPlannersAndCompatibilityMode("MERGE (a {id: 2})-[r:KNOWS]-(b {id: 1}) RETURN r")

    result.columnAs[Relationship]("r").toSet should equal(Set(r1, r2))
    result shouldBe empty
  }

  // TCK'd
  test("should reject merging nodes having the same id but different labels") {
    a [SyntaxException] should be thrownBy {
      updateWithBothPlannersAndCompatibilityMode("MERGE (a:Foo)-[r:KNOWS]->(a:Bar)")
    }
  }

  // TCK'd
  test("merge should handle list properties properly from variable") {
    val query =
      """
        |CREATE (a:Foo), (b:Bar)
        |WITH a, b
        |UNWIND ["a,b", "a,b"] AS str
        |WITH a, b, split(str, ",") AS roles
        |MERGE (a)-[r:FB {foobar: roles}]->(b)
        |RETURN count(*)""".stripMargin

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 1, labelsAdded = 2)
    result.toList should equal(List(Map("count(*)" -> 2)))
  }

  // TCK'd
  test("merge should handle list properties properly") {
    relate(createLabeledNode("A"), createLabeledNode("B"), "T", Map("prop" -> Array(42, 43)))

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (a:A), (b:B) MERGE (a)-[r:T {prop: [42, 43]}]->(b) RETURN count(*)")

    assertStats(result, nodesCreated = 0, relationshipsCreated = 0, propertiesWritten = 0)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  // TCK'd
  test("merge should see variables introduced by other update actions") {
    // when
    val result = updateWithBothPlannersAndCompatibilityMode("CREATE (a) MERGE (a)-[:X]->() RETURN count(a)")

    // then
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
    result.toList should equal(List(Map("count(a)" -> 1)))
  }

  // TCK'd
  test("unwind combined with multiple merges") {
    val query = """UNWIND ['Keanu Reeves', 'Hugo Weaving', 'Carrie-Anne Moss', 'Laurence Fishburne'] AS actor
                  |MERGE (m:Movie  {name: 'The Matrix'})
                  |MERGE (p:Person {name: actor})
                  |MERGE (p)-[:ACTED_IN]->(m)""".stripMargin

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 5, relationshipsCreated = 4, propertiesWritten = 5, labelsAdded = 5)
    result shouldBe empty
  }

  // TCK'd
  test("a merge following a delete of multiple rows should not match on a deleted entity") {
    // GIVEN
    val a = createLabeledNode("A")
    val branches = 2
    val b = (0 until branches).map(n => createLabeledNode(Map("value" -> n), "B"))
    val c = (0 until branches).map(_ => createLabeledNode("C"))
    (0 until branches).foreach(n => {
      relate(a, b(n))
      relate(b(n), c(n))
    })

    val query =
      """
        |MATCH (a:A)-[ab]->(b:B)-[bc]->(c:C)
        |DELETE ab, bc, b, c
        |MERGE (newB:B {value: 1})
        |MERGE (a)-[:REL]->(newB)
        |MERGE (newC:C)
        |MERGE (newB)-[:REL]->(newC)
      """.stripMargin

    // WHEN
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    // THEN
    result shouldBe empty
    assertStats(result, nodesCreated = 2, nodesDeleted = 4, labelsAdded = 2, propertiesWritten = 1, relationshipsCreated = 2, relationshipsDeleted = 4)
  }

  // TCK'd
  test("merges should not be able to match on deleted relationships") {
    // GIVEN
    val a = createNode()
    val b = createNode()
    relate(a, b, "T", Map("name" -> "rel1"))
    relate(a, b, "T", Map("name" -> "rel2"))

    val query = """
                  |MATCH (a)-[t:T]->(b)
                  |DELETE t
                  |MERGE (a)-[t2:T {name: 'rel3'}]->(b)
                  |RETURN t2.name
                """.stripMargin

    // WHEN
    val result = updateWithBothPlannersAndCompatibilityMode(query)

    // THEN
    result.toList should equal(List(Map("t2.name" -> "rel3"), Map("t2.name" -> "rel3")))
    assertStats(result, relationshipsDeleted = 2, relationshipsCreated = 1, propertiesWritten = 1)
  }

  // TCK'd
  test("should not create nodes when aliases are applied to variable names") {
    createNode()
    val query = "MATCH (n) MATCH (m) WITH n AS a, m AS b MERGE (a)-[r:T]->(b) RETURN id(a) AS a, id(b) AS b"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, relationshipsCreated = 1)
    result.toList should equal(List(Map("a" -> 0, "b" -> 0)))
  }

  // TCK'd
  test("should create only one node when an alias is applied to a variable name") {
    createNode()
    val query = "MATCH (n) WITH n AS a MERGE (a)-[r:T]->(b) RETURN id(a) AS a"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1)
    result.toList should equal(List(Map("a" -> 0)))
  }

  // TCK'd
  test("should not create nodes when aliases are applied to variable names multiple times") {
    createNode()
    val query = "MATCH (n) MATCH (m) WITH n AS a, m AS b MERGE (a)-[:T]->(b) WITH a AS x, b AS y MERGE (a)-[:T]->(b) RETURN id(x) AS x, id(y) AS y"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, relationshipsCreated = 1)
    result.toList should equal(List(Map("x" -> 0, "y" -> 0)))
  }

  // TCK'd
  test("should create only one node when an alias is applied to a variable name multiple times") {
    createNode()
    val query = "MATCH (n) WITH n AS a MERGE (a)-[:T]->() WITH a AS x MERGE (x)-[:T]->() RETURN id(x) AS x"

    val result = updateWithBothPlannersAndCompatibilityMode(query)

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1)
    result.toList should equal(List(Map("x" -> 0)))
  }
}
