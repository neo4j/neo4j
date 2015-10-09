/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.{NewPlannerTestSupport, ExecutionEngineFunSuite, QueryStatisticsTestSupport, SyntaxException}
import org.neo4j.graphdb.{DynamicRelationshipType, Direction, Node, Relationship}

class CreateAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("create a single node") {
    val result = updateWithBothPlanners("create ()")
    assertStats(result, nodesCreated = 1)
    // then
    result.toList shouldBe empty
  }

  test("create a single node with single label") {
    val result = updateWithBothPlanners("create (:A)")
    assertStats(result, nodesCreated = 1, labelsAdded = 1)
    // then
    result.toList shouldBe empty
  }

  test("create a single node with multiple labels") {
    val result = updateWithBothPlanners("create (n:A:B:C:D)")
    assertStats(result, nodesCreated = 1, labelsAdded = 4)
    // then
    result.toList shouldBe empty
  }

  test("combine match and create") {
    createNode()
    createNode()
    val result = updateWithBothPlanners("match (n) create ()")
    assertStats(result, nodesCreated = 2)
    // then
    result.toList shouldBe empty
  }

  test("combine match, with, and create") {
    createNode()
    createNode()
    val result = updateWithBothPlanners("match (n) create (n1) with * match(p) create (n2)")
    assertStats(result, nodesCreated = 10)
    // then
    result.toList shouldBe empty
  }


  test("should not see updates created by itself") {
    createNode()

    val result = updateWithBothPlanners("match (n) create ()")
    assertStats(result, nodesCreated = 1)
  }

  test("create a single node with properties") {
    val result = updateWithBothPlanners("create (n {prop: 'foo'}) return n.prop as p")
    assertStats(result, nodesCreated = 1, propertiesSet = 1)
    // then
    result.toList should equal(List(Map("p" -> "foo")))
  }

  test("using an undirected relationship pattern should fail on create") {
      intercept[SyntaxException](executeScalar[Relationship]("create (a {id: 2})-[r:KNOWS]-(b {id: 1}) RETURN r"))
  }

  test("create node using null properties should just ignore those properties") {
    // when
    val result = updateWithBothPlanners("create (n {id: 12, property: null}) return n.id as id")
    assertStats(result, nodesCreated = 1, propertiesSet = 1)

    // then
   result.toList should equal(List(Map("id" -> 12)))
  }

  test("create relationship using null properties should just ignore those properties") {
    // when
    val result = updateWithBothPlanners("create ()-[r:X {id: 12, property: null}]->() return r.id")
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesSet = 1)

    // then
    result.toList should equal(List(Map("r.id" -> 12)))
  }

  test("create simple pattern") {
    val result = updateWithBothPlanners("CREATE (a)-[r:R]->(b)")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("create both nodes and relationships") {
    val result = updateWithBothPlanners("CREATE (a), (b), (a)-[r:R]->(b)")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("create relationship with property") {
    val result = updateWithBothPlanners("CREATE (a)-[r:R {prop: 42}]->(b)")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesSet = 1)
  }

  test("creates relationship in correct direction") {
    import scala.collection.JavaConverters._
    val start = createLabeledNode("Y")
    val end = createLabeledNode("X")

    val typ = "TYPE"
    val result = updateWithBothPlanners(s"MATCH (x:X), (y:Y) CREATE (x)<-[:$typ]-(y)")

    assertStats(result, relationshipsCreated = 1)
    graph.inTx {
      start.getRelationships(DynamicRelationshipType.withName(typ), Direction.OUTGOING).asScala should have size 1
      end.getRelationships(DynamicRelationshipType.withName(typ), Direction.INCOMING).asScala should have size 1
    }
  }

  test("creates one node, matches one and create relationship") {
    import scala.collection.JavaConverters._
    val start = createLabeledNode("Start")

    val typ = "TYPE"
    val result = updateWithBothPlanners(s"MATCH (x:Start) CREATE (x)-[:$typ]->(y:End)")

    assertStats(result, nodesCreated = 1, labelsAdded = 1, relationshipsCreated = 1)
    graph.inTx {
      start.getRelationships(DynamicRelationshipType.withName(typ), Direction.OUTGOING).asScala should have size 1
    }
  }

  test("single create after with") {
    //given
    createNode()
    createNode()

    //when
    val query = "MATCH (n) CREATE() WITH * CREATE ()"
    val result = updateWithBothPlanners(query)

    //then
    assertStats(result, nodesCreated = 4)
    result should not(use("Apply"))
  }
  test("create relationship with reversed direction") {
    val result = updateWithBothPlanners("CREATE (a)<-[r1:R]-(b)")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)

    executeWithAllPlanners("MATCH (a)<-[r1:R]-(b) RETURN *").toList should have size 1
  }

  test("create relationship with multiple hops") {
    val result = updateWithBothPlanners("CREATE (a)-[r1:R]->(b)-[r2:R]->(c)")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2)

    executeWithAllPlanners("MATCH (a)-[r1:R]->(b)-[r2:R]->(c) RETURN *").toList should have size 1
  }

  test("create relationship with multiple hops and reversed direction") {
    val result = updateWithBothPlanners("CREATE (a)<-[r1:R]-(b)<-[r2:R]-(c)")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2)

    executeWithAllPlanners("MATCH (a)<-[r1:R]-(b)<-[r2:R]-(c) RETURN *").toList should have size 1
  }

  test("create relationship with multiple hops and changing directions") {
    val result = updateWithBothPlanners("CREATE (a:A)-[r1:R]->(b:B)<-[r2:R]-(c:C)")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2, labelsAdded = 3)

    executeWithAllPlanners("MATCH (a:A)-[r1:R]->(b:B)<-[r2:R]-(c:C) RETURN *").toList should have size 1
  }

  test("create relationship with multiple hops and changing directions 2") {
    val result = updateWithBothPlanners("CYPHER planner=rule CREATE (a:A)<-[r1:R]-(b:B)-[r2:R]->(c:C)")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2, labelsAdded = 3)

    executeWithAllPlanners("MATCH (a:A)<-[r1:R]-(b:B)-[r2:R]->(c:C) RETURN *").toList should have size 1
  }

  test("create relationship with multiple hops and varying directions and types") {
    val result = updateWithBothPlanners("CREATE (a)-[r1:R1]->(b)<-[r2:R2]-(c)-[r3:R3]->(d)")

    assertStats(result, nodesCreated = 4, relationshipsCreated = 3)

    executeWithAllPlanners("MATCH (a)-[r1:R1]->(b)<-[r2:R2]-(c)-[r3:R3]->(d) RETURN *").toList should have size 1
  }
}
