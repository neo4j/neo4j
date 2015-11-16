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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compiler.v3_0.executionplan.InternalExecutionResult
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.matching.Regex

class EagerizationAcceptanceTest extends ExecutionEngineFunSuite with TableDrivenPropertyChecks with QueryStatisticsTestSupport with NewPlannerTestSupport {
  val EagerRegEx: Regex = "Eager(?!(Aggregation))".r
  val RepeatableReadRegEx: Regex = "RepeatableRead".r

  test("should introduce eagerness between MATCH and DELETE relationships") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")
    val query = "MATCH (a)-[t:T]-(b) DELETE t RETURN count(*) as count"

    val result = updateWithBothPlanners(query)
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsDeleted = 1)
    assertNumberOfEagerness(query, 1)
  }

  test("should introduce eagerness between MATCH and CREATE relationships with overlapping relationship types") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")
    val query = "MATCH (a)-[t:T]-(b) CREATE (a)-[:T]->(b) RETURN count(*) as count"

    val result = updateWithBothPlanners(query)
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsCreated = 2)
    assertNumberOfEagerness(query, 1)
  }

  test("should not introduce eagerness between MATCH and CREATE relationships when properties don't overlap") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T", Map("prop1" -> "foo"))
    val query = "MATCH (a)-[t:T {prop1: 'foo'}]-(b) CREATE (a)-[:T {prop2: 'bar'}]->(b) RETURN count(*) as count"

    val result = updateWithBothPlanners(query)
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsCreated = 2, propertiesSet = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness between MATCH and CREATE relationships when properties overlap") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T", Map("prop1" -> "foo"))
    val query = "MATCH (a)-[t:T {prop1: 'foo'}]-(b) CREATE (a)-[:T {prop1: 'foo'}]->(b) RETURN count(*) as count"

    val result = updateWithBothPlanners(query)
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsCreated = 2, propertiesSet = 2)
    assertNumberOfEagerness(query, 1)
  }

  test("should not introduce eagerness between MATCH and CREATE relationships with overlapping relationship types when directed") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")
    val query = "MATCH (a)-[t:T]->(b) CREATE (a)-[:T]->(b) RETURN count(*) as count"

    val result = updateWithBothPlanners(query)
    result.columnAs[Int]("count").next should equal(1)
    assertStats(result, relationshipsCreated = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness between MATCH and CREATE relationships with unrelated relationship types") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")
    val query = "MATCH (a)-[t:T]-(b) CREATE (a)-[:T2]->(b) RETURN count(*) as count"

    val result = updateWithBothPlanners(query)
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsCreated = 2)
    assertNumberOfEagerness(query, 0)
  }

  // TESTS FOR DELETE AND MERGE

  test("should introduce eagerness between DELETE and MERGE for node") {
    createLabeledNode(Map("value" -> 0), "B")
    createLabeledNode(Map("value" -> 1), "B")
    val query =
      """
        |MATCH (b:B)
        |DELETE b
        |MERGE (b2:B { value: 1 })
        |RETURN b2.value
      """.stripMargin

    assertStats(executeWithRulePlanner(query), nodesCreated = 1, nodesDeleted = 2, propertiesSet = 1, labelsAdded = 1)
    assertNumberOfEagerness(query, 1)
  }

  test("should not introduce eagerness between DELETE and MERGE for nodes when there is no read matching the merge") {
    createLabeledNode("B")
    createLabeledNode("B")
    val query =
      """
        |MATCH (b:B)
        |DELETE b
        |MERGE ()
      """.stripMargin

    assertStats(executeWithRulePlanner(query), nodesCreated = 1, nodesDeleted = 2)
    assertNumberOfEagerness(query, 0)
  }

  ignore("should not introduce eagerness between DELETE and MERGE for nodes when deleting variable not bound for same label") {
    // TODO: Delete must know what label(s) on nodes it deletes to be able to solve this

    createLabeledNode("B")
    createLabeledNode("B")
    createLabeledNode("C")
    createLabeledNode("C")
    val query =
      """
        |MATCH (b:B)
        |MATCH (c:C)
        |DELETE b
        |MERGE (:C)
      """.stripMargin

    assertStats(executeWithRulePlanner(query), nodesCreated = 0, nodesDeleted = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness between MATCH and DELETE + DELETE and MERGE for relationship") {
    val a = createNode()
    val b = createNode()
    val rel1 = relate(a, b, "T")
    val rel2 = relate(a, b, "T")
    val query =
      """
        |MATCH (a)-[t:T]->(b)
        |DELETE t
        |MERGE (a)-[t2:T]->(b)
        |RETURN t2
      """.stripMargin

    //TODO: We should not need eager between MATCH and DELETE when directed pattern
    val result = executeWithRulePlanner(query)
    assertStats(result, relationshipsDeleted = 2, relationshipsCreated = 1)

    // Merge should not be able to match on deleted relationship
    result.toList should not contain Map("t2" -> rel1)
    result.toList should not contain Map("t2" -> rel2)
    assertNumberOfEagerness(query, 2)
  }

  test("should introduce eagerness between DELETE and MERGE for relationships when there is no read matching the merge") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")
    relate(a, b, "T")
    val query =
      """
        |MATCH (a)-[t:T]->(b)
        |DELETE t
        |MERGE (a)-[t2:T2]->(b)
        |RETURN t2
      """.stripMargin

    assertStats(executeWithRulePlanner(query), relationshipsDeleted = 2, relationshipsCreated = 1)
    //TODO this might be safe without eager, given that the match is directed and probably need to check the direction
    assertNumberOfEagerness(query, 2)
  }

  test("should introduce eagerness between DELETE and MERGE for relationships when there is a read matching the merge") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")
    relate(a, b, "T")
    val query =
      """
        |MATCH (a)-[t]->(b)
        |DELETE t
        |MERGE (a)-[t2:T2]->(b)
        |RETURN t2
      """.stripMargin

    assertStats(executeWithRulePlanner(query), relationshipsDeleted = 2, relationshipsCreated = 1)
    //TODO: We should not need eager between MATCH and DELETE, when directed pattern
    assertNumberOfEagerness(query, 2)
  }

  ignore("should not introduce eagerness between DELETE and MERGE for relationships when deleting variable not bound for same type") {
    // TODO: Delete must know what type(s) of relationship it deletes to be able to solve this
    val a = createNode()
    val b = createNode()
    relate(a, b, "T1")
    relate(a, b, "T1")
    relate(a, b, "T2")
    relate(a, b, "T2")
    val query =
      """
        |MATCH (a)-[t:T1]->(b)
        |MATCH (a)-[:T2]->(b)
        |DELETE t
        |MERGE (a)-[t2:T2]->(b)
        |RETURN t2
      """.stripMargin

    assertStats(executeWithRulePlanner(query), relationshipsDeleted = 2)
    assertNumberOfEagerness(query, 0)
  }

  // TESTS FOR MATCH AND CREATE

  test("should not introduce eagerness for MATCH nodes and CREATE relationships") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) CREATE (a)-[:KNOWS]->(b)"

    assertStats(updateWithBothPlanners(query), relationshipsCreated = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness for match create match") {
    createNode()
    createNode()

    val query = "MATCH () CREATE () WITH * MATCH (n) RETURN count(n) AS count"

    val result = updateWithBothPlanners(query)
    assertStats(result, nodesCreated = 2)
    result.columnAs[Int]("count").next should equal(8)
    assertNumberOfEagerness(query, 1)
  }

  test("should introduce eagerness for match create match create") {
    createNode("prop" -> 42)
    createNode("prop" -> 43)

    val query = "MATCH (k) CREATE (l {prop: 44}) WITH * MATCH (m) CREATE (n {prop:45}) RETURN k.prop, l.prop, m.prop, n.prop"

    val result = updateWithBothPlanners(query)
    result should use("RepeatableRead", "EagerApply")
    assertStats(result, nodesCreated = 10, propertiesSet = 10)
    assertNumberOfEagerness(query, 2)
  }

  test("should not introduce eagerness for simple match create with nodes created in same tx") {
    createNode()
    createNode()
    createNode()

    graph.inTx {
      createNode()
      createNode()
      createNode()

      val query = "MATCH () CREATE ()"

      val result = executeWithCostPlannerOnly(query)
      assertStats(result, nodesCreated = 6)
      assertNumberOfEagerness(query, 0)
    }
  }

  test("should not introduce eagerness for leaf create match") {
    val query = "CREATE () WITH * MATCH () RETURN 1"
    val result = updateWithBothPlanners(query)
    assertStats(result, nodesCreated = 1)
    result should not(use("ReadOnly"))
    assertNumberOfEagerness(query, 0)
  }

  test("should not need eagerness for match create with labels") {
    createLabeledNode("L")
    val query = "MATCH (:L) CREATE (:L)"

    assertStats(updateWithBothPlanners(query), nodesCreated = 1, labelsAdded = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("should not need eagerness for match create with labels and property with index") {
    createLabeledNode(Map("id" -> 0), "L")
    graph.createIndex("L", "id")

    val query = "MATCH (:L {id: 0}) CREATE (:L {id:0})"

    assertStats(updateWithBothPlanners(query), nodesCreated = 1, labelsAdded = 1,  propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("should need eagerness for double match and then create") {
    createNode()
    createNode()
    val query = "MATCH (), () CREATE ()"

    assertStats(updateWithBothPlanners(query), nodesCreated = 4)
    assertNumberOfEagerness(query, 1)
  }

  test("should not need eagerness for double match and then create when non-overlapping properties") {
    createNode("prop1" -> 42, "prop2" -> 42)
    createNode("prop1" -> 42, "prop2" -> 42)
    val query = "MATCH (a {prop1: 42}), (n {prop2: 42}) CREATE ({prop3: 42})"

    assertStats(updateWithBothPlanners(query), nodesCreated = 4, propertiesSet = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("should need eagerness for double match and then create when overlapping properties") {
    createNode("prop1" -> 42, "prop2" -> 42)
    createNode("prop1" -> 42, "prop2" -> 42)
    val query = "MATCH (a {prop1: 42}), (n {prop2: 42}) CREATE ({prop1: 42, prop2: 42})"

    assertStats(updateWithBothPlanners(query), nodesCreated = 4, propertiesSet = 8)
    assertNumberOfEagerness(query, 1)
  }

  test("should not add eagerness when not writing to nodes") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) CREATE (a)-[r:KNOWS]->(b) SET r = { key: 42 }"

    assertStats(updateWithBothPlanners(query), relationshipsCreated = 4, propertiesSet = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("matching using a pattern predicate and creating relationship should not be eager") {
    relate(createNode(), createNode())
    val query = "MATCH (n) WHERE (n)-->() CREATE (n)-[:T]->()"

    assertStats(updateWithBothPlanners(query), nodesCreated = 1, relationshipsCreated = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("should not be eager when creating single node after matching on pattern with relationship") {
    relate(createNode(), createNode())
    relate(createNode(), createNode())
    val query = "MATCH ()--() CREATE () RETURN count(*) AS count"

    assertStats(updateWithBothPlanners(query), nodesCreated = 4)
    assertNumberOfEagerness(query,  0)
  }

  ignore("should not be eager when creating single node after matching on pattern with relationship and also matching on label") {
    // TODO: Implement RelationShipBoundNodeEffect
    val query = "MATCH (:L) MATCH ()--() CREATE ()"

    assertNumberOfEagerness(query,  0)
  }

  test("should not be eager when creating single node after matching on empty node") {
    createNode()
    val query = "MATCH () CREATE ()"

    assertStats(updateWithBothPlanners(query), nodesCreated = 1)
    assertNumberOfEagerness(query,  0)
  }

  test("should not introduce an eager pipe between two node reads and a relationships create") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) CREATE (a)-[:TYPE]->(b)"

    assertStats(updateWithBothPlanners(query), relationshipsCreated = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce an eager pipe between two node reads and a relationships create when there is sorting between the two") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) WITH a, b ORDER BY id(a) CREATE (a)-[:TYPE]->(b)"

    assertStats(updateWithBothPlanners(query), relationshipsCreated = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce an eager pipe between a leaf node read and a relationship + node create") {
    createNode()
    createNode()
    val query = "MATCH (a) CREATE (a)-[:TYPE]->()"

    assertStats(updateWithBothPlanners(query), nodesCreated = 2, relationshipsCreated = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("should introduce an eager pipe between a non-leaf node read and a relationship + node create") {
    createNode()
    createNode()
    val query = "MATCH (), (a) CREATE (a)-[:TYPE]->()"

    assertStats(updateWithBothPlanners(query), nodesCreated = 4, relationshipsCreated = 4)
    assertNumberOfEagerness(query, 1)
  }

  test("should not introduce an eager pipe between a leaf relationship read and a relationship create") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH (a)-[:TYPE]->(b) CREATE (a)-[:TYPE]->(b) RETURN count(*)"

    assertStats(updateWithBothPlanners(query), relationshipsCreated = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("should introduce an eager pipe between a non-directional leaf relationship read and a relationship create") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH (a)-[:TYPE]-(b) CREATE (a)-[:TYPE]->(b) RETURN count(*) as count"

    //TODO this is probably safe, we need to check directions
    val result = updateWithBothPlanners(query)
    assertStats(result, relationshipsCreated = 4)
    result.columnAs[Int]("count").next should equal(4)
    assertNumberOfEagerness(query, 1)
  }

  test("should not introduce an eager pipe between a non-directional leaf relationship read and a relationship merge") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH (a)-[:TYPE]-(b) MERGE (a)-[:TYPE]->(b) RETURN count(*) as count"

    val result = executeWithRulePlanner(query)
    assertStats(result, relationshipsCreated = 2)
    //TODO: This should not need to be eager?
    assertNumberOfEagerness(query, 1)
    result.columnAs[Int]("count").next should equal(4)

  }

  test("should introduce an eager pipe between a non-leaf relationship read, rel uniqueness, and a relationship create") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH ()-[:TYPE]->(), (a)-[:TYPE]->(b) CREATE (a)-[:TYPE]->(b)"

    assertStats(updateWithBothPlanners(query), relationshipsCreated = 2)
    assertNumberOfEagerness(query, 1)
  }

  test("should handle conflicts with create after WITH") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH ()-[:TYPE]->() CREATE (a)-[:TYPE]->(b) WITH * MATCH ()-[:TYPE]->() CREATE (c)-[:TYPE]->(d) "

    assertStats(updateWithBothPlanners(query), nodesCreated = 20, relationshipsCreated = 10)
    assertNumberOfEagerness(query, 2)

  }

  test("should introduce an eager pipe between a non-leaf relationship read and a relationship create") {
    relate(createLabeledNode("LabelOne"), createLabeledNode("LabelTwo"), "TYPE")
    relate(createLabeledNode("LabelOne"), createLabeledNode("LabelTwo"), "TYPE")
    val query = "MATCH ()-[:TYPE]->() MATCH (a:LabelOne)-[:TYPE]->(b:LabelTwo) CREATE (a)-[:TYPE]->(b)"

    assertStats(updateWithBothPlanners(query), relationshipsCreated = 4)
    assertNumberOfEagerness(query, 1)
  }

  ignore("should not introduce an eager pipe between a non-leaf relationship read and a relationship create on different nodes") {
    // TODO: ExecuteUpdateCommandsPipe could interpret that it creates only rel-bound nodes in this case
    relate(createLabeledNode("LabelOne"), createLabeledNode("LabelTwo"), "TYPE")
    relate(createLabeledNode("LabelOne"), createLabeledNode("LabelTwo"), "TYPE")
    val query = "MATCH ()-[:TYPE]->() MATCH (a:LabelOne)-[:TYPE]->(b:LabelTwo) CREATE ()-[:TYPE]->()"

    assertStats(executeWithRulePlanner(query), relationshipsCreated = 4, nodesCreated = 8)
    assertNumberOfEagerness(query, 0)
  }

  ignore("should not introduce eagerness for a non-leaf match on simple pattern and create single node") {
    // TODO: SimplePatternMatcher uses an AllNodes item in a NodeStartPipe, losing us information
    // about the match actually being on relationship-bound nodes.
    relate(createNode(), createNode())
    relate(createNode(), createNode())
    val query = "MATCH ()-->() MATCH ()-->() CREATE ()"

    assertStats(executeWithRulePlanner(query), nodesCreated = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness for match - create on different relationship types") {
    relate(createNode(), createNode(), "T1")
    relate(createNode(), createNode(), "T1")
    val query = "MATCH ()-[:T1]->() CREATE ()-[:T2]->()"

    assertStats(updateWithBothPlanners(query), nodesCreated = 4, relationshipsCreated = 2)
    assertNumberOfEagerness(query, 0)
  }

  // TESTS FOR MATCH AND DELETE

  test("should introduce eagerness when deleting nodes on normal matches") {
    val node0 = createLabeledNode("Person")
    val node1 = createLabeledNode("Person")
    val node2 = createLabeledNode("Movie")
    val node3 = createLabeledNode("Movie")
    val node4 = createNode()

    val query = "MATCH (a:Person), (m:Movie) DELETE a,m"


    val res = updateWithBothPlanners(query)
    assertStats(res, nodesDeleted = 4)
    assertNumberOfEagerness(query, 1)
  }

  test("should introduce eagerness when deleting nodes on single leaf") {
    createLabeledNode("Person")
    createLabeledNode("Person")
    createLabeledNode("Movie")

    val query = "MATCH (a:Person) DELETE a"

    assertStats(updateWithBothPlanners(query), nodesDeleted = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("should include eagerness when reading and deleting") {
    relate(createNode(), createNode())

    val query = s"MATCH (a)-[r]-(b) DELETE r,a,b"
    val result = executeWithCostPlannerOnly(query)
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 1)

    assertNumberOfEagerness(query, 1)
  }

  test("matching relationship, deleting relationship and nodes should be eager") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")

    val query = "MATCH (a)-[r]-(b) DELETE r, a, b RETURN count(*) as count"
    val result = updateWithBothPlanners(query)

    result.toList should equal(List(Map("count" -> 2)))
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 1)
    assertNumberOfEagerness(query, 1)
  }

  test("matching relationship with property, deleting relationship and nodes should be eager") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T", Map("prop" -> 3))

    val query = "MATCH (a)-[r {prop : 3}]-(b) DELETE r, a, b"

    assertStats(updateWithBothPlanners(query), nodesDeleted = 2, relationshipsDeleted = 1)
    assertNumberOfEagerness(query, 1)
  }

  test("matching directional relationship, deleting relationship and nodes should not be eager") {
    relate(createNode(), createNode(), "T1")
    relate(createNode(), createNode(), "T2")
    relate(createNode(), createNode())
    relate(createNode(), createNode(), "T1")
    relate(createNode(), createNode(), "T2")
    relate(createNode(), createNode())

    val query = "MATCH (a)-[r]->(b) DELETE r, a, b RETURN count(*)"

    val result = updateWithBothPlanners(query)
    result.toList should equal (List(Map("count(*)" -> 6)))
    assertStats(result, nodesDeleted = 12, relationshipsDeleted = 6)
    assertNumberOfEagerness(query, 0)
  }

  test("matching directional relationship, deleting relationship and labeled nodes should not be eager") {
    relate(createLabeledNode("A"), createLabeledNode("B"), "T")
    relate(createLabeledNode("A"), createLabeledNode("B"), "T")

    val query = "MATCH (a:A)-[r]->(b:B) DELETE r, a, b RETURN count(*)"

    val result = updateWithBothPlanners(query)
    result.toList should equal (List(Map("count(*)" -> 2)))
    assertStats(result, nodesDeleted = 4, relationshipsDeleted = 2)
    assertNumberOfEagerness(query, 0)
  }


  test("matching reversed directional relationship, deleting relationship and labeled nodes should not be eager") {
    relate(createLabeledNode("A"), createLabeledNode("B"), "T")
    relate(createLabeledNode("A"), createLabeledNode("B"), "T")

    val query = "MATCH (b:B)<-[r]-(a:A) DELETE r, a, b RETURN count(*)"

    val result = updateWithBothPlanners(query)
    result.toList should equal (List(Map("count(*)" -> 2)))
    assertStats(result, nodesDeleted = 4, relationshipsDeleted = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("matching directional relationship with property, deleting relationship and nodes should not be eager") {
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))

    val query = "MATCH (a)-[r {prop : 3}]->(b) DELETE r, a, b RETURN count(*)"

    val result = updateWithBothPlanners(query)
    result.toList should equal (List(Map("count(*)" -> 4)))
    assertStats(result, nodesDeleted = 8, relationshipsDeleted = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("matching undirected relationship, deleting relationship and nodes should be eager") {
    relate(createNode(), createNode(), "T1")
    relate(createNode(), createNode(), "T2")
    relate(createNode(), createNode())
    relate(createNode(), createNode(), "T1")
    relate(createNode(), createNode(), "T2")
    relate(createNode(), createNode())

    val query = "MATCH (a)-[r]-(b) DELETE r, a, b RETURN count(*)"

    val result = updateWithBothPlanners(query)
    result.toList should equal (List(Map("count(*)" -> 12)))
    assertStats(result, nodesDeleted = 12, relationshipsDeleted = 6)
    assertNumberOfEagerness(query, 1)
  }

  test("matching undirected relationship, deleting relationship and labeled nodes should be eager") {
    relate(createLabeledNode("A"), createLabeledNode("B"), "T1")
    relate(createLabeledNode("A"), createLabeledNode("A"), "T2")
    relate(createLabeledNode("B"), createLabeledNode("A"), "T3")

    val query = "MATCH (a:A)-[r]-(b) DELETE r, a, b RETURN count(*)"

    val result = updateWithBothPlanners(query)
    result.toList should equal (List(Map("count(*)" -> 4)))
    assertStats(result, nodesDeleted = 6, relationshipsDeleted = 3)
    assertNumberOfEagerness(query, 1)
  }

  test("matching undirected relationship with property, deleting relationship and nodes should be eager") {
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))

    val query = "MATCH (a)-[r {prop : 3}]-(b) DELETE r, a, b RETURN count(*)"

    val result = updateWithBothPlanners(query)
    result.toList should equal (List(Map("count(*)" -> 8)))
    assertStats(result, nodesDeleted = 8, relationshipsDeleted = 4)
    assertNumberOfEagerness(query, 1)
  }

  test("matching directional multi-step relationship, deleting relationship and nodes should be eager") {
    val b = createNode()
    relate(createNode(), b)
    relate(createNode(), b)
    relate(b, createNode())
    relate(b, createNode())

    val query = "MATCH (a)-[r1]->(b)-[r2]->(c) DELETE r1, r2, a, b, c RETURN count(*)"

    val result = updateWithBothPlanners(query)
    result.toList should equal (List(Map("count(*)" -> 4)))
    assertStats(result, nodesDeleted = 5, relationshipsDeleted = 4)
    assertNumberOfEagerness(query, 1)
  }

  test("matching directional varlength relationship, deleting relationship and nodes should be eager") {
    val b = createNode()
    relate(createNode(), b)
    relate(createNode(), b)
    relate(b, createNode())
    relate(b, createNode())

    val query = "MATCH (a)-[r*]->(b) DETACH DELETE a, b RETURN count(*)"

    val result = updateWithBothPlanners(query)
    result.toList should equal (List(Map("count(*)" -> 8)))
    assertStats(result, nodesDeleted = 5, relationshipsDeleted = 4)
    assertNumberOfEagerness(query, 1)
  }

  test("create directional relationship with property, match and delete relationship and nodes within same transaction should be eager and work") {
    val query = "CREATE ()-[:T {prop: 3}]->() WITH * MATCH (a)-[r {prop : 3}]->(b) DELETE r, a, b"

    assertStats(updateWithBothPlanners(query), nodesCreated = 2, relationshipsCreated = 1, propertiesSet = 1, nodesDeleted = 2, relationshipsDeleted = 1)
    assertNumberOfEagerness(query, 1)
  }

  // TESTS USING OPTIONAL MATCHES

  test("should need eagerness for match optional match create") {
    createLabeledNode("A", "B")
    createLabeledNode("A", "B")
    createLabeledNode("A")
    val query = "MATCH (a:A) OPTIONAL MATCH (b:B) CREATE (:B)"

    val result = updateWithBothPlanners(query)

    assertStats(result, nodesCreated = 6, labelsAdded = 6)
    assertNumberOfEagerness(query, 1)
  }

  test("should not need eagerness for match optional match create where labels do not interfere") {
    createLabeledNode("A", "B")
    createLabeledNode("A", "B")
    createLabeledNode("A")
    val query = "MATCH (a:A) OPTIONAL MATCH (b:B) CREATE (:A)"

    val result = updateWithBothPlanners(query)

    assertStats(result, nodesCreated = 6, labelsAdded = 6)
    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness when deleting things on optional matches") {
    val node0 = createLabeledNode("Person")
    val node1 = createLabeledNode("Person")
    val node2 = createNode()
    relate(node0, node0)
    relate(node0, node2)
    relate(node0, node2)
    relate(node0, node2)
    relate(node1, node2)
    relate(node1, node2)
    relate(node1, node2)
    relate(node2, node2)

    val query = "MATCH (a:Person) OPTIONAL MATCH (a)-[r1]-() DELETE a, r1"

    val result = updateWithBothPlanners(query)
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 7)
    assertNumberOfEagerness(query, 1)
  }

  test("should introduce eagerness when deleting things from an optional match which is a cartesian product") {
    val node0 = createLabeledNode("Person")
    val node1 = createLabeledNode("Person")
    val node2 = createLabeledNode("Movie")
    val node3 = createLabeledNode("Movie")
    val node4 = createNode()
    relate(node0, node1)
    relate(node0, node1)
    relate(node0, node1)
    relate(node2, node4)

    val query = "MATCH (a:Person) OPTIONAL MATCH (a)-[r1]-(), (m:Movie)-[r2]-() DELETE a, r1, m, r2"

    assertStats(updateWithBothPlanners(query), nodesDeleted = 3, relationshipsDeleted = 4)
    assertNumberOfEagerness(query, 1)
  }

  test("MATCH (a:Person),(m:Movie) CREATE (a)-[:T]->(m) WITH a OPTIONAL MATCH (a) RETURN *") {
    createLabeledNode("Person")
    createLabeledNode("Movie")
    val query = "MATCH (a:Person),(m:Movie) CREATE (a)-[:T]->(m) WITH a OPTIONAL MATCH (a) RETURN *"

    assertStats(updateWithBothPlanners(query), relationshipsCreated = 1)
    assertNumberOfEagerness(query, 0)
  }

  // TESTS FOR MATCH AND MERGE

  test("should not introduce eagerness for MATCH nodes and CREATE UNIQUE relationships") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) CREATE UNIQUE (a)-[r:KNOWS]->(b)"

    assertStats(executeWithRulePlanner(query), relationshipsCreated = 4)

    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness for MATCH nodes and MERGE relationships") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) MERGE (a)-[r:KNOWS]->(b)"
    assertStats(executeWithRulePlanner(query), relationshipsCreated = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness when the ON MATCH includes writing to a non-matched property") {
    val a = createLabeledNode("Foo")
    val b = createLabeledNode("Bar")
    relate(a, b, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET a.prop = 42"
    assertStats(executeWithRulePlanner(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness when the ON MATCH includes writing to a left-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET b:Foo"

    assertStats(executeWithRulePlanner(query), relationshipsCreated = 3, labelsAdded = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness when the ON MATCH includes writing to a right-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET a:Bar"

    assertStats(executeWithRulePlanner(query), relationshipsCreated = 3, labelsAdded = 1)
    assertNumberOfEagerness(query, 1)
  }

  test("should not introduce eagerness when the ON CREATE includes writing to a left-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON CREATE SET b:Foo"

    assertStats(executeWithRulePlanner(query), relationshipsCreated = 3, labelsAdded = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness when the ON CREATE includes writing to a right-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON CREATE SET a:Bar"

    assertStats(executeWithRulePlanner(query), relationshipsCreated = 3, labelsAdded = 2)
    assertNumberOfEagerness(query, 1)
  }

  test("should not add eagerness when reading and merging nodes and relationships when matching different label") {
    createLabeledNode("A")
    val query = "MATCH (a:A) MERGE (a)-[:BAR]->(b:B) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN a"

    assertStats(executeWithRulePlanner(query), relationshipsCreated = 1, nodesCreated = 1, labelsAdded = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("should add eagerness when reading and merging nodes and relationships on matching same label") {
    val node0 = createLabeledNode("A")
    val node1 = createLabeledNode("A")
    val node2 = createLabeledNode("A")
    relate(node1, node2, "BAR")

    val query = "MATCH (a:A) MERGE (a)-[:BAR]->(b:A) WITH a MATCH (a2) RETURN count (a2) AS nodes"

    val result: InternalExecutionResult = executeWithRulePlanner(query)
    assertStats(result, nodesCreated = 2, relationshipsCreated = 2, labelsAdded = 2)

    result.toList should equal(List(Map("nodes" -> 15)))
    assertNumberOfEagerness(query, 1)
  }

  test("should not add eagerness when reading nodes and merging relationships") {
    createLabeledNode("A")
    createLabeledNode("B")
    val query = "MATCH (a:A), (b:B) MERGE (a)-[:BAR]->(b) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN a"

    assertStats(executeWithRulePlanner(query), relationshipsCreated = 1)
    assertNumberOfEagerness(query, 0)
  }

  // TESTS WITH MULTIPLE MERGES

  test("should not be eager when merging on two different labels") {
    val query = "MERGE(:L1) MERGE(p:L2) ON CREATE SET p.name = 'Blaine'"

    assertStats(executeWithRulePlanner(query), nodesCreated = 2, propertiesSet = 1, labelsAdded = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("does not need to be eager when merging on the same label, merges match") {
    createLabeledNode("L1")
    createLabeledNode("L1")
    val query = "MERGE(:L1) MERGE(p:L1) ON CREATE SET p.name = 'Blaine'"

    assertStats(executeWithRulePlanner(query))
    assertNumberOfEagerness(query, 0)
  }

  test("does not need to be eager when merging on the same label, merges create") {
    createNode()
    val query = "MERGE(:L1) MERGE(p:L1) ON CREATE SET p.name = 'Blaine'"

    assertStats(executeWithRulePlanner(query), nodesCreated = 1, labelsAdded = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("does not need to be eager when right side creates nodes for left side, merges match") {
    createNode()
    createLabeledNode("Person")
    val query = "MERGE() MERGE(p: Person) ON CREATE SET p.name = 'Blaine'"

    assertStats(executeWithRulePlanner(query))
    assertNumberOfEagerness(query, 0)
  }

  test("does not need to be eager when right side creates nodes for left side, 2nd merge create") {
    createNode()
    createNode()
    val query = "MERGE() MERGE(p: Person) ON CREATE SET p.name = 'Blaine'"

    assertStats(executeWithRulePlanner(query), nodesCreated = 1, labelsAdded = 1, propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("does not need to be eager when no merge has labels, merges match") {
    createNode()
    val query = "MERGE() MERGE(p) ON CREATE SET p.name = 'Blaine'"

    assertStats(executeWithRulePlanner(query))
    assertNumberOfEagerness(query, 0)
  }

  test("does not need to be eager when no merge has labels, merges create") {
    val query = "MERGE() MERGE(p) ON CREATE SET p.name = 'Blaine'"

    assertStats(executeWithRulePlanner(query), nodesCreated = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("should not be eager when merging on already bound variables") {
    val query = "MERGE (city:City) MERGE (country:Country) MERGE (city)-[:IN]->(country)"

    assertStats(executeWithRulePlanner(query), nodesCreated = 2, labelsAdded = 2, relationshipsCreated = 1)
    assertNumberOfEagerness(query,  0)
  }

  // TESTS FOR SET

  test("matching property and writing different property should not be eager") {
    createLabeledNode(Map("prop" -> 5), "Node")
    val query = "MATCH (n:Node {prop:5}) SET n.value = 10 RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching label and setting different label should not be eager") {
    createLabeledNode(Map("prop" -> 5), "Node")
    val query = "MATCH (n:Node) SET n:Lol"

    assertStats(updateWithBothPlanners(query), labelsAdded = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching label and setting same label should not be eager") {
    createLabeledNode(Map("prop" -> 5), "Node")
    val query = "MATCH (n:Lol) SET n:Lol"

    assertStats(updateWithBothPlanners(query), labelsAdded = 0)
    assertNumberOfEagerness(query, 0)
  }

  test("matching label on right-hand side and setting same label should be eager") {
    createLabeledNode("Lol")
    createNode()
    val query = "MATCH (n), (m:Lol) SET n:Lol"

    val result = eengine.execute(s"cypher planner=rule $query")
    assertStatsResult(labelsAdded = 1)(result.queryStatistics())
    assertNumberOfEagerness(query, 1)
  }

  test("matching label on right-hand side and setting same label should be eager and get the count right") {
    createLabeledNode("Two")
    createLabeledNode("Two")
    createNode()
    val query = "MATCH (m1:Two), (m2:Two), (n) SET n:Two RETURN count(*) AS c"

    val result: InternalExecutionResult = updateWithBothPlanners(query)
    assertStats(result, labelsAdded = 1)
    assertNumberOfEagerness(query, 1)
    result.toList should equal(List(Map("c" -> 12)))
  }

  test("matching label on right-hand side and setting different label should not be eager") {
    createLabeledNode("Lol")
    createNode()
    val query = "MATCH (n), (m1:Lol), (m2:Lol) SET n:Rofl"

    assertStats(updateWithBothPlanners(query), labelsAdded = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("setting label in tail should be eager if overlap") {
    createNode()
    createNode()
    createLabeledNode("Foo")
    createLabeledNode("Foo")
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o:Foo) SET n:Foo RETURN count(*)"

    val result = updateWithBothPlanners(query)
    assertStats(result, labelsAdded = 2, nodesCreated = 4)
    assertNumberOfEagerness(query, 1)
  }

  test("setting label in tail should be eager if overlap within tail") {
    createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("B")
    val query = "MATCH (n:A) CREATE (m:C) WITH * MATCH (o:B), (p:C) SET p:B RETURN count(*)"

    val result = updateWithBothPlanners(query)
    assertStats(result, labelsAdded = 4, nodesCreated = 2)
    assertNumberOfEagerness(query, 2)
  }

  test("setting label in tail should not be eager if no overlap") {
    createLabeledNode("Foo")
    createLabeledNode("Foo")
    createLabeledNode("Bar")
    createLabeledNode("Bar")
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o:Bar) SET n:Foo RETURN count(*)"

    val result = updateWithBothPlanners(query)
    assertStats(result, labelsAdded = 2, nodesCreated = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("matching property and setting label should not be eager") {
    createNode(Map("name" -> "thing"))
    val query = "MATCH (n {name : 'thing'}) SET n:Lol"

    assertStats(updateWithBothPlanners(query), labelsAdded = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("single simple match followed by set property should not be eager") {
    createNode()
    val query = "MATCH (n) SET n.prop = 5 RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("single property match followed by set property should not be eager") {
    createNode(Map("prop" -> 20))
    val query = "MATCH (n { prop: 20 }) SET n.prop = 10 RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("single label match followed by set property should not be eager") {
    createLabeledNode("Node")
    val query = "MATCH (n:Node) SET n.prop = 10 RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("single label+property match followed by set property should not be eager") {
    val query = "MATCH (n:Node {prop:5}) SET n.prop = 10"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property via index and writing same property should not be eager") {
    executeWithRulePlanner("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")
    updateWithBothPlanners("CREATE (b:Book {isbn : '123'})")

    val query = "MATCH (b :Book {isbn : '123'}) SET b.isbn = '456'"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property via index and writing same property should be eager") {
    executeWithRulePlanner("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")
    updateWithBothPlanners("CREATE (b:Book {isbn : '123'})")

    val query = "MATCH (a), (b :Book {isbn : '123'}) SET a.isbn = '456' RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 1)
  }

  test("match property on right-side followed by property write on left-side match needs eager") {
    createNode()
    createNode(Map("id" -> 0))
    val query = "MATCH (a),(b {id: 0}) SET a.id = 0 RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 2)
    assertNumberOfEagerness(query, 1)
  }

  test("match property on right-side followed by property write on right-side match needs eager") {
    val query = "MATCH (a),(b {id: 0}) SET b.id = 1 RETURN count(*)"

    assertStats(updateWithBothPlanners(query))
    assertNumberOfEagerness(query, 1)
  }

  test("match property on left-side followed by property write does not need eager") {
    createNode()
    createNode(Map("id" -> 0))
    val query = "MATCH (b {id: 0}) SET b.id = 1 RETURN count(*)"
    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching property using RegEx and writing should be eager") {
    createNode(Map("prop" -> "Fooo"))
    val query = "MATCH (n) WHERE n.prop =~ 'Foo*' SET n.prop = 'bar' RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching property using REPLACE and writing should be eager") {
    createNode(Map("prop" -> "baz"))
    val query = "MATCH (n) WHERE replace(n.prop, 'foo', 'bar') = 'baz' SET n.prop = 'qux' RETURN count(*)"
    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching property using SUBSTRING and writing should be eager") {
    createNode(Map("prop" -> "aafooaaa"))
    val query = "MATCH (n) WHERE substring(n.prop, 2, 3) = 'foo' SET n.prop = 'bar' RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching property using LEFT and writing should be eager") {
    createNode(Map("prop" -> "fooaaa"))
    val query = "MATCH (n) WHERE left(n.prop, 3) = 'foo' SET n.prop = 'bar' RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching property using RIGHT and writing should be eager") {
    createNode(Map("prop" -> "aaafoo"))
    val query = "MATCH (n) WHERE right(n.prop, 3) = 'foo' SET n.prop = 'bar' RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching property using SPLIT and writing should be eager") {
    createNode(Map("prop" -> "foo,bar"))
    val query = "MATCH (n) WHERE split(n.prop, ',') = ['foo', 'bar'] SET n.prop = 'baz,qux' RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching node property, writing relationship property should not be eager") {
    relate(createNode(Map("prop" -> 5)),createNode())
    val query = "MATCH (n {prop : 5})-[r]-() SET r.prop = 6 RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching node property, writing same node property should be eager") {
    relate(createNode(Map("prop" -> 5)),createNode())
    val query = "MATCH (n {prop : 5})-[r]-(m) SET m.prop = 5 RETURN count(*)"
    val result = updateWithBothPlanners(query)
    assertStats(result, propertiesSet = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))

    assertNumberOfEagerness(query, 1)
  }

  test("matching relationship property, writing same relationship property should be eager") {
    relate(createNode(), createNode(), "prop" -> 3)
    val query = "MATCH ()-[r {prop : 3}]-() SET r.prop = 6 RETURN count(*) AS c"

    val result = updateWithBothPlanners(query)
    assertStats(result, propertiesSet = 2)
    result.toList should equal(List(Map("c" -> 2)))

    assertNumberOfEagerness(query, 1)
  }

  test("matching relationship property, writing different relationship property should not be eager") {
    relate(createNode(), createNode(), "prop1" -> 3)
    val query = "MATCH ()-[r {prop1 : 3}]-() SET r.prop2 = 6"

    assertStats(updateWithBothPlanners(query), propertiesSet = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("matching relationship property, writing node property should not be eager") {
    relate(createNode(), createNode(), "prop" -> 3)
    val query = "MATCH (n)-[r {prop : 3}]-() SET n.prop = 6 RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("matching on relationship property existence, writing same property should not be eager") {
    relate(createNode(), createNode(), "prop" -> 42)
    relate(createNode(), createNode())

    val query = "MATCH ()-[r]-() WHERE exists(r.prop) SET r.prop = 'foo' RETURN count(*)"

    assertStats(updateWithBothPlanners(query), propertiesSet = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("matching on relationship property existence, writing different property should not be eager") {
    relate(createNode(), createNode(), "prop1" -> 42)
    relate(createNode(), createNode())

    val query = "MATCH ()-[r]-() WHERE exists(r.prop1) SET r.prop2 = 'foo'"

    assertStats(updateWithBothPlanners(query), propertiesSet = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("setting property in tail should be eager if overlap") {
    createNode()
    createNode()
    createNode("prop" -> 42)
    createNode("prop" -> 42)
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o {prop:42}) SET n.prop = 42 RETURN count(*)"

    val result = updateWithBothPlanners(query)
    assertStats(result, propertiesSet = 8, nodesCreated = 4)
    assertNumberOfEagerness(query, 1)
  }

  test("setting property in tail should not be eager if no overlap") {
    createNode()
    createNode()
    createNode("prop" -> 42)
    createNode("prop" -> 42)
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o {prop:42}) SET n.prop2 = 42 RETURN count(*)"

    val result = updateWithBothPlanners(query)
    assertStats(result, propertiesSet = 8, nodesCreated = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("matching node property, writing with += should be eager") {
    relate(createNode(Map("prop" -> 5)),createNode())
    val query = "MATCH (n {prop : 5})-[r]-(m) SET m += {prop: 5} RETURN count(*)"
    val result = updateWithBothPlanners(query)
    assertStats(result, propertiesSet = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))

    assertNumberOfEagerness(query, 1)
  }

  test("matching node property, writing with += should not be eager when we can avoid it") {
    relate(createNode(Map("prop" -> 5)),createNode())
    val query = "MATCH (n {prop : 5})-[r]-(m) SET m += {prop2: 5} RETURN count(*)"
    val result = updateWithBothPlanners(query)
    assertStats(result, propertiesSet = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))

    assertNumberOfEagerness(query, 0)
  }

  test("matching node property, writing with += should be eager when using parameters") {
    relate(createNode(Map("prop" -> 5)),createNode())
    val query = "MATCH (n {prop : 5})-[r]-(m) SET m += {props} RETURN count(*)"
    val result = updateWithBothPlanners(query, "props" -> Map("prop" -> 5))
    assertStats(result, propertiesSet = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))

    assertNumberOfEagerness(query, 1)
  }

  test("matching rel property, writing with += should not be eager when we can avoid it") {
    relate(createNode(Map("prop" -> 5)),createNode())
    val query = "MATCH (n {prop : 5})-[r]-(m) SET m += {prop2: 5} RETURN count(*)"
    val result = updateWithBothPlanners(query)
    assertStats(result, propertiesSet = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))

    assertNumberOfEagerness(query, 0)
  }

  //REMOVE LABEL
  test("matching label and removing different label should not be eager") {
    createLabeledNode(Map("prop" -> 5), "Node", "Lol")
    val query = "MATCH (n:Node) REMOVE n:Lol"

    assertStats(updateWithBothPlanners(query), labelsRemoved = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching label and removing same label should not be eager") {
    createLabeledNode(Map("prop" -> 5), "Node")
    val query = "MATCH (n:Node) REMOVE n:Node"

    assertStats(updateWithBothPlanners(query), labelsRemoved = 1)
    assertNumberOfEagerness(query, 0)
  }


  test("matching label on right-hand side and removing same label should not be eager") {
    createLabeledNode("Lol")
    createNode()
    val query = "MATCH  (m:Lol), (n) REMOVE n:Lol RETURN count(*)"

    val result = updateWithBothPlanners(query)

    assertStats(result, labelsRemoved = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("remove same label without anything else reading it should not be eager") {
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")
    val query = "MATCH  (m1:A), (m2:B), (n:C) REMOVE n:C RETURN count(*)"

    val result = updateWithBothPlanners(query)
    assertStats(result, labelsRemoved = 1)
    assertNumberOfEagerness(query, 0)
  }

  test("matching label on right-hand side and removing same label should be eager and get the count right") {
    createLabeledNode("Two")
    createLabeledNode("Two")
    createNode()
    val query = "MATCH (m1:Two), (m2:Two), (n) REMOVE n:Two RETURN count(*) AS c"

    val result = updateWithBothPlanners(query)
    assertStats(result, labelsRemoved = 2)
    assertNumberOfEagerness(query, 1)
    result.toList should equal(List(Map("c" -> 12)))
  }

  test("matching label on right-hand side and removing different label should not be eager") {
    createLabeledNode("Lol", "Rofl")
    createLabeledNode("Lol", "Rofl")
    createLabeledNode("Rofl")
    val query = "MATCH (n), (m1:Lol), (m2:Lol) REMOVE n:Rofl RETURN count(*)"

    assertStats(updateWithBothPlanners(query), labelsRemoved = 3)
    assertNumberOfEagerness(query, 0)
  }

  test("removing label in tail should be eager if overlap") {
    createLabeledNode("Foo")
    createLabeledNode("Foo")
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o:Foo) REMOVE n:Foo RETURN count(*)"

    val result = updateWithBothPlanners(query)
    assertStats(result, labelsRemoved = 2, nodesCreated = 2)
    assertNumberOfEagerness(query, 1)
  }

  test("removing label in tail should not be eager if no overlap") {
    createLabeledNode("Foo")
    createLabeledNode("Foo")
    createLabeledNode("Bar")
    createLabeledNode("Bar")
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o:Bar) REMOVE n:Foo RETURN count(*)"

    val result = updateWithBothPlanners(query)
    assertStats(result, labelsRemoved = 2, nodesCreated = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("undirected expand followed by remove label needn't to be eager") {
    relate(createLabeledNode("Foo"), createLabeledNode("Foo"))
    relate(createLabeledNode("Foo"), createLabeledNode("Foo"))

    val query = "MATCH (n:Foo)--(m) REMOVE m:Foo RETURN count(*)"

    val result = updateWithBothPlanners(query)
    assertStats(result, labelsRemoved = 4)
    assertNumberOfEagerness(query, 0)
  }

  // OTHER TESTS

  test("should understand symbols introduced by FOREACH") {
    createLabeledNode("Label")
    createLabeledNode("Label")
    createLabeledNode("Label2")
    val query =
      """MATCH (a:Label)
        |WITH collect(a) as nodes
        |MATCH (b:Label2)
        |FOREACH(n in nodes |
        |  CREATE UNIQUE (n)-[:SELF]->(b))""".stripMargin

    assertStats(executeWithRulePlanner(query), relationshipsCreated = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("LOAD CSV FROM 'file:///something' AS line MERGE (b:B {p:line[0]}) RETURN b") {
    val query = "LOAD CSV FROM 'file:///something' AS line MERGE (b:B {p:line[0]}) RETURN b"

    assertNumberOfEagerness(query, 0)
  }

  private def assertNumberOfEagerness(query: String, expectedEagerCount: Int) {
    val q = if (query.contains("EXPLAIN")) query else "EXPLAIN CYPHER " + query
    val result = eengine.execute(q)
    val plan = result.executionPlanDescription().toString
    result.close()
    val length = EagerRegEx.findAllIn(plan).length + RepeatableReadRegEx.findAllIn(plan).length
    assert(length == expectedEagerCount, plan)
  }
}
