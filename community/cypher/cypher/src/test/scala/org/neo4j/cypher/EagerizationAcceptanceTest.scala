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

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.matching.Regex

class EagerizationAcceptanceTest extends ExecutionEngineFunSuite with TableDrivenPropertyChecks with QueryStatisticsTestSupport {
  val EagerRegEx: Regex = "Eager(?!A)".r

  // TESTS FOR DELETE AND MERGE

  test("should introduce eagerness between DELETE and MERGE for node") {
    createLabeledNode(Map("value" -> 0), "B")
    createLabeledNode(Map("value" -> 1), "B")
    val query =
      """
        |MATCH (b:B)
        |DELETE b
        |MERGE (b2:B { value: 1 })
        |RETURN b2
      """.stripMargin

    assertNumberOfEagerness(query, 1)
    assertStats(execute(query), nodesCreated = 1, nodesDeleted = 2, propertiesSet = 1, labelsAdded = 1)
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

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query), nodesCreated = 1, nodesDeleted = 2)
  }

  ignore("should not introduce eagerness between DELETE and MERGE for nodes when deleting identifier not bound for same label") {
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

    assertStats(execute(query), nodesCreated = 0, nodesDeleted = 2)
    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness between DELETE and MERGE for relationship") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")
    relate(a, b, "T")
    val query =
      """
        |MATCH (a)-[t:T]->(b)
        |DELETE t
        |MERGE (a)-[t2:T]->(b)
        |RETURN t2
      """.stripMargin

    assertNumberOfEagerness(query, 1)
    assertStats(execute(query), relationshipsDeleted = 2, relationshipsCreated = 1)
  }

  test("should not introduce eagerness between DELETE and MERGE for relationships when there is no read matching the merge") {
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

    assertStats(execute(query), relationshipsDeleted = 2, relationshipsCreated = 1)
    assertNumberOfEagerness(query, 0)
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

    assertNumberOfEagerness(query, 1)
    assertStats(execute(query), relationshipsDeleted = 2, relationshipsCreated = 1)
  }

  ignore("should not introduce eagerness between DELETE and MERGE for relationships when deleting identifier not bound for same type") {
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

    assertStats(execute(query), relationshipsDeleted = 2)
    assertNumberOfEagerness(query, 0)
  }

  // TESTS FOR MATCH AND CREATE

  test("should not introduce eagerness for MATCH nodes and CREATE relationships") {
    val query = "MATCH a, b CREATE (a)-[:KNOWS]->(b)"

    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness for match create match") {
    createNode()
    createNode()

    val query = "MATCH () CREATE () WITH * MATCH (n) RETURN count(n) AS count"

    assertNumberOfEagerness(query, 1)
    val result = execute(query)
    assertStats(result, nodesCreated = 2)
    result.columnAs[Int]("count").next should equal(8)
  }

  test("should introduce eagerness for match create match create") {
    createNode()
    createNode()

    val query = "MATCH () CREATE () WITH * MATCH () CREATE ()"

    assertNumberOfEagerness(query, 2)
    assertStats(execute(query), nodesCreated = 10)
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

      val result = execute(query)
      assertStats(result, nodesCreated = 6)
      assertNumberOfEagerness(query, 0)
    }
  }

  test("should not introduce eagerness for leaf create match") {
    val query = "CREATE () WITH * MATCH () RETURN 1"

    assertNumberOfEagerness(query, 0)
  }

  test("should not need eagerness for match create with labels") {
    createLabeledNode("L")
    val query = "MATCH (:L) CREATE (:L)"

    val result = execute(query)

    println(result.dumpToString())
    assertNumberOfEagerness(query, 0)
  }

  test("should not need eagerness for match create with labels and property with index") {
    createLabeledNode(Map("id" -> 0), "L")
    graph.createIndex("L", "id")
    val query = "MATCH (:L {id: 0}) CREATE (:L {id:0})"

    val result = execute(query)

    println(result.dumpToString())
    assertNumberOfEagerness(query, 0)
  }

  test("should need eagerness for double match and then create") {
    createNode()
    createNode()
    val query = "MATCH (), () CREATE ()"

    val result = execute(query)

    println(result.dumpToString())
    assertNumberOfEagerness(query, 1)
  }

  test("should not add eagerness when not writing to nodes") {
    val query = "MATCH a, b CREATE (a)-[r:KNOWS]->(b) SET r = { key: 42 }"

    assertNumberOfEagerness(query, 0)
  }

  test("matching using a pattern predicate and creating relationship should not be eager") {
    val query = "MATCH n WHERE n-->() CREATE n-[:T]->()"

    assertNumberOfEagerness(query, 0)
  }

  test("should not be eager when creating single node after matching on pattern with relationship") {
    val query = "MATCH ()--() CREATE ()"

    assertNumberOfEagerness(query,  0)
  }

  ignore("should not be eager when creating single node after matching on pattern with relationship and also matching on label") {
    // TODO: Implement RelationShipBoundNodeEffect
    val query = "MATCH (:L) MATCH ()--() CREATE ()"

    assertNumberOfEagerness(query,  0)
  }

  test("should not be eager when creating single node after matching on empty node") {
    val query = "MATCH () CREATE ()"

    assertNumberOfEagerness(query,  0)
  }

  test("should not introduce an eager pipe between two node reads and a relationships create") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) CREATE (a)-[:TYPE]->(b)"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query), relationshipsCreated = 4)
  }

  test("should not introduce an eager pipe between two node reads and a relationships create when there is sorting between the two") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) WITH a, b ORDER BY id(a) CREATE (a)-[:TYPE]->(b)"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query), relationshipsCreated = 4)
  }

  test("should not introduce an eager pipe between a leaf node read and a relationship + node create") {
    createNode()
    createNode()
    val query = "MATCH (a) CREATE (a)-[:TYPE]->()"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query), nodesCreated = 2, relationshipsCreated = 2)
  }

  test("should introduce an eager pipe between a non-leaf node read and a relationship + node create") {
    createNode()
    createNode()
    val query = "MATCH (), (a) CREATE (a)-[:TYPE]->()"

    assertNumberOfEagerness(query, 1)
    assertStats(execute(query), nodesCreated = 4, relationshipsCreated = 4)
  }

  test("should not introduce an eager pipe between a leaf relationship read and a relationship create") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH (a)-[:TYPE]->(b) CREATE (a)-[:TYPE]->(b)"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query), relationshipsCreated = 2)
  }

  test("should introduce an eager pipe between a non-leaf relationship read, rel uniqueness, and a relationship create") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH ()-[:TYPE]->(), (a)-[:TYPE]->(b) CREATE (a)-[:TYPE]->(b)"

    assertNumberOfEagerness(query, 1)
    assertStats(execute(query), relationshipsCreated = 2)
  }

  test("should introduce an eager pipe between a non-leaf relationship read and a relationship create") {
    relate(createLabeledNode("LabelOne"), createLabeledNode("LabelTwo"), "TYPE")
    relate(createLabeledNode("LabelOne"), createLabeledNode("LabelTwo"), "TYPE")
    val query = "MATCH ()-[:TYPE]->() MATCH (a:LabelOne)-[:TYPE]->(b:LabelTwo) CREATE (a)-[:TYPE]->(b)"

    assertNumberOfEagerness(query, 1)
    assertStats(execute(query), relationshipsCreated = 4)
  }

  ignore("should not introduce an eager pipe between a non-leaf relationship read and a relationship create on different nodes") {
    // TODO: ExecuteUpdateCommandsPipe could interpret that it creates only rel-bound nodes in this case
    relate(createLabeledNode("LabelOne"), createLabeledNode("LabelTwo"), "TYPE")
    relate(createLabeledNode("LabelOne"), createLabeledNode("LabelTwo"), "TYPE")
    val query = "MATCH ()-[:TYPE]->() MATCH (a:LabelOne)-[:TYPE]->(b:LabelTwo) CREATE ()-[:TYPE]->()"

    assertStats(execute(query), relationshipsCreated = 4, nodesCreated = 8)
    assertNumberOfEagerness(query, 0)
  }

  ignore("should not introduce eagerness for a non-leaf match on simple pattern and create single node") {
    // TODO: SimplePatternMatcher uses an AllNodes item in a NodeStartPipe, losing us information
    // about the match actually being on relationship-bound nodes.
    relate(createNode(), createNode())
    relate(createNode(), createNode())
    val query = "MATCH ()-->() MATCH ()-->() CREATE ()"

    assertStats(execute(query), nodesCreated = 4)
    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness for match - create on different relationship types") {
    relate(createNode(), createNode(), "T1")
    relate(createNode(), createNode(), "T1")
    val query = "MATCH ()-[:T1]->() CREATE ()-[:T2]->()"

    assertStats(execute(query), nodesCreated = 4, relationshipsCreated = 2)
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

    assertNumberOfEagerness(query, 1)

    assertStats(execute(query), nodesDeleted = 4)
  }

  test("should not introduce eagerness when deleting nodes on single leaf") {
    val node0 = createLabeledNode("Person")
    val node1 = createLabeledNode("Person")
    val node2 = createLabeledNode("Movie")

    val query = "MATCH (a:Person) DELETE a"

    assertNumberOfEagerness(query, 0)

    assertStats(execute(query), nodesDeleted = 2)
  }

  // TESTS USING OPTIONAL MATCHES

  test("should need eagerness for match optional match create") {
    createLabeledNode("A", "B")
    createLabeledNode("A", "B")
    createLabeledNode("A")
    val query = "MATCH (a:A) OPTIONAL MATCH (b:B) CREATE (:B)"

    val result = execute(query)

    assertNumberOfEagerness(query, 1)
    assertStats(result, nodesCreated = 6, labelsAdded = 6)
  }

  test("should not need eagerness for match optional match create where labels do not interfere") {
    createLabeledNode("A", "B")
    createLabeledNode("A", "B")
    createLabeledNode("A")
    val query = "MATCH (a:A) OPTIONAL MATCH (b:B) CREATE (:A)"

    val result = execute(query)

    assertNumberOfEagerness(query, 0)
    assertStats(result, nodesCreated = 6, labelsAdded = 6)
  }

  test("should not introduce eagerness when deleting things on optional matches that aren't cartesian products") {
    val node0 = createLabeledNode("Person")
    val node1 = createLabeledNode("Person")
    val node2 = createNode()
    relate(node0, node2)
    relate(node0, node2)
    relate(node0, node2)
    relate(node1, node2)
    relate(node1, node2)
    relate(node1, node2)

    val query = "MATCH (a:Person) OPTIONAL MATCH (a)-[r1]-() DELETE a, r1"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query), nodesDeleted = 2, relationshipsDeleted = 6)
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

    assertStats(execute(query), nodesDeleted = 3, relationshipsDeleted = 4)
    assertNumberOfEagerness(query, 1)
  }

  test("MATCH (a:Person),(m:Movie) CREATE (a)-[:T]->(m) WITH a OPTIONAL MATCH (a) RETURN *") {
    val query = "MATCH (a:Person),(m:Movie) CREATE (a)-[:T]->(m) WITH a OPTIONAL MATCH (a) RETURN *"

    assertNumberOfEagerness(query, 0)
  }

  // TESTS FOR MATCH AND MERGE

  test("should not introduce eagerness for MATCH nodes and CREATE UNIQUE relationships") {
    val query = "MATCH a, b CREATE UNIQUE (a)-[r:KNOWS]->(b)"

    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness for MATCH nodes and MERGE relationships") {
    val query = "MATCH a, b MERGE (a)-[r:KNOWS]->(b)"

    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness when the ON MATCH includes writing to a non-matched property") {
    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET a.prop = 42"

    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness when the ON MATCH includes writing to a left-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET b:Foo"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query), relationshipsCreated = 3, labelsAdded = 1)
  }

  test("should introduce eagerness when the ON MATCH includes writing to a right-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET a:Bar"

    assertNumberOfEagerness(query, 1)
    assertStats(execute(query), relationshipsCreated = 3, labelsAdded = 1)
  }

  test("should not introduce eagerness when the ON CREATE includes writing to a left-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON CREATE SET b:Foo"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query), relationshipsCreated = 3, labelsAdded = 2)
  }

  test("should introduce eagerness when the ON CREATE includes writing to a right-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON CREATE SET a:Bar"

    assertNumberOfEagerness(query, 1)
    assertStats(execute(query), relationshipsCreated = 3, labelsAdded = 2)
  }

  test("should not add eagerness when reading and merging nodes and relationships when matching different label") {
    val query = "MATCH (a:A) MERGE (a)-[:BAR]->(b:B) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN a"

    assertNumberOfEagerness(query, 0)
  }

  test("should add eagerness when reading and merging nodes and relationships on matching same label") {
    val node0 = createLabeledNode("A")
    val node1 = createLabeledNode("A")
    val node2 = createLabeledNode("A")
    relate(node1, node2, "BAR")

    val query = "MATCH (a:A) MERGE (a)-[:BAR]->(b:A) WITH a MATCH (a2) RETURN count (a2) AS nodes"

    assertNumberOfEagerness(query, 1)
    val result: InternalExecutionResult = execute(query)
    assertStats(result, nodesCreated = 2, relationshipsCreated = 2, labelsAdded = 2)

    result.toList should equal(List(Map("nodes" -> 15)))
  }

  test("should not add eagerness when reading nodes and merging relationships") {
    val query = "MATCH (a:A), (b:B) MERGE (a)-[:BAR]->(b) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN a"

    assertNumberOfEagerness(query, 0)
  }

  // TESTS WITH MULTIPLE MERGES

  test("should not be eager when merging on two different labels") {
    val query = "MERGE(:L1) MERGE(p:L2) ON CREATE SET p.name = 'Blaine'"

    assertNumberOfEagerness(query, 0)
  }

  test("does not need to be eager when merging on the same label, merges match") {
    createLabeledNode("L1")
    createLabeledNode("L1")
    val query = "MERGE(:L1) MERGE(p:L1) ON CREATE SET p.name = 'Blaine'"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query))
  }

  test("does not need to be eager when merging on the same label, merges create") {
    createNode()
    val query = "MERGE(:L1) MERGE(p:L1) ON CREATE SET p.name = 'Blaine'"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query), nodesCreated = 1, labelsAdded = 1)
  }

  test("does not need to be eager when right side creates nodes for left side, merges match") {
    createNode()
    createLabeledNode("Person")
    val query = "MERGE() MERGE(p: Person) ON CREATE SET p.name = 'Blaine'"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query))
  }

  test("does not need to be eager when right side creates nodes for left side, 2nd merge create") {
    createNode()
    createNode()
    val query = "MERGE() MERGE(p: Person) ON CREATE SET p.name = 'Blaine'"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query), nodesCreated = 1, labelsAdded = 1, propertiesSet = 1)
  }

  test("does not need to be eager when no merge has labels, merges match") {
    createNode()
    val query = "MERGE() MERGE(p) ON CREATE SET p.name = 'Blaine'"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query))
  }

  test("does not need to be eager when no merge has labels, merges create") {
    val query = "MERGE() MERGE(p) ON CREATE SET p.name = 'Blaine'"

    assertNumberOfEagerness(query, 0)
    assertStats(execute(query), nodesCreated = 1)
  }

  test("should not be eager when merging on already bound identifiers") {
    val query = "MERGE (city:City) MERGE (country:Country) MERGE (city)-[:IN]->(country)"

    assertNumberOfEagerness(query,  0)
  }

  // TESTS FOR SET

  test("matching property and writing different property should not be eager") {
    val query = "MATCH (n:Node {prop:5}) SET n.value = 10"

    assertNumberOfEagerness(query, 0)
  }

  test("matching label and writing different label should not be eager") {
    val query = "MATCH (n:Node) SET n:Lol"

    assertNumberOfEagerness(query, 0)
  }

  test("matching label and writing same label should not be eager") {
    val query = "MATCH (n:Lol) SET n:Lol"

    assertNumberOfEagerness(query, 0)
  }
  test("matching label on right-hand side and writing same label should be eager") {
    val query = "MATCH (n), (m:Lol) SET n:Lol"

    assertNumberOfEagerness(query, 1)
  }
  test("matching label on right-hand side and writing different label should not be eager") {
    val query = "MATCH (n), (m:Lol) SET n:Rofl"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property and writing label should not be eager") {
    val query = "MATCH (n {name : 'thing'}) SET n:Lol"

    assertNumberOfEagerness(query, 0)
  }

  test("single simple match followed by set property should not be eager") {
    val query = "MATCH (n) SET n.prop = 5"

    assertNumberOfEagerness(query, 0)
  }

  test("single property match followed by set property should not be eager") {
    val query = "MATCH (n { prop: 20 }) SET n.prop = 10"

    assertNumberOfEagerness(query, 0)
  }

  test("single label match followed by set property should not be eager") {
    val query = "MATCH (n:Node) SET n.prop = 10"

    assertNumberOfEagerness(query, 0)
  }

  test("single label+property match followed by set property should not be eager") {
    val query = "MATCH (n:Node {prop:5}) SET n.prop = 10"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property via index and writing same property should not be eager") {
    execute("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")
    execute("CREATE (b:Book {isbn : '123'})")

    val query = "MATCH (b :Book {isbn : '123'}) SET b.isbn = '456'"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property via index and writing same property should be eager") {
    execute("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")
    execute("CREATE (b:Book {isbn : '123'})")

    val query = "MATCH (a), (b :Book {isbn : '123'}) SET a.isbn = '456'"

    assertNumberOfEagerness(query, 1)
  }

  test("match property on right-side followed by property write on left-side match needs eager") {
    val query = "MATCH a,(b {id: 0}) SET a.id = 0"
    assertNumberOfEagerness(query, 1)
  }

  test("match property on right-side followed by property write on right-side match needs eager") {
    val query = "MATCH a,(b {id: 0}) SET b.id = 1"
    assertNumberOfEagerness(query, 1)
  }

  test("match property on left-side followed by property write does not need eager") {
    val query = "MATCH (b {id: 0}) SET b.id = 1"
    assertNumberOfEagerness(query, 0)
  }

  test("matching property using RegEx and writing should be eager") {
    val query = "MATCH n WHERE n.prop =~ 'Foo*' SET n.prop = 'bar'"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using REPLACE and writing should be eager") {
    val query = "MATCH n WHERE replace(n.prop, 'foo', 'bar') = 'baz' SET n.prop = 'qux'"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using SUBSTRING and writing should be eager") {
    val query = "MATCH n WHERE substring(n.prop, 3, 5) = 'foo' SET n.prop = 'bar'"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using LEFT and writing should be eager") {
    val query = "MATCH n WHERE left(n.prop, 5) = 'foo' SET n.prop = 'bar'"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using RIGHT and writing should be eager") {
    val query = "MATCH n WHERE right(n.prop, 5) = 'foo' SET n.prop = 'bar'"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using SPLIT and writing should be eager") {
    val query = "MATCH n WHERE split(n.prop, ',') = ['foo', 'bar'] SET n.prop = 'baz,qux'"

    assertNumberOfEagerness(query, 0)
  }

  test("matching node property, writing relationship property should not be eager") {
    val query = "MATCH (n {prop : 5})-[r]-() SET r.prop = 6"

    assertNumberOfEagerness(query, 0)
  }

  test("matching relationship property, writing same relationship property should not be eager") {
    val query = "MATCH ()-[r {prop : 3}]-() SET r.prop = 6"

    assertNumberOfEagerness(query, 0)
  }

  test("matching relationship property, writing different relationship property should not be eager") {
    val query = "MATCH ()-[r {prop1 : 3}]-() SET r.prop2 = 6"

    assertNumberOfEagerness(query, 0)
  }

  test("matching relationship property, writing node property should not be eager") {
    val query = "MATCH (n)-[r {prop : 3}]-() SET n.prop = 6"

    assertNumberOfEagerness(query, 0)
  }

  test("matching on relationship property existence, writing same property should not be eager") {
    val query = "MATCH ()-[r]-() WHERE has(r.prop) SET r.prop = 'foo'"

    assertNumberOfEagerness(query, 0)
  }

  test("matching on relationship property existence, writing different property should not be eager") {
    val query = "MATCH ()-[r]-() WHERE has(r.prop1) SET r.prop2 = 'foo'"

    assertNumberOfEagerness(query, 0)
  }

  // OTHER TESTS

  test("should understand symbols introduced by FOREACH") {
    val query =
      """MATCH (a:Label)
        |WITH collect(a) as nodes
        |MATCH (b:Label2)
        |FOREACH(n in nodes |
        |  CREATE UNIQUE (n)-[:SELF]->(b))""".stripMargin

    assertNumberOfEagerness(query, 0)
  }

  test("LOAD CSV FROM 'file:///something' AS line MERGE (b:B {p:line[0]}) RETURN b") {
    val query = "LOAD CSV FROM 'file:///something' AS line MERGE (b:B {p:line[0]}) RETURN b"

    assertNumberOfEagerness(query, 0)
  }

  private def assertNumberOfEagerness(query: String, expectedEagerCount: Int) {
    val q = if (query.contains("EXPLAIN")) query else "EXPLAIN " + query
    val result = execute(q)
    val plan = result.executionPlanDescription().toString
    println(plan)
    result.close()
    val length = EagerRegEx.findAllIn(plan).length
    assert(length == expectedEagerCount, plan)
  }
}
