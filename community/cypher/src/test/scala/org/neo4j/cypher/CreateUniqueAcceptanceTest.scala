/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.junit.Test
import collection.JavaConverters._
import org.scalatest.Assertions
import org.neo4j.graphdb.{Path, Node, Relationship}

class CreateUniqueAcceptanceTest extends ExecutionEngineHelper with Assertions with StatisticsChecker {

  val stats = QueryStatistics()

  @Test
  def create_new_node_with_labels_on_the_right() {
    val a = createNode()
    val b = createNode()
    relate(a, b, "X")

    val result = parseAndExecute("start a = node(1) create unique a-[r:X]->b:FOO return b")
    val createdNode = result.columnAs[Node]("b").toList.head

    assertStats(result, relationshipsCreated =  1, nodesCreated = 1, labelsAdded = 1)
    assertInTx(createdNode.labels === List("FOO"))
  }

  @Test
  def create_new_node_with_labels_on_the_left() {
    val a = createNode()
    val b = createNode()
    relate(a, b, "X")

    val result = parseAndExecute("start b = node(1) create unique a:FOO-[r:X]->b return a")
    val createdNode = result.columnAs[Node]("a").toList.head

    assertStats(result, relationshipsCreated =  1, nodesCreated = 1, labelsAdded = 1)
    assertInTx(createdNode.labels === List("FOO"))
  }

  @Test
  def create_new_node_with_labels_everywhere() {
    val a = createNode()

    val result = parseAndExecute("start a = node(1) create unique a-[:X]->(b:FOO)-[:X]->(c:BAR)-[:X]->(d:BAZ) RETURN d")
    val createdNode = result.columnAs[Node]("d").toList.head

    assertStats(result, relationshipsCreated = 3, nodesCreated = 3, labelsAdded = 3)
    assertInTx(createdNode.labels === List("BAZ"))
  }

  @Test
  def create_new_node_with_labels_and_values() {
    val a = createLabeledNode(Map("name"-> "Andres"), "FOO")
    val b = createNode()
    relate(a, b, "X")

    val result = parseAndExecute(s"start b = node(${b.getId}) create unique (a:FOO {name: 'Andres'})-[r:X]->b return a, b")

    val row: Map[String, Any] = result.toList.head
    val resultA = row("a").asInstanceOf[Node]
    val resultB = row("b").asInstanceOf[Node]

    assertStats(result)
    assert(resultA === a)
    assert(resultB === b)
  }

  @Test
  def create_a_missing_relationship() {
    val a = createNode()
    val b = createNode()

    val result = parseAndExecute("start a = node(1), b=node(2) create unique a-[r:X]->b return r")
    val createdRel = result.columnAs[Relationship]("r").toList.head

    assertStats(result, relationshipsCreated = 1)

    val r = a.getRelationships.asScala.head

    assert(createdRel === r)
    assert(r.getStartNode === a)
    assert(r.getEndNode === b)
  }

  @Test
  def should_be_able_to_handle_a_param_as_map() {
    val a = createNode()

    val nodeProps = Map("name"->"Lasse")

    val result = parseAndExecute("start a = node(1) create unique a-[r:X]->({p}) return r", "p" -> nodeProps)
    val createdRel = result.columnAs[Relationship]("r").toList.head

    assertStats(result, relationshipsCreated = 1, nodesCreated = 1, propertiesSet = 1)

    val r = a.getRelationships.asScala.head

    assert(createdRel === r)
    assert(r.getStartNode === a)
    val endNode = r.getEndNode

    assertInTx(endNode.getProperty("name") === "Lasse")
  }

  @Test
  def should_be_able_to_handle_a_param_as_map_in_a_path() {
    val nodeProps = Map("name"->"Lasse")

    val result = parseAndExecute("start a = node(0) create unique path=a-[:X]->({p}) return last(path)", "p" -> nodeProps)
    val endNode = result.columnAs[Node]("last(path)").toList.head

    assertStats(result, relationshipsCreated = 1, nodesCreated = 1, propertiesSet = 1)

    assertInTx(endNode.getProperty("name") === "Lasse")
  }

  @Test
  def should_be_able_to_handle_two_params() {
    val props1 = Map("name"->"Andres", "position"->"Developer")
    val props2 = Map("name"->"Lasse", "awesome"->true)

    val result = parseAndExecute("start n=node(0) create unique n-[:REL]->(a {props1})-[:LER]->(b {props2}) return a,b", "props1"->props1, "props2"->props2)

    assertStats(result, relationshipsCreated = 2, nodesCreated = 2, propertiesSet = 4)
    val resultMap = result.toList.head

    graph.inTx {
      assert(resultMap("a").asInstanceOf[Node].getProperty("name") === "Andres")
      assert(resultMap("a").asInstanceOf[Node].getProperty("position") === "Developer")
      assert(resultMap("b").asInstanceOf[Node].getProperty("name") === "Lasse")
      assert(resultMap("b").asInstanceOf[Node].getProperty("awesome") === true)
    }
  }

  @Test
  def should_be_able_to_handle_two_params_without_named_nodes() {
    val props1 = Map("name"->"Andres", "position"->"Developer")
    val props2 = Map("name"->"Lasse", "awesome"->true)

    val result = parseAndExecute("start n=node(0) create unique p=n-[:REL]->({props1})-[:LER]->({props2}) return p", "props1"->props1, "props2"->props2)

    assertStats(result, relationshipsCreated = 2, nodesCreated = 2, propertiesSet = 4)
    val path = result.toList.head("p").asInstanceOf[Path]

    val lasse = path.endNode()
    val andres = path.nodes().asScala.toList(1)

    graph.inTx {
      assert(andres.getProperty("name") === "Andres")
      assert(andres.getProperty("position") === "Developer")
      assert(lasse.getProperty("name") === "Lasse")
      assert(lasse.getProperty("awesome") === true)
    }
  }

  @Test
  def should_be_able_to_handle_a_param_as_node() {
    val a = createNode()
    val b = createNode()

    val result = parseAndExecute("start a = node(1) create unique a-[r:X]->({p}) return r", "p" -> b)
    val createdRel = result.columnAs[Relationship]("r").toList.head

    assertStats(result, relationshipsCreated = 1)

    val r = a.getRelationships.asScala.head

    assert(createdRel === r)
    assert(r.getStartNode === a)
    assert(r.getEndNode === b)
  }

  @Test
  def does_not_create_a_missing_relationship() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "X")
    val result = parseAndExecute("start a = node(1), b=node(2) create unique a-[r:X]->b return r")
    val createdRel = result.columnAs[Relationship]("r").toList.head

    assert(a.getRelationships.asScala.size === 1)
    assert(createdRel === r)
  }

  @Test
  def creates_rel_if_it_is_of_wrong_type() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "BAR")
    val result = parseAndExecute("start a = node(1), b=node(2) create unique a-[r:FOO]->b return r")
    val createdRel = result.columnAs[Relationship]("r").toList.head

    assertStats(result, relationshipsCreated = 1)

    assert(a.getRelationships.asScala.size === 2)
    assert(createdRel != r, "A new relationship should have been created")
  }

  @Test
  def creates_minimal_amount_of_nodes() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()

    val result = parseAndExecute("start a = node(1,2), b=node(3), c=node(4) create unique a-[:X]->b-[:X]->c")

    assertStats(result, relationshipsCreated = 3)

    assert(a.getRelationships.asScala.size === 1)
    assert(b.getRelationships.asScala.size === 1)
    assert(c.getRelationships.asScala.size === 3)
    assert(d.getRelationships.asScala.size === 1)
  }

  @Test
  def creates_minimal_amount_of_nodes_reverse() {
    val a = createNode()

    val result = parseAndExecute("start a = node(1) create unique c-[:X]->b-[:X]->a")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 2)

    val aRels = a.getRelationships.asScala.toList
    val bRels = aRels.head.getOtherNode(a).getRelationships.asScala.toList
    assert(aRels.size === 1)
    assert(bRels.size === 2)
  }

  @Test
  def creates_node_if_it_is_missing() {
    val a = createNode()

    val result = parseAndExecute("start a = node(1) create unique a-[:X]->root return root")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1)

    val aRels = a.getRelationships.asScala
    assert(aRels.size === 1)
  }

  @Test
  def creates_node_if_it_is_missing_pattern_reversed() {
    val a = createNode()

    val result = parseAndExecute("start a = node(1) create unique root-[:X]->a return root")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1)

    val aRels = a.getRelationships.asScala
    assert(aRels.size === 1)
  }

  @Test
  def creates_node_if_it_is_missing_a_property() {
    val a = createNode()
    val b = createNode("name" -> "Michael")
    relate(a, b, "X")

    val createdNode = executeScalar[Node]("start a = node(1) create unique a-[:X]->(b {name:'Andres'}) return b")

    assert(b != createdNode, "We should have created a new node - this one doesn't match")
    assertInTx(createdNode.getProperty("name") === "Andres")
  }

  @Test
  def discards_rel_based_on_props() {
    val a = createNode()
    val b = createNode()
    relate(a, b, "X", Map("foo" -> "bar"))

    val createdNode = executeScalar[Node]("start a = node(1) create unique a-[:X {foo:'not bar'}]->b return b")

    assert(b != createdNode, "We should have created a new node - this one doesn't match")
    val createdRelationship = createdNode.getRelationships.asScala.toList.head
    assertInTx(createdRelationship.getProperty("foo") === "not bar")
  }

  @Test
  def discards_rel_based_on_props_between_nodes() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "X", Map("foo" -> "bar"))

    val createdRel = executeScalar[Relationship]("start a = node(1), b = node(2) create unique a-[r:X {foo:'not bar'}]->b return r")

    graph.inTx {
      assert(r != createdRel, "We should have created a new rel - this one doesn't match")
      assert(createdRel.getProperty("foo") === "not bar")
      assert(createdRel.getStartNode === a)
      assert(createdRel.getEndNode === b)
    }
  }

  @Test
  def handle_optional_nulls() {
    createNode()

    intercept[CypherTypeException](parseAndExecute("start a = node(1) match a-[?]->b create unique b-[:X]->c"))
  }

  @Test
  def creates_single_node_if_it_is_missing() {
    val a = createNode()
    val b = createNode()

    val result = parseAndExecute("start a = node(1), b=node(2) create unique a-[:X]->root<-[:X]-b return root")
    assertStats(result, nodesCreated = 1, relationshipsCreated = 2)

    val aRels = a.getRelationships.asScala.toList
    val bRels = b.getRelationships.asScala.toList
    assert(aRels.size === 1)
    assert(bRels.size === 1)

    val aOpposite = aRels.head.getEndNode
    val bOpposite = bRels.head.getEndNode
    assert(aOpposite != b)
    assert(bOpposite != a)

    assert(aOpposite === bOpposite)
  }

  @Test
  def creates_node_with_value_if_it_is_missing() {
    /*        This is the pattern we're looking for
                tagRoot
                   ^
                 / | \
                 a b c
                 \ | /
                   v
                  book


              And this is what we start with:
                tagRoot
                   ^
                 /   \
                 a   c
    */

    val tagRoot = createNode()
    val a = createNode("name" -> "a")
    val c = createNode("name" -> "c")
    relate(tagRoot, a, "tag")
    relate(tagRoot, c, "tag")

    val result = parseAndExecute("""
START root = node(1)
CREATE book
FOREACH(name in ['a','b','c'] |
  CREATE UNIQUE root-[:tag]->(tag {name:name})<-[:tagged]-book
)
return book
""")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 4, propertiesSet = 1)

    val book = result.toList.head("book").asInstanceOf[Node]

    val bookTags = book.getRelationships.asScala.map(_.getOtherNode(book)).toList

    assert(bookTags.contains(a), "Should have been tagged")
    assert(bookTags.contains(c), "Should have been tagged")
  }

  @Test
  def creates_node_with_value_if_it_is_missing_simple() {
    /*        This is the pattern we're looking for
                tagRoot
                   ^
                 / | \
                 a b c
    */

    val tagRoot = createNode()
    val a = createNode("name" -> "a")
    val c = createNode("name" -> "c")
    relate(tagRoot, a, "tag")
    relate(tagRoot, c, "tag")

    val result = parseAndExecute("""
START root = node(1)
FOREACH(name in ['a','b','c'] |
  CREATE UNIQUE root-[:tag]->(tag {name:name})
)
""")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1, propertiesSet = 1)

    graph.inTx {
      val bookTags = tagRoot.getRelationships.asScala.map(_.getOtherNode(tagRoot)).map(_.getProperty("name")).toSet
      assert(bookTags === Set("a", "b", "c"))
    }
  }

  @Test def should_find_nodes_with_properties_first() {
    createNode()
    val b = createNode()
    val wrongX = createNode("foo" -> "absolutely not bar")
    relate(b, wrongX, "X")

    val result = parseAndExecute("""
START a = node(1), b = node(2)
CREATE UNIQUE
  a-[:X]->()-[:X]->(x {foo:'bar'}),
  b-[:X]->x
RETURN x""")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 3, propertiesSet = 1)
  }

  @Test def should_not_create_unnamed_parts_unnecessarily() {
    val a = createNode()
    val b = createNode()

    val result = parseAndExecute("""
START a = node(1), b = node(2)
CREATE UNIQUE
  a-[:X]->()-[:X]->()-[:X]->b""")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 3)
  }

  @Test def should_find_nodes_with_properties_first2() {
    createNode()
    val b = createNode()
    val wrongX = createNode("foo" -> "absolutely not bar")
    relate(b, wrongX, "X")

    val result = parseAndExecute("""
START a = node(1), b = node(2)
CREATE UNIQUE
  a-[:X]->z-[:X]->(x {foo:'bar'}),
  b-[:X]->x-[:X]->(z {foo:'buzz'})
RETURN x""")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 4, propertiesSet = 2)
  }

  @Test def should_work_well_inside_foreach() {
    createNode()
    createNode()
    createNode()

    val result = parseAndExecute("""
START a = node(*)
WITH collect(a) as nodes
FOREACH( x in nodes |
      FOREACH( y in nodes |
          CREATE UNIQUE x-[:FOO]->y
      )
)""")

    // Nodes: Reference plus 3 created nodes.
    // One outgoing node to all nodes (including to self) = 4 x 4 relationships created
    assertStats(result, relationshipsCreated = 16)
  }


  @Test
  def two_outgoing_parts() {
    val a = createNode()
    val b1 = createNode()
    val b2 = createNode()

    relate(a,b1,"X")
    relate(a,b2,"X")

    intercept[UniquePathNotUniqueException](parseAndExecute("""START a = node(1) CREATE UNIQUE a-[:X]->b-[:X]->d"""))
  }
  @Test
  def tree_structure() {
    val a = createNode()

    val result = parseAndExecute("""START root=node(1)
CREATE (value {year:2012, month:5, day:11})
WITH root,value
CREATE UNIQUE root-[:X]->(year {value:value.year})-[:X]->(month {value:value.month})-[:X]->(day {value:value.day})-[:X]->value
RETURN value;""")

    /*
             root
              |
             2012
              |
              5
              |
              11
              |
             v1
     */

    assertStats(result, nodesCreated = 4, relationshipsCreated = 4, propertiesSet = 6)

    val result2 = parseAndExecute("""START root=node(1)
CREATE (value { year:2012, month:5, day:12 })
WITH root,value
CREATE UNIQUE root-[:X]->(year {value:value.year})-[:X]->(month {value:value.month})-[:X]->(day {value:value.day})-[:X]->value
RETURN value;""")

    assertStats(result2, nodesCreated = 2, relationshipsCreated = 2, propertiesSet = 4)

    /*
             root
              |
             2012
              |
              5
             / \
            11   12
                 |
            v1  v2
     */
  }
}
