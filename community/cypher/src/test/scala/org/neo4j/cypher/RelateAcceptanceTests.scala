package org.neo4j.cypher

/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.junit.Test
import collection.JavaConverters._
import org.scalatest.Assertions
import org.neo4j.graphdb.{Node, Relationship}

class RelateAcceptanceTests extends ExecutionEngineHelper with Assertions with StatisticsChecker {

  val stats = QueryStatistics.empty

  @Test
  def create_a_missing_relationship() {
    val a = createNode()
    val b = createNode()

    val result = parseAndExecute("start a = node(1), b=node(2) relate a-[r:X]->b return r")
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
    val result = parseAndExecute("start a = node(1), b=node(2) relate a-[r:X]->b return r")
    val createdRel = result.columnAs[Relationship]("r").toList.head

    assert(a.getRelationships.asScala.size === 1)
    assert(createdRel === r)
  }

  @Test
  def creates_rel_if_it_is_of_wrong_type() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "BAR")
    val result = parseAndExecute("start a = node(1), b=node(2) relate a-[r:FOO]->b return r")
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

    val result = parseAndExecute("start a = node(1,2), b=node(3), c=node(4) relate a-[:X]->b-[:X]->c")

    assertStats(result, relationshipsCreated = 3)

    assert(a.getRelationships.asScala.size === 1)
    assert(b.getRelationships.asScala.size === 1)
    assert(c.getRelationships.asScala.size === 3)
    assert(d.getRelationships.asScala.size === 1)
  }

  @Test
  def creates_minimal_amount_of_nodes_reverse() {
    val a = createNode()

    val result = parseAndExecute("start a = node(1) relate c-[:X]->b-[:X]->a")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 2)

    val aRels = a.getRelationships.asScala.toList
    val bRels = aRels.head.getOtherNode(a).getRelationships.asScala.toList
    assert(aRels.size === 1)
    assert(bRels.size === 2)
  }

  @Test
  def creates_node_if_it_is_missing() {
    val a = createNode()

    val result = parseAndExecute("start a = node(1) relate a-[:X]->root return root")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1)

    val aRels = a.getRelationships.asScala
    assert(aRels.size === 1)
  }

  @Test
  def creates_node_if_it_is_missing_pattern_reversed() {
    val a = createNode()

    val result = parseAndExecute("start a = node(1) relate root-[:X]->a return root")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1)

    val aRels = a.getRelationships.asScala
    assert(aRels.size === 1)
  }

  @Test
  def creates_node_if_it_is_missing_a_property() {
    val a = createNode()
    val b = createNode("name" -> "Michael")
    relate(a, b, "X")

    val createdNode = executeScalar[Node]("start a = node(1) relate a-[:X]->(b {name:'Andres'}) return b")

    assert(b != createdNode, "We should have created a new node - this one doesn't match")
    assert(createdNode.getProperty("name") === "Andres")
  }

  @Test
  def discards_rel_based_on_props() {
    val a = createNode()
    val b = createNode()
    relate(a, b, "X", Map("foo" -> "bar"))

    val createdNode = executeScalar[Node]("start a = node(1) relate a-[:X {foo:'not bar'}]->b return b")

    assert(b != createdNode, "We should have created a new node - this one doesn't match")
    val createdRelationship = createdNode.getRelationships.asScala.toList.head
    assert(createdRelationship.getProperty("foo") === "not bar")
  }

  @Test
  def discards_rel_based_on_props_between_nodes() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "X", Map("foo" -> "bar"))

    val createdRel = executeScalar[Relationship]("start a = node(1), b = node(2) relate a-[r:X {foo:'not bar'}]->b return r")

    assert(r != createdRel, "We should have created a new rel - this one doesn't match")
    assert(createdRel.getProperty("foo") === "not bar")
    assert(createdRel.getStartNode === a)
    assert(createdRel.getEndNode === b)
  }

  @Test
  def handle_optional_nulls() {
    createNode()

    intercept[CypherTypeException](parseAndExecute("start a = node(1) match a-[?]->b relate b-[:X]->c"))
  }


  // discard based on rel-props and node-props
  // two_nodes_with_one_more_rel_created() {
  // start r = rel(0),a=node(0) relate a-[r:X]-b //BLOW UP
  // start a=node(0) match a-[?]-b relate b-[:X]->a //Blow up if b is null
  // test self-relationships
  // double delete and then a create after

  @Test
  def creates_single_node_if_it_is_missing() {
    val a = createNode()
    val b = createNode()

    val result = parseAndExecute("start a = node(1), b=node(2) relate a-[:X]->root<-[:X]-b return root")
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
FOREACH(name in ['a','b','c'] :
  RELATE root-[:tag]->(tag {name:name})<-[:tagged]-book
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
FOREACH(name in ['a','b','c'] :
  RELATE root-[:tag]->(tag {name:name})
)
""")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1, propertiesSet = 1)

    val bookTags = tagRoot.getRelationships.asScala.map(_.getOtherNode(tagRoot)).map(_.getProperty("name")).toSet
    assert(bookTags === Set("a", "b", "c"))
  }

  @Test
  def two_outoing_parts() {
    val a = createNode()
    val b1 = createNode()
    val b2 = createNode()

    relate(a,b1,"X")
    relate(a,b2,"X")

    intercept[RelatePathNotUnique](parseAndExecute("""START a = node(1) RELATE a-[:X]->b-[:X]->d"""))
  }
  @Test
  def tree_structure() {
    val a = createNode()

    val result = parseAndExecute("""START root=node(1)
      CREATE value = { year:2012, month:5, day:11 }
      WITH root,value
      RELATE root-[:X]->(year {value:value.year})-[:X]->(month {value:value.month})-[:X]->(day {value:value.day})-[:X]->value
    return value;""")

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
      CREATE value = { year:2012, month:5, day:12 }
      WITH root,value
      RELATE root-[:X]->(year {value:value.year})-[:X]->(month {value:value.month})-[:X]->(day {value:value.day})-[:X]->value
    return value;""")

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