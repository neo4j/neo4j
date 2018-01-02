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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.graphdb.{Direction, Node, Path, Relationship}

import scala.collection.JavaConverters._

class CreateUniqueAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {

  val stats = QueryStatistics()

  test("should create unique relations in reverse direction when path variable is not specified") {
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")
    val rel = executeScalar[Relationship]("MATCH (a:A), (b:B) CREATE UNIQUE (a)<-[r:X]-(b) RETURN r")
    graph.inTx {
      a.getRelationships(Direction.OUTGOING).iterator().hasNext should equal(false)
      b.getRelationships(Direction.OUTGOING).iterator().hasNext should equal(true)
      rel.getEndNode should be(a)
      rel.getStartNode should be(b)
    }
  }

  test("should create unique relations in reverse direction even when path variable is specified") {
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")
    val rel = executeScalar[Relationship]("MATCH (a:A), (b:B) CREATE UNIQUE p = (a)<-[r:X]-(b) RETURN r")
    graph.inTx {
      a.getRelationships(Direction.OUTGOING).iterator().hasNext should equal(false)
      b.getRelationships(Direction.OUTGOING).iterator().hasNext should equal(true)
      rel.getEndNode should be(a)
      rel.getStartNode should be(b)
    }
  }

  test("create unique accepts undirected relationship") {
    createNode("id" -> 1)
    createNode("id" -> 2)

    val rel = executeScalar[Relationship]("MATCH (a {id: 1}), (b {id: 2}) CREATE UNIQUE (a)-[r:X]-(b) RETURN r")
    graph.inTx {
      rel.getStartNode.getProperty("id") should equal(1)
      rel.getEndNode.getProperty("id") should equal(2)
    }
  }

  test("create unique accepts undirected relationship with second node needing to be created") {
    createNode("existing" -> true)

    val rel = executeScalar[Relationship]("MATCH (a {existing: true}) CREATE UNIQUE (a)-[r:X]-(b) RETURN r")
    graph.inTx {
      // De-facto behavior in Rule planner was to create relationships from new node to existing node
      rel.getStartNode.hasProperty("existing") should equal(false)
      rel.getEndNode.hasProperty("existing") should equal(true)
    }
  }

  test("create unique accepts undirected relationship with first node needing to be created") {
    createNode("existing" -> true)

    val rel = executeScalar[Relationship]("MATCH (b {existing: true}) CREATE UNIQUE (a)-[r:X]-(b) RETURN r")
    graph.inTx {
      // De-facto behavior in Rule planner was to create relationships from new node to existing node
      rel.getStartNode.hasProperty("existing") should equal(false)
      rel.getEndNode.hasProperty("existing") should equal(true)
    }
  }

  test("create_new_node_with_labels_on_the_right") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "X")

    val result = execute("match (a) where id(a) = 0 create unique a-[r:X]->(b:FOO) return b")
    val createdNode = result.columnAs[Node]("b").toList.head

    assertStats(result, relationshipsCreated =  1, nodesCreated = 1, labelsAdded = 1)
    graph.inTx {
      createdNode.labels should equal(List("FOO"))
    }
  }

  //zendesk issue 2038
  test("test_create_unique_with_array_properties_as_parameters") {
    // given
    createNode()
    createNode()
    val nodeProps1: Map[String, Any] = Map("foo"-> Array("Pontus"))
    val nodeProps2: Map[String, Any] = Map("foo"-> Array("Pontus"))

    // when
    val query = "MATCH (s) WHERE id(s) = 0 CREATE UNIQUE s-[:REL]->(n:label {param}) RETURN n"
    val res1 = execute(query,"param" -> nodeProps1)
    val res2 = execute(query, "param" -> nodeProps2)

    //then
    assertStats(res1, nodesCreated = 1, relationshipsCreated = 1, propertiesSet = 1, labelsAdded = 1)
    assertStats(res2, nodesCreated = 0, relationshipsCreated = 0, propertiesSet = 0, labelsAdded = 0)
  }

  test("create_new_node_with_labels_on_the_left") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "X")

    val result = execute("match (b) where id(b) = 0 create unique (a:FOO)-[r:X]->b return a")
    val createdNode = result.columnAs[Node]("a").toList.head

    assertStats(result, relationshipsCreated =  1, nodesCreated = 1, labelsAdded = 1)
    graph.inTx {
      createdNode.labels should equal(List("FOO"))
    }
  }

  test("create_new_node_with_labels_everywhere") {
    val a = createNode()

    val result = execute("match (a) where id(a) = 0 create unique a-[:X]->(b:FOO)-[:X]->(c:BAR)-[:X]->(d:BAZ) RETURN d")
    val createdNode = result.columnAs[Node]("d").toList.head

    assertStats(result, relationshipsCreated = 3, nodesCreated = 3, labelsAdded = 3)
    graph.inTx {
      createdNode.labels should equal(List("BAZ"))
    }
  }

  test("create_new_node_with_labels_and_values") {
    val a = createLabeledNode(Map("name"-> "Andres"), "FOO")
    val b = createNode()
    relate(a, b, "X")

    val result = execute(s"match (b) where id(b) = ${b.getId} create unique (a:FOO {name: 'Andres'})-[r:X]->b return a, b")

    val row: Map[String, Any] = result.toList.head
    val resultA = row("a").asInstanceOf[Node]
    val resultB = row("b").asInstanceOf[Node]

    assertStats(result)
    resultA should equal(a)
    resultB should equal(b)
  }

  test("create_a_missing_relationship") {
    val a = createNode()
    val b = createNode()

    val result = execute("match (a), (b) where id(a) = 0 and id(b) = 1 create unique a-[r:X]->b return r")
    val createdRel = result.columnAs[Relationship]("r").toList.head

    assertStats(result, relationshipsCreated = 1)

    val r = graph.inTx(a.getRelationships.asScala.head)

    assert(createdRel === r)
    graph.inTx {
      r.getStartNode should equal(a)
      r.getEndNode should equal(b)
    }
  }

  test("should_be_able_to_handle_a_param_as_map") {
    val a = createNode()

    val nodeProps = Map("name"->"Lasse")

    val result = execute("match (a) where id(a) = 0 create unique a-[r:X]->({p}) return r", "p" -> nodeProps)
    val createdRel = result.columnAs[Relationship]("r").toList.head

    assertStats(result, relationshipsCreated = 1, nodesCreated = 1, propertiesSet = 1)

    val r = graph.inTx(a.getRelationships.asScala.head)

    createdRel should equal(r)
    graph.inTx {
      r.getStartNode should equal(a)
      r.getEndNode.getProperty("name") should equal("Lasse")
    }
  }

  test("should_be_able_to_handle_a_param_as_map_in_a_path") {
    createNode()
    val nodeProps = Map("name"->"Lasse")

    val result = execute("match (a) where id(a) = 0 create unique path=a-[:X]->({p}) return last(nodes(path))", "p" -> nodeProps)
    val endNode = result.columnAs[Node]("last(nodes(path))").toList.head

    assertStats(result, relationshipsCreated = 1, nodesCreated = 1, propertiesSet = 1)
    graph.inTx {
      endNode.getProperty("name") should equal("Lasse")
    }
  }

  test("should_be_able_to_handle_two_params") {
    createNode()
    val props1 = Map[String, Any]("name"->"Andres", "position"->"Developer")
    val props2 = Map[String, Any]("name"->"Lasse", "awesome"->true)

    val result = execute("match (n) where id(n) = 0 create unique n-[:REL]->(a {props1})-[:LER]->(b {props2}) return a,b", "props1"->props1, "props2"->props2)

    assertStats(result, relationshipsCreated = 2, nodesCreated = 2, propertiesSet = 4)
    val resultMap = result.toList.head

    graph.inTx {
      resultMap("a").asInstanceOf[Node].getProperty("name") should equal("Andres")
      resultMap("a").asInstanceOf[Node].getProperty("position") should equal("Developer")
      resultMap("b").asInstanceOf[Node].getProperty("name") should equal("Lasse")
      resultMap("b").asInstanceOf[Node].getProperty("awesome") should equal(true)
    }
  }

  test("should_be_able_to_handle_two_params_without_named_nodes") {
    createNode()

    val props1 = Map[String, Any]("name"->"Andres", "position"->"Developer")
    val props2 = Map[String, Any]("name"->"Lasse", "awesome"->true)

    val result = execute("match (n) where id(n) = 0 create unique p=n-[:REL]->({props1})-[:LER]->({props2}) return p", "props1"->props1, "props2"->props2)

    assertStats(result, relationshipsCreated = 2, nodesCreated = 2, propertiesSet = 4)
    val path = result.toList.head("p").asInstanceOf[Path]

    val lasse = path.endNode()
    val andres = path.nodes().asScala.toList(1)

    graph.inTx {
      andres.getProperty("name") should equal("Andres")
      andres.getProperty("position") should equal("Developer")
      lasse.getProperty("name") should equal("Lasse")
      lasse.getProperty("awesome") should equal(true)
    }
  }

  test("should_be_able_to_handle_a_param_as_node") {
    val n = createNode()
    intercept[CypherTypeException](execute("match (a) where id(a) = 0 create unique a-[r:X]->({param}) return r", "param" -> n))
  }

  test("does_not_create_a_missing_relationship") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "X")
    val result = execute("match (a), (b) where id(a) = 0 and id(b) = 1 create unique a-[r:X]->b return r")
    val createdRel = result.columnAs[Relationship]("r").toList.head

    createdRel should equal(r)
    graph.inTx {
      a.getRelationships.asScala should have size 1
    }
  }

  test("creates_rel_if_it_is_of_wrong_type") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "BAR")
    val result = execute("match (a), (b) where id(a) = 0 and id(b) = 1 create unique a-[r:FOO]->b return r")
    val createdRel = result.columnAs[Relationship]("r").toList.head

    assertStats(result, relationshipsCreated = 1)

    createdRel should not equal (r)
    graph.inTx {
      a.getRelationships.asScala should have size 2
    }
  }

  test("creates_minimal_amount_of_nodes") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()

    val result = execute("match (a), (b), (c) where id(a) in [0,1] and id(b) = 2 and id(c) = 3 create unique a-[:X]->b-[:X]->c")

    assertStats(result, relationshipsCreated = 3)
    graph.inTx {
      a.getRelationships.asScala should have size 1
      b.getRelationships.asScala should have size 1
      c.getRelationships.asScala should have size 3
      d.getRelationships.asScala should have size 1
    }
  }

  test("creates_minimal_amount_of_nodes_reverse") {
    val a = createNode()

    val result = execute("match (a) where id(a) = 0 create unique c-[:X]->b-[:X]->a")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 2)
    graph.inTx {
      val aRels = a.getRelationships.asScala.toList
      aRels should have size 1
      val bRels = aRels.head.getOtherNode(a).getRelationships.asScala.toList
      bRels should have size 2
    }
  }

  test("creates_node_if_it_is_missing") {
    val a = createNode()

    val result = execute("match (a) where id(a) = 0 create unique a-[:X]->root return root")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1)
    graph.inTx {
      a.getRelationships.asScala should have size 1
    }
  }

  test("creates_node_if_it_is_missing_pattern_reversed") {
    val a = createNode()

    val result = execute("match (a) where id(a) = 0 create unique root-[:X]->a return root")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1)
    graph.inTx {
      a.getRelationships.asScala should have size 1
    }
  }

  test("creates_node_if_it_is_missing_a_property") {
    val a = createNode()
    val b = createNode("name" -> "Michael")
    relate(a, b, "X")

    val createdNode = executeScalar[Node]("match (a) where id(a) = 0 create unique a-[:X]->(b {name:'Andres'}) return b")

    createdNode should not equal b
    graph.inTx {
      createdNode.getProperty("name") should equal("Andres")
    }
  }

  test("discards_rel_based_on_props") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "X", Map("foo" -> "bar"))

    val createdNode = executeScalar[Node]("match (a) where id(a) = 0 create unique a-[:X {foo:'not bar'}]->b return b")

    createdNode should not equal b
    graph.inTx {
      createdNode.getRelationships.asScala.toList.head.getProperty("foo") should equal("not bar")
    }
  }

  test("discards_rel_based_on_props_between_nodes") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b, "X", Map("foo" -> "bar"))

    val createdRel = executeScalar[Relationship]("match (a), (b) where id(a) = 0 and id(b) = 1 create unique a-[r:X {foo:'not bar'}]->b return r")

    createdRel should not equal r

    graph.inTx {
      createdRel.getProperty("foo") should equal("not bar")
      createdRel.getStartNode should equal(a)
      createdRel.getEndNode should equal(b)
    }
  }

  test("creates_single_node_if_it_is_missing") {
    val a = createNode()
    val b = createNode()

    val result = execute("match (a), (b) where id(a) = 0 and id(b) = 1 create unique a-[:X]->root<-[:X]-b return root")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 2)
    graph.inTx {
      val aRels = a.getRelationships.asScala.toList
      val bRels = b.getRelationships.asScala.toList
      aRels should have size 1
      bRels should have size 1

      val aOpposite = aRels.head.getEndNode
      val bOpposite = bRels.head.getEndNode
      aOpposite should not equal b
      bOpposite should not equal a
      aOpposite should equal(bOpposite)
    }
  }

  test("creates_node_with_value_if_it_is_missing") {
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

    val result = execute("""
match (root) where id(root) = 0
CREATE book
FOREACH(name in ['a','b','c'] |
  CREATE UNIQUE root-[:tag]->(tag {name:name})<-[:tagged]-book
)
return book
""")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 4, propertiesSet = 1)

    val book = result.toList.head("book").asInstanceOf[Node]

    graph.inTx {
      val bookTags = book.getRelationships.asScala.map(_.getOtherNode(book)).toList
      bookTags should contain(a)
      bookTags should contain(c)
    }
  }

  test("creates_node_with_value_if_it_is_missing_simple") {
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

    val result = execute("""
match (root) where id(root) = 0
FOREACH(name in ['a','b','c'] |
  CREATE UNIQUE root-[:tag]->(tag {name:name})
)
""")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 1, propertiesSet = 1)

    graph.inTx {
      val bookTags = tagRoot.getRelationships.asScala.map(_.getOtherNode(tagRoot)).map(_.getProperty("name")).toSet
      bookTags should equal(Set("a", "b", "c"))
    }
  }

  test("should_find_nodes_with_properties_first") {
    createNode()
    val b = createNode()
    val wrongX = createNode("foo" -> "absolutely not bar")
    relate(b, wrongX, "X")

    val result = execute("""
MATCH (a), (b) WHERE id(a) = 0 AND id(b) = 1
CREATE UNIQUE
  a-[:X]->()-[:X]->(x {foo:'bar'}),
  b-[:X]->x
RETURN x""")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 3, propertiesSet = 1)
  }

  test("should_not_create_unnamed_parts_unnecessarily") {
    val a = createNode()
    val b = createNode()

    val result = execute("""
MATCH (a), (b) WHERE id(a) = 0 AND id(b) = 1
CREATE UNIQUE
  a-[:X]->()-[:X]->()-[:X]->b""")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 3)
  }

  test("should_find_nodes_with_properties_first2") {
    createNode()
    val b = createNode()
    val wrongX = createNode("foo" -> "absolutely not bar")
    relate(b, wrongX, "X")

    val result = execute("""
MATCH (a), (b) WHERE id(a) = 0 AND id(b) = 1
CREATE UNIQUE
  a-[:X]->z-[:X]->(x {foo:'bar'}),
  b-[:X]->x-[:X]->(z {foo:'buzz'})
RETURN x""")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 4, propertiesSet = 2)
  }

  test("should_work_well_inside_foreach") {
    createNode()
    createNode()
    createNode()

    val result = execute("""
MATCH (a)
WITH collect(a) as nodes
FOREACH( x in nodes |
      FOREACH( y in nodes |
          CREATE UNIQUE x-[:FOO]->y
      )
)""")

    // Nodes: 3 created nodes.
    // One outgoing node to all nodes (including to self) = 3 x 3 relationships created
    assertStats(result, relationshipsCreated = 9)
  }


  test("two_outgoing_parts") {
    val a = createNode()
    val b1 = createNode()
    val b2 = createNode()

    relate(a,b1,"X")
    relate(a,b2,"X")

    intercept[UniquePathNotUniqueException](execute("""match (a) where id(a) = 0 CREATE UNIQUE a-[:X]->b-[:X]->d"""))
  }

  test("tree_structure") {
    val a = createNode()

    val result = execute("""match (root) where id(root) = 0
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

    val result2 = execute("""match (root) where id(root) = 0
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
