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
import org.junit.Assert._
import collection.JavaConverters._
import org.scalatest.Assertions
import org.neo4j.graphdb._
import java.util.HashMap

class MutatingIntegrationTest extends ExecutionEngineHelper with Assertions with StatisticsChecker {

  val stats = QueryStatistics()

  @Test
  def create_a_single_node() {
    val before = graph.getAllNodes.asScala.size

    val result = parseAndExecute("create a")

    assertStats(result, nodesCreated = 1)
    assert(graph.getAllNodes.asScala.size === before + 1)
  }


  @Test
  def create_a_single_node_with_props_and_return_it() {
    val before = graph.getAllNodes.asScala.size

    val result = parseAndExecute("create (a {name : 'Andres'}) return a")

    assertStats(result, nodesCreated = 1, propertiesSet = 1)
    assert(graph.getAllNodes.asScala.size === before + 1)

    assert(graph.getAllNodes.asScala.size === before + 1)

    val list = result.toList
    assert(list.size === 1)
    val createdNode = list.head("a").asInstanceOf[Node]
    assert(createdNode.getProperty("name") === "Andres")
  }

  @Test
  def start_with_a_node_and_create_a_new_node_with_the_same_properties() {
    createNode("age" -> 15)

    val result = parseAndExecute("start a = node(1) with a create (b {age : a.age * 2}) return b")

    assertStats(result, nodesCreated = 1, propertiesSet = 1)

    val list = result.toList
    assert(list.size === 1)
    val createdNode = list.head("b").asInstanceOf[Node]
    assert(createdNode.getProperty("age") === 30)
  }

  @Test
  def create_two_nodes_and_a_relationship_between_them() {
    val result = parseAndExecute("create a, b, a-[r:REL]->b")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  @Test
  def create_one_node_and_dumpToString() {
    val result = parseAndExecute("create (a {name:'Cypher'})")

    assertStats(result,
      nodesCreated = 1,
      propertiesSet = 1
    )
  }

  @Test
  def deletes_single_node() {
    val a = createNode().getId

    val result = parseAndExecute("start a = node(1) delete a")
    assertStats(result,       deletedNodes = 1    )

    assert(result.toList === List())
    intercept[NotFoundException](graph.getNodeById(a))
  }

  @Test
  def multiple_deletes_should_not_break_anything() {
    (1 to 4).foreach(i => createNode())

    val result = parseAndExecute("start a = node(1),b=node(2,3,4) delete a")
    assertStats(result, deletedNodes = 1)

    assert(result.toList === List())
  }

  @Test
  def deletes_all_relationships() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()

    relate(a, b)
    relate(a, c)
    relate(a, d)

    val result = parseAndExecute("start a = node(1) match a-[r]->() delete r")
    assertStats(result,        deletedRelationships = 3    )

    assert(a.getRelationships.asScala.size === 0)
  }

  @Test
  def create_multiple_relationships_in_one_query() {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    val result = parseAndExecute("create n with n start x = node(1,2,3) create n-[:REL]->x")
    assertStats(result,
      nodesCreated = 1,
      relationshipsCreated = 3
    )

    assert(a.getRelationships.asScala.size === 1)
    assert(b.getRelationships.asScala.size === 1)
    assert(c.getRelationships.asScala.size === 1)
  }

  @Test
  def set_a_property() {
    val a = createNode("name" -> "Andres")

    val result = parseAndExecute("start n=node(1) set n.name = 'Michael'")
    assertStats(result,      propertiesSet = 1    )

    assert(a.getProperty("name") === "Michael")
  }

  @Test
  def set_a_property_to_an_expression() {
    val a = createNode("name" -> "Andres")

    val result = parseAndExecute("start n=node(1) set n.name = n.name + ' was here'")
    assertStats(result,
      propertiesSet = 1
    )

    assert(a.getProperty("name") === "Andres was here")
  }

  @Test
  def set_a_property_to_a_collection() {
    createNode("Andres")
    createNode("Michael")
    createNode("Peter")

    val result = parseAndExecute("start n=node(1,2,3) with collect(n.name) as names create ({name : names})")
    assertStats(result,
      propertiesSet = 1,
      nodesCreated = 1
    )

    assert(graph.getNodeById(4).getProperty("name") === Array("Andres", "Michael", "Peter"))
  }

  @Test
  def set_a_property_to_an_empty_collection() {
    createNode("Andres")

    val result = parseAndExecute("start n=node(1) with filter(x in collect(n.name) : x = 12) as names create ({x : names})")
    assertStats(result,
      propertiesSet = 1,
      nodesCreated = 1
    )

    assert(graph.getNodeById(2).getProperty("x") === Array())
  }

  @Test
  def create_node_from_map_values() {
    parseAndExecute("create (n {a}) return n", "a" -> Map("name" -> "Andres", "age" -> 66))
    val n = graph.createdNodes.dequeue()
    assert(n.getProperty("name") === "Andres")
    assert(n.getProperty("age") === 66)
  }

  @Test
  def set_property_for_null_removes_the_property() {
    val n = createNode("name" -> "Michael")
    parseAndExecute("start n = node(1) set n.name = null return n")

    assertFalse("Property should have been removed", n.hasProperty("name"))
  }

  @Test
  def create_rel_from_map_values() {
    createNode()
    createNode()

    val r = parseAndExecute("start a = node(1), b = node(2) create a-[r:REL {param}]->b return r", "param" -> Map("name" -> "Andres", "age" -> 66)).
      toList.head("r").asInstanceOf[Relationship]

    assert(r.getProperty("name") === "Andres")
    assert(r.getProperty("age") === 66)
  }

  @Test
  def mark_nodes_in_path() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b)
    relate(b, c)

    val q = """
start a = node(1), c = node(3)
match p=a-->b-->c
=== p ===
foreach(n in nodes(p) :
  set n.marked = true
)
            """

    parseAndExecute(q)

    assertTrue(a.getProperty("marked").asInstanceOf[Boolean])
    assertTrue(b.getProperty("marked").asInstanceOf[Boolean])
    assertTrue(c.getProperty("marked").asInstanceOf[Boolean])
  }

  @Test
  def match_and_delete() {
    val a = createNode()
    val b = createNode()

    relate(a, b, "HATES")
    relate(a, b, "LOVES")

    intercept[NodeStillHasRelationshipsException](parseAndExecute("start n = node(1) match n-[r:HATES]->() delete n,r"))
  }

  @Test
  def delete_and_return() {
    val a = createNode()

    val result = parseAndExecute("start n = node(1) delete n return n").toList
    assert(result === List(Map("n" -> a)))
  }

  @Test
  def create_multiple_nodes() {
    val maps = List(
      Map("name" -> "Andres", "prefers" -> "Scala"),
      Map("name" -> "Michael", "prefers" -> "Java"),
      Map("name" -> "Peter", "prefers" -> "Java"))

    val result = parseAndExecute("create ({params})", "params" -> maps)

    assertStats(result,
      nodesCreated = 3,
      propertiesSet = 6
    )
  }

  @Test
  def create_multiple_nodes_and_return() {
    val maps = List(
      Map("name" -> "Andres"),
      Map("name" -> "Michael"),
      Map("name" -> "Peter"))

    val result = parseAndExecute("create (n {params}) return n", "params" -> maps).toList
    assert(result.size === 3)
  }

  @Test
  def fail_to_create_from_two_iterables() {
    val maps1 = List(
      Map("name" -> "Andres"),
      Map("name" -> "Michael"),
      Map("name" -> "Peter"))
    val maps2 = List(
      Map("name" -> "Andres"),
      Map("name" -> "Michael"),
      Map("name" -> "Peter"))

    intercept[ParameterWrongTypeException](parseAndExecute("create (a {params1}), (b {params2})", "params1" -> maps1, "params2" -> maps2))
  }

  @Test
  def first_read_then_write() {
    val root = createNode()
    val a = createNode("Alfa")
    val b = createNode("Beta")
    val c = createNode("Gamma")

    relate(root, a)
    relate(root, b)
    relate(root, c)

    parseAndExecute("start root=node(1) match root-->other create (new {name:other.name}), root-[:REL]->new")

    val result = parseAndExecute("start root=node(1) match root-->other return other.name order by other.name").columnAs[String]("other.name").toList
    assert(result === List("Alfa", "Alfa", "Beta", "Beta", "Gamma", "Gamma"))
  }

  @Test
  def create_node_and_rel_in_foreach() {
    parseAndExecute("""
create center
foreach(x in range(1,10) :
  create (leaf1 {number : x}) , center-[:X]->leaf1
)
return distinct center""")
  }

  @Test
  def extract_on_arrays() {
    val result = parseAndExecute( """start n=node(0) set n.x=[1,2,3] return extract (i in n.x : i/2.0) as x""")
    assert(result.toList === List(Map("x" -> List(0.5, 1.0, 1.5))))
  }

  @Test
  def delete_optionals() {
    createNode()
    val a = createNode()
    val b = createNode()
    relate(a,b)

    parseAndExecute("""start n=node(*) match n-[r?]-() delete n,r""")
    assert(graph.getAllNodes.asScala.size === 0)
  }

  @Test
  def delete_path() {
    val a = createNode()
    val b = createNode()
    relate(a,b)

    parseAndExecute("""start n=node(1) match p=n-->() delete p""")
    assert(graph.getAllNodes.asScala.size === 1)
  }

  @Test
  def string_literals_should_not_be_mistaken_for_identifiers() {
    //https://github.com/neo4j/community/issues/523
    val result = executeScalar[List[Node]]("create (tag1 {name:'tag2'}), (tag2 {name:'tag1'}) return [tag1,tag2] as tags")
    assert(result.size == 2)
  }

  @Test
  def concatenate_to_a_collection() {
    val result = executeScalar[Array[Long]]("create a={foo:[1,2,3]} set a.foo = a.foo + [4,5] return a.foo")

    assert(result.toList === List(1,2,3,4,5))
  }

  @Test
  def concatenate_to_a_collection_in_reverse() {
    val result = executeScalar[Array[Long]]("create a={foo:[3,4,5]} set a.foo = [1,2] + a.foo return a.foo")

    assert(result.toList === List(1,2,3,4,5))
  }

  @Test
  def create_node_from_map_with_array_value_from_java() {
    val list = new java.util.ArrayList[String]()
    list.add("foo")
    list.add("bar")

    val map = new java.util.HashMap[String, Object]()
    map.put("arrayProp", list)

    val q = "create a={param} return a.arrayProp"
    val result =  executeScalar[Array[String]](q, "param" -> map)

    assertStats(parseAndExecute(q, "param"->map), nodesCreated = 1, propertiesSet = 1)
    assert(result.toList === List("foo","bar"))
  }

  @Test
  def failed_query_should_not_leave_dangling_transactions() {
    intercept[NotFoundException](parseAndExecute("START left=node(1), right=node(3,4) CREATE UNIQUE left-[r:KNOWS]->right RETURN r"))

    assertNull("Did not expect to be in a transaction now", graph.getTxManager.getTransaction)
  }

  @Test
  def create_unique_twice_with_param_map() {
    createNode()
    createNode()

    val map1 = Map("name" -> "Anders")
    val map2 = new HashMap[String, Any]()
    map2.put("name", "Anders")

    val r1 = executeScalar[Relationship]("start a=node(1), b=node(2) create unique a-[r:FOO {param}]->b return r", "param" -> map1)
    val r2 = executeScalar[Relationship]("start a=node(1), b=node(2) create unique a-[r:FOO {param}]->b return r", "param" -> map2)

    assert(r1 === r2)
  }
  @Test
  def create_unique_relationship_and_use_created_identifier_in_set() {
    createNode()
    createNode()

    val r1 = executeScalar[Relationship]("start a=node(1), b=node(2) create unique a-[r:FOO]->b set r.foo = 'bar' return r")

    assert("bar" === r1.getProperty("foo"))
  }

  @Test
  def create_unique_twice_with_array_prop() {
    createNode()
    createNode()

    parseAndExecute("start a=node(1) create unique a-[:X]->({foo:[1,2,3]})")
    val result = parseAndExecute("start a=node(1) create unique a-[:X]->({foo:[1,2,3]})")

    assertFalse("Should not have created node", result.queryStatistics().containsUpdates)
  }

  @Test
  def full_path_in_one_create() {
    createNode()
    createNode()
    val result = parseAndExecute("start a=node(1), b=node(2) create a-[:KNOWS]->()<-[:LOVES]->b")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 2)
  }

  @Test
  def delete_and_delete_again() {
    createNode()
    val result = parseAndExecute("start a=node(1) delete a foreach( x in [1] : delete a)")

    assertStats(result, deletedNodes = 1)
  }

  @Test
  def created_paths_honor_directions() {
    val a = createNode()
    val b = createNode()
    val result = parseAndExecute("start a=node(1), b=node(2) create p = a<-[:X]-b return p").toList.head("p").asInstanceOf[Path]

    assert(result.startNode() === a)
    assert(result.endNode() === b)
  }

  @Test
  def create_unique_paths_honor_directions() {
    val a = createNode()
    val b = createNode()
    val result = parseAndExecute("start a=node(1), b=node(2) create unique p = a<-[:X]-b return p").toList.head("p").asInstanceOf[Path]

    assert(result.startNode() === a)
    assert(result.endNode() === b)
  }

  @Test
  def create_with_parameters_is_not_ok_when_identifier_already_exists() {
    intercept[SyntaxException](parseAndExecute("create a with a create (a {name:\"Foo\"})-[:BAR]->()").toList)
  }

  @Test
  def failure_only_fails_inner_transaction() {
    val tx = graph.beginTx()
    try {
      parseAndExecute("start a=node({id}) set a.foo = 'bar' return a","id"->"0")
    } catch {
      case _: Throwable => tx.failure()
    }
    finally tx.finish()
  }

  @Test
  def create_two_rels_in_one_command_should_work() {
    val result = parseAndExecute("create (a{name:'a'})-[:test]->b, a-[:test2]->c")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2, propertiesSet = 1)
  }

  @Test
  def cant_set_properties_after_node_is_already_created() {
    intercept[SyntaxException](parseAndExecute("create a-[:test]->b, (a {name:'a'})-[:test2]->c"))
  }

  @Test
  def cant_set_properties_after_node_is_already_created2() {
    intercept[SyntaxException](parseAndExecute("create a-[:test]->b create unique (a {name:'a'})-[:test2]->c"))
  }

  @Test
  def can_create_anonymous_nodes_inside_foreach() {
    val result = parseAndExecute("start me=node(0) foreach (i in range(1,10) : create me-[:FRIEND]->())")

    assert(result.toList === List())
  }

  @Test
  def should_be_able_to_use_external_identifiers_inside_foreach() {
    val result = parseAndExecute("start a=node(0), b=node(0) foreach(x in [b] : create x-[:FOO]->a) ")

    assert(result.toList === List())
  }

  @Test
  def should_be_able_to_create_node_with_labels() {
    val result = parseAndExecute("create n:FOO:BAR return n")
    val createdNode = result.columnAs[Node]("n").next()

    assert(createdNode.getLabels.asScala.map(_.name()) === List("FOO", "BAR"))
    assertStats(result, nodesCreated = 1, addedLabels = 2);
  }
  
  @Test
  def should_be_able_to_add_label_to_node() {
    val result = parseAndExecute("start n=node(0) set n:FOO return n")
    val createdNode = result.columnAs[Node]("n").next()

    assert(createdNode.getLabels.asScala.map(_.name()) === List("FOO"))
    assertStats(result, addedLabels = 1);
  }
}

trait StatisticsChecker extends Assertions {
  def assertStats(result: ExecutionResult,
                  nodesCreated: Int = 0,
                  relationshipsCreated: Int = 0,
                  propertiesSet: Int = 0,
                  deletedNodes: Int = 0,
                  deletedRelationships: Int = 0,
                  addedLabels: Int = 0,
                  removedLabels: Int = 0) {
    val statistics = result.queryStatistics()
    assert(statistics === QueryStatistics(nodesCreated, relationshipsCreated, propertiesSet, deletedNodes,
                                deletedRelationships, addedLabels, removedLabels)
    )
  }
}