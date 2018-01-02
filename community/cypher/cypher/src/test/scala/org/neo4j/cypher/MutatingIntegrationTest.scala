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
package org.neo4j.cypher

import java.util.HashMap

import org.neo4j.graphdb._
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.tooling.GlobalGraphOperations
import org.scalatest.Assertions

import scala.collection.JavaConverters._

class MutatingIntegrationTest extends ExecutionEngineFunSuite with Assertions with QueryStatisticsTestSupport {

  test("create_a_single_node") {
    val before = graph.inTx(GlobalGraphOperations.at(graph).getAllNodes.asScala.size)

    val result = execute("create a")

    assertStats(result, nodesCreated = 1)
    graph.inTx {
      GlobalGraphOperations.at(graph).getAllNodes.asScala should have size before + 1
    }
  }


  test("create_a_single_node_with_props_and_return_it") {
    val before = graph.inTx(GlobalGraphOperations.at(graph).getAllNodes.asScala.size)

    val result = execute("create (a {name : 'Andres'}) return a")

    assertStats(result, nodesCreated = 1, propertiesSet = 1)
    graph.inTx {
      GlobalGraphOperations.at(graph).getAllNodes.asScala should have size before + 1
    }

    val list = result.toList
    list should have size 1
    val createdNode = list.head("a").asInstanceOf[Node]
    graph.inTx{
      createdNode.getProperty("name") should equal("Andres")
    }
  }

  test("start_with_a_node_and_create_a_new_node_with_the_same_properties") {
    createNode("age" -> 15)

    val result = execute("match (a) where id(a) = 0 with a create (b {age : a.age * 2}) return b")

    assertStats(result, nodesCreated = 1, propertiesSet = 1)

    val list = result.toList
    list should have size 1
    val createdNode = list.head("b").asInstanceOf[Node]
    graph.inTx{
      createdNode.getProperty("age") should equal(30l)
    }
  }

  test("create_two_nodes_and_a_relationship_between_them") {
    val result = execute("create a, b, a-[r:REL]->b")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("create_one_node_and_dumpToString") {
    val result = execute("create (a {name:'Cypher'})")

    assertStats(result,
      nodesCreated = 1,
      propertiesSet = 1
    )
  }

  test("deletes_single_node") {
    val a = createNode().getId

    val result = execute("match (a) where id(a) = 0 delete a")
    assertStats(result, nodesDeleted = 1)

    result.toList shouldBe empty
    intercept[NotFoundException](graph.inTx(graph.getNodeById(a)))
  }

  test("multiple_deletes_should_not_break_anything") {
    (1 to 4).foreach(i => createNode())

    val result = execute("match (a), (b) where id(a) = 0 AND id(b) IN [1, 2, 3] delete a")
    assertStats(result, nodesDeleted = 1)

    result.toList shouldBe empty
  }

  test("deletes_all_relationships") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()

    relate(a, b)
    relate(a, c)
    relate(a, d)

    val result = execute("match (a) where id(a) = 0 match a-[r]->() delete r")
    assertStats( result, relationshipsDeleted = 3  )

    graph.inTx {
      a.getRelationships.asScala shouldBe empty
    }
  }

  test("create_multiple_relationships_in_one_query") {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    val result = execute("create n with n MATCH x WHERE id(x) IN [0, 1, 2] create n-[:REL]->x")
    assertStats(result,
      nodesCreated = 1,
      relationshipsCreated = 3
    )

    graph.inTx {
      a.getRelationships.asScala should have size 1
      b.getRelationships.asScala should have size 1
      c.getRelationships.asScala should have size 1
    }
  }

  test("set_a_property_to_a_collection") {
    createNode("Andres")
    createNode("Michael")
    createNode("Peter")

    val result = execute("MATCH n WHERE id(n) IN [0, 1, 2] with collect(n.name) as names create ({name : names})")
    assertStats(result,
      propertiesSet = 1,
      nodesCreated = 1
    )

    graph.inTx {
      graph.getNodeById(3).getProperty("name") should equal(Array("Andres", "Michael", "Peter"))
    }
  }

  test("set_a_property_to_an_empty_collection") {
    createNode("Andres")

    val result = execute("match (n) where id(n) = 0 with filter(x in collect(n.name) WHERE x = 12) as names create ({x : names})")
    assertStats(result,
      propertiesSet = 1,
      nodesCreated = 1
    )

    graph.inTx {
      graph.getNodeById(1).getProperty("x") should equal(Array())
    }
  }

  test("create_node_from_map_values") {
    val result = execute("create (n {a}) return n.age, n.name", "a" -> Map("name" -> "Andres", "age" -> 66))

    result.toList should equal(List(Map("n.age" -> 66, "n.name" -> "Andres")))
  }


  test("create_rel_from_map_values") {
    createNode()
    createNode()

    val r = execute("match (a), (b) where id(a) = 0 AND id(b) = 1 create a-[r:REL {param}]->b return r", "param" -> Map("name" -> "Andres", "age" -> 66)).
      toList.head("r").asInstanceOf[Relationship]
    graph.inTx {
      r.getProperty("name") should equal("Andres")
      r.getProperty("age") should equal(66)
    }
  }

  test("match_and_delete") {
    val a = createNode()
    val b = createNode()

    relate(a, b, "HATES")
    relate(a, b, "LOVES")

    intercept[NodeStillHasRelationshipsException](execute("match (n) where id(n) = 0 match n-[r:HATES]->() delete n,r"))
  }

  test("delete_and_return") {
    val a = createNode()

    val result = execute("match (n) where id(n) = 0 delete n return n").toList

    result should equal(List(Map("n" -> a)))
  }

  test("create_multiple_nodes") {
    val maps = List(
      Map("name" -> "Andres", "prefers" -> "Scala"),
      Map("name" -> "Michael", "prefers" -> "Java"),
      Map("name" -> "Peter", "prefers" -> "Java"))

    val result = execute("create ({params})", "params" -> maps)

    assertStats(result,
      nodesCreated = 3,
      propertiesSet = 6
    )
  }

  test("create_multiple_nodes_and_return") {
    val maps = List(
      Map("name" -> "Andres"),
      Map("name" -> "Michael"),
      Map("name" -> "Peter"))

    val result = execute("create (n {params}) return n", "params" -> maps).toList

    result should have size 3
  }

  test("fail_to_create_from_two_iterables") {
    val maps1 = List(
      Map("name" -> "Andres"),
      Map("name" -> "Michael"),
      Map("name" -> "Peter"))
    val maps2 = List(
      Map("name" -> "Andres"),
      Map("name" -> "Michael"),
      Map("name" -> "Peter"))

    intercept[ParameterWrongTypeException](execute("create (a {params1}), (b {params2})", "params1" -> maps1, "params2" -> maps2))
  }

  test("first_read_then_write") {
    val root = createNode()
    val a = createNode("Alfa")
    val b = createNode("Beta")
    val c = createNode("Gamma")

    relate(root, a)
    relate(root, b)
    relate(root, c)

    execute("match (root) where id(root) = 0 match root-->other create (new {name:other.name}), root-[:REL]->new")

    val result = execute("match (root) where id(root) = 0 match root-->other return other.name order by other.name").columnAs[String]("other.name").toList
    result should equal(List("Alfa", "Alfa", "Beta", "Beta", "Gamma", "Gamma"))
  }

  test("create_node_and_rel_in_foreach") {
    execute("""
create center
foreach(x in range(1,10) |
  create (leaf1 {number : x}) , center-[:X]->leaf1
)
return distinct center""")
  }

  test("delete_optionals") {
    createNode()
    val a = createNode()
    val b = createNode()
    relate(a,b)

    execute("""start n=node(*) optional match n-[r]-() delete n,r""")

    graph.inTx {
      GlobalGraphOperations.at(graph).getAllNodes.asScala shouldBe empty
    }
  }

  test("delete_path") {
    val a = createNode()
    val b = createNode()
    relate(a,b)

    execute("""match (n) where id(n) = 0 match p=n-->() delete p""")

    graph.inTx {
      GlobalGraphOperations.at(graph).getAllNodes.asScala shouldBe empty
    }
  }

  test("string_literals_should_not_be_mistaken_for_identifiers") {
    //https://github.com/neo4j/community/issues/523
    val result = executeScalar[List[Node]]("create (tag1 {name:'tag2'}), (tag2 {name:'tag1'}) return [tag1,tag2] as tags")
    result should have size 2
  }

  test("create_node_from_map_with_array_value_from_java") {
    val list = new java.util.ArrayList[String]()
    list.add("foo")
    list.add("bar")

    val map = new java.util.HashMap[String, Object]()
    map.put("arrayProp", list)

    val q = "create (a{param}) return a.arrayProp"
    val result =  executeScalar[Array[String]](q, "param" -> map)

    assertStats(execute(q, "param"->map), nodesCreated = 1, propertiesSet = 1)
    result.toList should equal(List("foo","bar"))
  }

  test("failed_query_should_not_leave_dangling_transactions") {
    intercept[RuntimeException](execute("RETURN 1 / 0"))

    val contextBridge : ThreadToStatementContextBridge = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
    contextBridge.getTopLevelTransactionBoundToThisThread( false ) should be(null)
  }

  test("create_unique_twice_with_param_map") {
    createNode()
    createNode()

    val map1 = Map("name" -> "Anders")
    val map2 = new HashMap[String, Any]()
    map2.put("name", "Anders")

    val r1 = executeScalar[Relationship]("match (a), (b) where id(a) = 0 AND id(b) = 1 create unique a-[r:FOO {param}]->b return r", "param" -> map1)
    val r2 = executeScalar[Relationship]("match (a), (b) where id(a) = 0 AND id(b) = 1 create unique a-[r:FOO {param}]->b return r", "param" -> map2)

    r1 should equal(r2)
  }
  test("create_unique_relationship_and_use_created_identifier_in_set") {
    createNode()
    createNode()

    val r1 = executeScalar[Relationship]("match (a), (b) where id(a) = 0 AND id(b) = 1 create unique a-[r:FOO]->b set r.foo = 'bar' return r")

    graph.inTx {
      r1.getProperty("foo") should equal("bar")
    }
  }

  test("create_unique_twice_with_array_prop") {
    createNode()
    createNode()

    execute("match (a) where id(a) = 0 create unique a-[:X]->({foo:[1,2,3]})")
    val result = execute("match (a) where id(a) = 0 create unique a-[:X]->({foo:[1,2,3]})")

    result.queryStatistics().containsUpdates should be(false)
  }

  test("full_path_in_one_create") {
    createNode()
    createNode()
    val result = execute("match (a), (b) where id(a) = 0 AND id(b) = 1 create a-[:KNOWS]->()-[:LOVES]->b")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 2)
  }

  test("delete_and_delete_again") {
    createNode()
    val result = execute("match (a) where id(a) = 0 delete a foreach( x in [1] | delete a)")

    assertStats(result, nodesDeleted = 1)
  }

  test("created_paths_honor_directions") {
    val a = createNode()
    val b = createNode()
    val result = execute("match (a), (b) where id(a) = 0 AND id(b) = 1 create p = a<-[:X]-b return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal(a)
    result.endNode() should equal(b)
  }

  test("create_unique_paths_honor_directions") {
    val a = createNode()
    val b = createNode()
    val result = execute("match (a), (b) where id(a) = 0 AND id(b) = 1 create unique p = a<-[:X]-b return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal(a)
    result.endNode() should equal(b)
  }

  test("create_with_parameters_is_not_ok_when_identifier_already_exists") {
    intercept[SyntaxException](execute("create a with a create (a {name:\"Foo\"})-[:BAR]->()").toList)
  }

  test("failure_only_fails_inner_transaction") {
    val tx = graph.beginTx()
    try {
      execute("match a where id(a) = {id} set a.foo = 'bar' return a","id"->"0")
    } catch {
      case _: Throwable => tx.failure()
    }
    finally tx.close()
  }

  test("create_two_rels_in_one_command_should_work") {
    val result = execute("create (a{name:'a'})-[:test]->b, a-[:test2]->c")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2, propertiesSet = 1)
  }

  test("cant_set_properties_after_node_is_already_created") {
    intercept[SyntaxException](execute("create a-[:test]->b, (a {name:'a'})-[:test2]->c"))
  }

  test("cant_set_properties_after_node_is_already_created2") {
    intercept[SyntaxException](execute("create a-[:test]->b create unique (a {name:'a'})-[:test2]->c"))
  }

  test("can_create_anonymous_nodes_inside_foreach") {
    createNode()
    val result = execute("match (me) where id(me) = 0 foreach (i in range(1,10) | create me-[:FRIEND]->())")

    result.toList shouldBe empty
  }

  test("should_be_able_to_use_external_identifiers_inside_foreach") {
    createNode()
    val result = execute("match a, b where id(a) = 0 AND id(b) = 0 foreach(x in [b] | create x-[:FOO]->a) ")

    result.toList shouldBe empty
  }

  test("should_be_able_to_create_node_with_labels") {
    val result = execute("create (n:FOO:BAR) return n")
    val createdNode = result.columnAs[Node]("n").next()

    assertStats(result, nodesCreated = 1, labelsAdded = 2)

    graph.inTx {
      val labels = createdNode.getLabels().asScala.map(_.name).toSet
      labels should equal(Set("FOO", "BAR"))
    }
  }

  test("complete_graph") {
    val result =
      execute("""CREATE (center { count:0 })
                 FOREACH (x IN range(1,6) | CREATE (leaf { count : x }),(center)-[:X]->(leaf))
                 WITH center
                 MATCH (leaf1)<--(center)-->(leaf2)
                 WHERE id(leaf1)<id(leaf2)
                 CREATE (leaf1)-[:X]->(leaf2)
                 WITH center
                 MATCH (center)-[r]->()
                 DELETE center,r""")

    assertStats(result, nodesCreated = 7, propertiesSet = 7, relationshipsCreated = 21, nodesDeleted = 1, relationshipsDeleted = 6)
  }

  test("for_each_applied_to_null_should_never_execute") {
    val result = execute("foreach(x in null| create ())")

    assertStats(result, nodesCreated = 0)
  }

  test("should_execute_when_null_is_contained_in_a_collection") {
    val result = execute("foreach(x in [null]| create ())")

    assertStats(result, nodesCreated = 1)
  }

  test("should be possible to remove nodes created in the same query") {
    val result = execute(
      """CREATE (a)-[:FOO]->(b)
         WITH *
         MATCH (x)-[r]-(y)
         DELETE x, r, y""".stripMargin)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, nodesDeleted = 2, relationshipsDeleted = 1)
  }
}
