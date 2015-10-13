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

import java.util

import org.neo4j.graphdb._
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.tooling.GlobalGraphOperations
import org.scalatest.Assertions

import scala.collection.JavaConverters._

class MutatingIntegrationTest extends ExecutionEngineFunSuite with Assertions with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("create a single node") {
    val before = graph.inTx(GlobalGraphOperations.at(graph).getAllNodes.asScala.size)

    val result = updateWithBothPlanners("create (a)")

    assertStats(result, nodesCreated = 1)
    graph.inTx {
      GlobalGraphOperations.at(graph).getAllNodes.asScala should have size before + 1
    }
  }


  test("create a single node with props and return it") {
    val before = graph.inTx(GlobalGraphOperations.at(graph).getAllNodes.asScala.size)

    val result = updateWithBothPlanners("create (a {name : 'Andres'}) return a.name")

    assertStats(result, nodesCreated = 1, propertiesSet = 1)
    graph.inTx {
      GlobalGraphOperations.at(graph).getAllNodes.asScala should have size before + 1
    }

    result.toList should equal(List(Map("a.name" -> "Andres")))
  }

  test("start with a node and create a new node with the same properties") {
    createNode("age" -> 15)

    val result = updateWithBothPlanners("match (a) where id(a) = 0 with a create (b {age : a.age * 2}) return b.age")

    assertStats(result, nodesCreated = 1, propertiesSet = 1)

    result.toList should equal(List(Map("b.age" -> 30)))
  }

  test("create two nodes and a relationship between them") {
    val result = executeWithRulePlanner("create (a), (b), (a)-[r:REL]->(b)")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("create one node and dumpToString") {
    val result = updateWithBothPlanners("create (a {name:'Cypher'})")

    assertStats(result,
      nodesCreated = 1,
      propertiesSet = 1
    )
  }

  test("deletes single node") {
    val a = createNode().getId

    val result = executeWithRulePlanner("match (a) where id(a) = 0 delete a")
    assertStats(result, nodesDeleted = 1)

    result.toList shouldBe empty
    intercept[NotFoundException](graph.inTx(graph.getNodeById(a)))
  }

  test("multiple deletes should not break anything") {
    (1 to 4).foreach(i => createNode())

    val result = executeWithRulePlanner("match (a), (b) where id(a) = 0 AND id(b) IN [1, 2, 3] delete a")
    assertStats(result, nodesDeleted = 1)

    result.toList shouldBe empty
  }

  test("deletes all relationships") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()

    relate(a, b)
    relate(a, c)
    relate(a, d)

    val result = executeWithRulePlanner("match (a) where id(a) = 0 match (a)-[r]->() delete r")
    assertStats( result, relationshipsDeleted = 3  )

    graph.inTx {
      a.getRelationships.asScala shouldBe empty
    }
  }

  test("create multiple relationships in one query") {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    val result = executeWithRulePlanner("create (n) with n MATCH (x) WHERE id(x) IN [0, 1, 2] create (n)-[:REL]->(x)")
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

  test("set a property to a collection") {
    createNode("Andres")
    createNode("Michael")
    createNode("Peter")

    val result = updateWithBothPlanners("MATCH (n) with collect(n.name) as names create (m {name : names}) RETURN m.name")
    assertStats(result,
      propertiesSet = 1,
      nodesCreated = 1
    )

      result.toComparableResult should equal(List(Map("m.name" -> List("Andres", "Michael", "Peter"))))
  }

  test("set a property to an empty collection") {
    val result = updateWithBothPlanners("create (n {x : []}) return n.x")
    assertStats(result,
      propertiesSet = 1,
      nodesCreated = 1
    )
    result.toComparableResult should equal (List(Map("n.x" -> List.empty)))
  }

  test("create node from map values") {
    val result = updateWithBothPlanners("create (n {a}) return n.age, n.name", "a" -> Map("name" -> "Andres", "age" -> 66))

    result.toList should equal(List(Map("n.age" -> 66, "n.name" -> "Andres")))
  }


  test("create rel from map values") {
    createNode()
    createNode()

    val r = executeWithRulePlanner("match (a), (b) where id(a) = 0 AND id(b) = 1 create (a)-[r:REL {param}]->(b) return r", "param" -> Map("name" -> "Andres", "age" -> 66)).
      toList.head("r").asInstanceOf[Relationship]
    graph.inTx {
      r.getProperty("name") should equal("Andres")
      r.getProperty("age") should equal(66)
    }
  }

  test("match and delete") {
    val a = createNode()
    val b = createNode()

    relate(a, b, "HATES")
    relate(a, b, "LOVES")

    intercept[NodeStillHasRelationshipsException](executeWithRulePlanner("match (n) where id(n) = 0 match (n)-[r:HATES]->() delete n,r"))
  }

  test("delete and return") {
    val a = createNode()

    val result = executeWithRulePlanner("match (n) where id(n) = 0 delete n return n").toList

    result should equal(List(Map("n" -> a)))
  }

  test("create multiple nodes") {
    val maps = List(
      Map("name" -> "Andres", "prefers" -> "Scala"),
      Map("name" -> "Michael", "prefers" -> "Java"),
      Map("name" -> "Peter", "prefers" -> "Java"))

    val result = executeWithRulePlanner("unwind {params} as m create (x) set x = m ", "params" -> maps)

    assertStats(result,
      nodesCreated = 3,
      propertiesSet = 6
    )
  }

  test("not allowed to create multiple nodes with parameter list") {
    val maps = List(
      Map("name" -> "Andres", "prefers" -> "Scala"),
      Map("name" -> "Michael", "prefers" -> "Java"),
      Map("name" -> "Peter", "prefers" -> "Java"))

    intercept[CypherTypeException](executeWithCostPlannerOnly("create ({params})", "params" -> maps))
  }

  test("not allowed to create multiple nodes with parameter list in rule planner") {
    val maps = List(
      Map("name" -> "Andres", "prefers" -> "Scala"),
      Map("name" -> "Michael", "prefers" -> "Java"),
      Map("name" -> "Peter", "prefers" -> "Java"))

    intercept[CypherTypeException](eengine.execute("cypher planner=rule create ({params})", Map("params" -> maps)))
  }

  test("fail to create from two iterables") {
    val maps1 = List(
      Map("name" -> "Andres"),
      Map("name" -> "Michael"),
      Map("name" -> "Peter"))
    val maps2 = List(
      Map("name" -> "Andres"),
      Map("name" -> "Michael"),
      Map("name" -> "Peter"))

    intercept[CypherTypeException](updateWithBothPlanners("create (a {params1}), (b {params2})", "params1" -> maps1, "params2" -> maps2))
  }

  test("first read then write") {
    val root = createNode()
    val a = createNode("Alfa")
    val b = createNode("Beta")
    val c = createNode("Gamma")

    relate(root, a)
    relate(root, b)
    relate(root, c)

    executeWithRulePlanner("match (root) where id(root) = 0 match (root)-->(other) create (new {name:other.name}), (root)-[:REL]->(new)")

    val result = executeWithAllPlanners("match (root) where id(root) = 0 match (root)-->(other) return other.name order by other.name").columnAs[String]("other.name").toList
    result should equal(List("Alfa", "Alfa", "Beta", "Beta", "Gamma", "Gamma"))
  }

  test("create node and rel in foreach") {
    executeWithRulePlanner("""
create (center)
foreach(x in range(1,10) |
  create (leaf1 {number : x}) , (center)-[:X]->(leaf1)
)
return distinct center""")
  }

  test("delete optionals") {
    createNode()
    val a = createNode()
    val b = createNode()
    relate(a,b)

    executeWithRulePlanner("""start n=node(*) optional match (n)-[r]-() delete n,r""")

    graph.inTx {
      GlobalGraphOperations.at(graph).getAllNodes.asScala shouldBe empty
    }
  }

  test("delete path") {
    val a = createNode()
    val b = createNode()
    relate(a,b)

    executeWithRulePlanner("""match (n) where id(n) = 0 match p=(n)-->() delete p""")

    graph.inTx {
      GlobalGraphOperations.at(graph).getAllNodes.asScala shouldBe empty
    }
  }

  test("string literals should not be mistaken for identifiers") {
    //https://github.com/neo4j/community/issues/523
    updateWithBothPlanners("EXPLAIN create (tag1 {name:'tag2'}), (tag2 {name:'tag1'}) return [tag1,tag2] as tags")
    val result = executeScalar[List[Node]]("create (tag1 {name:'tag2'}), (tag2 {name:'tag1'}) return [tag1,tag2] as tags")
    result should have size 2
  }

  test("create node from map with array value from java") {
    val list = new java.util.ArrayList[String]()
    list.add("foo")
    list.add("bar")

    val map = new java.util.HashMap[String, Object]()
    map.put("arrayProp", list)

    val q = "create (a{param}) return a.arrayProp"
    val result =  executeScalar[Array[String]](q, "param" -> map)

    assertStats(updateWithBothPlanners(q, "param"->map), nodesCreated = 1, propertiesSet = 1)
    result.toList should equal(List("foo","bar"))
  }

  test("failed query should not leave dangling transactions") {
    intercept[RuntimeException](executeWithAllPlannersAndRuntimes("RETURN 1 / 0"))

    val contextBridge : ThreadToStatementContextBridge = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
    contextBridge.getTopLevelTransactionBoundToThisThread( false ) should be(null)
  }

  test("create unique twice with param map") {
    createNode()
    createNode()

    val map1 = Map("name" -> "Anders")
    val map2 = new util.HashMap[String, Any]()
    map2.put("name", "Anders")

    val r1 = executeScalar[Relationship]("match (a), (b) where id(a) = 0 AND id(b) = 1 create unique (a)-[r:FOO {param}]->(b) return r", "param" -> map1)
    val r2 = executeScalar[Relationship]("match (a), (b) where id(a) = 0 AND id(b) = 1 create unique (a)-[r:FOO {param}]->(b) return r", "param" -> map2)

    r1 should equal(r2)
  }
  test("create unique relationship and use created identifier in set") {
    createNode()
    createNode()

    val r1 = executeScalar[Relationship]("match (a), (b) where id(a) = 0 AND id(b) = 1 create unique (a)-[r:FOO]->(b) set r.foo = 'bar' return r")

    graph.inTx {
      r1.getProperty("foo") should equal("bar")
    }
  }

  test("create unique twice with array prop") {
    createNode()
    createNode()

    eengine.execute("match (a) where id(a) = 0 create unique (a)-[:X]->({foo:[1,2,3]})")
    val result = eengine.execute("match (a) where id(a) = 0 create unique (a)-[:X]->({foo:[1,2,3]})")

    result.queryStatistics().containsUpdates should be(false)
  }

  test("full path in one create") {
    createNode()
    createNode()
    val result = executeWithRulePlanner("match (a), (b) where id(a) = 0 AND id(b) = 1 create (a)-[:KNOWS]->()-[:LOVES]->(b)")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 2)
  }

  test("delete and delete again") {
    createNode()
    val result = executeWithRulePlanner("match (a) where id(a) = 0 delete a foreach( x in [1] | delete a)")

    assertStats(result, nodesDeleted = 1)
  }

  test("created paths honor directions") {
    val a = createNode()
    val b = createNode()
    val result = executeWithRulePlanner("match (a), (b) where id(a) = 0 AND id(b) = 1 create p = (a)<-[:X]-(b) return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal(a)
    result.endNode() should equal(b)
  }

  test("create unique paths honor directions") {
    val a = createNode()
    val b = createNode()
    val result = executeWithRulePlanner("match (a), (b) where id(a) = 0 AND id(b) = 1 create unique p = (a)<-[:X]-(b) return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal(a)
    result.endNode() should equal(b)
  }

  test("create with parameters is not ok when identifier already exists") {
    intercept[SyntaxException](updateWithBothPlanners("create a with a create (a {name:\"Foo\"})-[:BAR]->()").toList)
  }

  test("failure_only_fails_inner_transaction") {
    val tx = graph.beginTx()
    try {
      executeWithRulePlanner("match (a) where id(a) = {id} set a.foo = 'bar' return a","id"->"0")
    } catch {
      case _: Throwable => tx.failure()
    }
    finally tx.close()
  }

  test("create two rels in one command should work") {
    val result = executeWithRulePlanner("create (a{name:'a'})-[:test]->(b), (a)-[:test2]->(c)")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2, propertiesSet = 1)
  }

  test("cant set properties after node is already created") {
    intercept[SyntaxException](executeWithRulePlanner("create a-[:test]->b, (a {name:'a'})-[:test2]->c"))
  }

  test("cant set properties after node is already created2") {
    intercept[SyntaxException](executeWithRulePlanner("create a-[:test]->b create unique (a {name:'a'})-[:test2]->c"))
  }

  test("can create anonymous nodes inside foreach") {
    createNode()
    val result = executeWithRulePlanner("match (me) where id(me) = 0 foreach (i in range(1,10) | create (me)-[:FRIEND]->())")

    result.toList shouldBe empty
  }

  test("should be able to use external identifiers inside foreach") {
    createNode()
    val result = executeWithRulePlanner("match (a), (b) where id(a) = 0 AND id(b) = 0 foreach(x in [b] | create (x)-[:FOO]->(a)) ")

    result.toList shouldBe empty
  }

  test("should be able to create node with labels") {
    val result = updateWithBothPlanners("create (n:FOO:BAR) return labels(n) as l")

    assertStats(result, nodesCreated = 1, labelsAdded = 2)
    result.toList should equal(List(Map("l" -> List("FOO", "BAR"))))
  }

  test("complete graph") {
    val result =
      executeWithRulePlanner("""CREATE (center { count:0 })
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

  test("for each applied to null should never execute") {
    val result = executeWithRulePlanner("foreach(x in null| create ())")

    assertStats(result, nodesCreated = 0)
  }

  test("should execute when null is contained in a collection") {
    val result = executeWithRulePlanner("foreach(x in [null]| create ())")

    assertStats(result, nodesCreated = 1)
  }

  test("should be possible to remove nodes created in the same query") {
    val result = executeWithRulePlanner(
      """CREATE (a)-[:FOO]->(b)
         WITH *
         MATCH (x)-[r]-(y)
         DELETE x, r, y""".stripMargin)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, nodesDeleted = 2, relationshipsDeleted = 1)
  }
}
