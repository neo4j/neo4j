/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.graphdb._
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.internal.kernel.api.Transaction.Type
import org.neo4j.kernel.api.security.AnonymousContext
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge

import scala.collection.JavaConverters._

class MutatingIntegrationTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  test("create a single node") {
    val before = graph.inTx(graph.getAllNodes.asScala.size)

    val result = executeWith(Configs.UpdateConf, "create (a)")

    assertStats(result, nodesCreated = 1)
    graph.inTx {
      graph.getAllNodes.asScala should have size before + 1
    }
  }

  test("create a single node with props and return it") {
    val before = graph.inTx(graph.getAllNodes.asScala.size)

    val result = executeWith(Configs.UpdateConf, "create (a {name : 'Andres'}) return a.name")

    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
    graph.inTx {
      graph.getAllNodes.asScala should have size before + 1
    }

    result.toList should equal(List(Map("a.name" -> "Andres")))
  }

  test("start with a node and create a new node with the same properties") {
    createNode("age" -> 15)

    val result = executeWith(Configs.UpdateConf, "match (a) where id(a) = 0 with a create (b {age : a.age * 2}) return b.age")

    assertStats(result, nodesCreated = 1, propertiesWritten = 1)

    result.toList should equal(List(Map("b.age" -> 30)))
  }

  test("create two nodes and a relationship between them") {
    val result = executeWith(Configs.UpdateConf - Configs.Cost2_3, "create (a), (b), (a)-[r:REL]->(b)")

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  test("create one node and dumpToString") {
    val result = executeWith(Configs.UpdateConf, "create (a {name:'Cypher'})")

    assertStats(result,
      nodesCreated = 1,
      propertiesWritten = 1
    )
  }

  test("deletes single node") {
    val a = createNode().getId

    val result = executeWith(Configs.UpdateConf, "match (a) where id(a) = 0 delete a")
    assertStats(result, nodesDeleted = 1)

    result.toList shouldBe empty
    intercept[NotFoundException](graph.inTx(graphOps.getNodeById(a)))
  }

  test("multiple deletes should not break anything") {
    (1 to 4).foreach(i => createNode())

    val result = executeWith(Configs.UpdateConf, "match (a), (b) where id(a) = 0 AND id(b) IN [1, 2, 3] delete a")
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

    val result = executeWith(Configs.UpdateConf, "match (a) where id(a) = 0 match (a)-[r]->() delete r")
    assertStats( result, relationshipsDeleted = 3  )

    graph.inTx {
      a.getRelationships.asScala shouldBe empty
    }
  }

  test("create multiple relationships in one query") {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    val result = executeWith(Configs.UpdateConf, "create (n) with n MATCH (x) WHERE id(x) IN [0, 1, 2] create (n)-[:REL]->(x)")
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

    val result = executeWith(Configs.UpdateConf, "MATCH (n) with collect(n.name) as names create (m {name : names}) RETURN m.name")
    assertStats(result,
      propertiesWritten = 1,
      nodesCreated = 1
    )

      result.toComparableResult should equal(List(Map("m.name" -> List("Andres", "Michael", "Peter"))))
  }

  test("set a property to an empty collection") {
    val result = executeWith(Configs.UpdateConf, "create (n {x : []}) return n.x")
    assertStats(result,
      propertiesWritten = 1,
      nodesCreated = 1
    )
    result.toComparableResult should equal (List(Map("n.x" -> List.empty)))
  }

  test("create node from map values") {
    val result = executeWith(Configs.UpdateConf, "create (n {a}) return n.age, n.name", params = Map("a" -> Map("name" -> "Andres", "age" -> 66)))

    result.toList should equal(List(Map("n.age" -> 66, "n.name" -> "Andres")))
  }

  test("create rel from map values") {
    createNode()
    createNode()


    val result = executeWith(Configs.UpdateConf, "match (a), (b) where id(a) = 0 AND id(b) = 1 create (a)-[r:REL {param}]->(b) return r.name, r.age", params = Map("param" -> Map("name" -> "Andres", "age" -> 66)))

    result.toList should equal(List(Map("r.name" -> "Andres", "r.age" -> 66)))
  }

  test("match and delete") {
    val a = createNode()
    val b = createNode()

    relate(a, b, "HATES")
    relate(a, b, "LOVES")

    val msg = "Cannot delete node<0>, because it still has relationships. To delete this node, you must first delete its relationships."
    failWithError(Configs.UpdateConf + Configs.Procs, "match (n) where id(n) = 0 match (n)-[r:HATES]->() delete n,r", List(msg))
  }

  test("delete and return") {
    val a = createNode()

    val result = executeWith(Configs.UpdateConf, "match (n) where id(n) = 0 delete n return n")

    result.toList should equal(List(Map("n" -> a)))
  }

  test("create multiple nodes") {
    val maps = List(
      Map("name" -> "Andres", "prefers" -> "Scala"),
      Map("name" -> "Michael", "prefers" -> "Java"),
      Map("name" -> "Peter", "prefers" -> "Java"))

    val result = executeWith(Configs.UpdateConf, "unwind {params} as m create (x) set x = m ", params = Map("params" -> maps))

    assertStats(result,
      nodesCreated = 3,
      propertiesWritten = 6
    )
  }

  test("not allowed to create multiple nodes with parameter list") {
    val maps = List(
      Map("name" -> "Andres", "prefers" -> "Scala"),
      Map("name" -> "Michael", "prefers" -> "Java"),
      Map("name" -> "Peter", "prefers" -> "Java"))

    val errorMessages = List("If you want to create multiple nodes, please use UNWIND.", "Parameter provided for node creation is not a Map")
    failWithError(Configs.UpdateConf - Configs.Rule2_3 + Configs.Procs, "create ({params})", params = Map("params" -> maps), message = errorMessages)
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
    val query = "create (a {params1}), (b {params2})"
    val errorMessages = List("If you want to create multiple nodes, please use UNWIND.", "Parameter provided for node creation is not a Map", "If you create multiple elements, you can only create one of each.")
    failWithError(Configs.UpdateConf + Configs.Procs, query, message = errorMessages, params = Map("params1" -> maps1, "params2" -> maps2))
  }

  test("first read then write") {
    val root = createNode()
    val a = createNode("Alfa")
    val b = createNode("Beta")
    val c = createNode("Gamma")

    relate(root, a)
    relate(root, b)
    relate(root, c)

    executeWith(Configs.UpdateConf, "match (root) where id(root) = 0 match (root)-->(other) create (new {name:other.name}), (root)-[:REL]->(new)")

    val result = executeWith(Configs.All, "match (root) where id(root) = 0 match (root)-->(other) return other.name order by other.name").columnAs[String]("other.name").toList
    result should equal(List("Alfa", "Alfa", "Beta", "Beta", "Gamma", "Gamma"))
  }

  test("create node and rel in foreach") {
    executeWith(Configs.UpdateConf, """
      |create (center {name: "center"})
      |foreach(x in range(1,10) |
      |  create (leaf1 {number : x}) , (center)-[:X]->(leaf1)
      |)
      |return distinct center.name""".stripMargin)
  }

  test("delete optionals") {
    createNode()
    val a = createNode()
    val b = createNode()
    relate(a,b)

    executeWith(Configs.UpdateConf, """match (n) optional match (n)-[r]-() delete n,r""")

    graph.inTx {
      graph.getAllNodes.asScala shouldBe empty
    }
  }

  test("delete path") {
    val a = createNode()
    val b = createNode()
    relate(a,b)

    executeWith(Configs.UpdateConf, """match (n) where id(n) = 0 match p=(n)-->() delete p""")

    graph.inTx {
      graph.getAllNodes().asScala shouldBe empty
    }
  }

  test("string literals should not be mistaken for variables") {
    //https://github.com/neo4j/community/issues/523
    executeWith(Configs.UpdateConf, "EXPLAIN create (tag1 {name:'tag2'}), (tag2 {name:'tag1'}) return [tag1,tag2] as tags")
    val result = executeScalar[Seq[Node]]("create (tag1 {name:'tag2'}), (tag2 {name:'tag1'}) return [tag1,tag2] as tags")
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

    assertStats(executeWith(Configs.UpdateConf, q, params = Map("param"->map)), nodesCreated = 1, propertiesWritten = 1)
    result.toList should equal(List("foo","bar"))
  }

  test("failed query should not leave dangling transactions") {
    failWithError(Configs.AbsolutelyAll, "RETURN 1 / 0", List("/ by zero", "divide by zero"))

    val contextBridge : ThreadToStatementContextBridge = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
    contextBridge.getTopLevelTransactionBoundToThisThread( false ) should be(null)
  }

  test("full path in one create") {
    createNode()
    createNode()
    val result = executeWith(Configs.UpdateConf, "match (a), (b) where id(a) = 0 AND id(b) = 1 create (a)-[:KNOWS]->()-[:LOVES]->(b)")

    assertStats(result, nodesCreated = 1, relationshipsCreated = 2)
  }

  test("delete and delete again") {
    createNode()
    val result = executeWith(Configs.UpdateConf, "match (a) where id(a) = 0 delete a foreach( x in [1] | delete a)")

    assertStats(result, nodesDeleted = 1)
  }

  test("created paths honor directions") {
    val a = createNode(Map("prop" -> "start"))
    val b = createNode(Map("prop" -> "end"))

    val query = "match (a), (b) where a.prop = 'start' AND b.prop = 'end' create p = (a)<-[:X]-(b) with p unwind nodes(p) as x return x.prop"
    val result = executeWith(Configs.UpdateConf, query)

    result.toList should equal(List(Map("x.prop" -> "start"), Map("x.prop" -> "end")))
  }

  test("create with parameters is not ok when variable already exists") {
    val errorMessages = List("The variable is already declared in this context", "It already exists in this context")
    failWithError(Configs.AbsolutelyAll - Configs.Cost2_3, "create (a) with a create (a {name:\"Foo\"})-[:BAR]->()", errorMessages)
  }

  test("failure_only_fails_inner_transaction") {
    val tx = graph.beginTransaction( Type.explicit, AnonymousContext.write() )
    try {
      executeWith(Configs.UpdateConf, "match (a) where id(a) = {id} set a.foo = 'bar' return a", params = Map("id"->"0"))
    } catch {
      case _: Throwable => tx.failure()
    }
    finally tx.close()
  }

  test("create two rels in one command should work") {
    val result = executeWith(Configs.UpdateConf, "create (a{name:'a'})-[:test]->(b), (a)-[:test2]->(c)")

    assertStats(result, nodesCreated = 3, relationshipsCreated = 2, propertiesWritten = 1)
  }

  test("cant set properties after node is already created") {
    val errorMessages = List("The variable is already declared in this context", "It already exists in this context")
    failWithError(Configs.AbsolutelyAll - Configs.Cost2_3, "create (a)-[:test]->(b), (a {name:'a'})-[:test2]->(c)", errorMessages)
  }

  test("can create anonymous nodes inside foreach") {
    createNode()
    val result = executeWith(Configs.UpdateConf, "match (me) where id(me) = 0 foreach (i in range(1,10) | create (me)-[:FRIEND]->())")

    result.toList shouldBe empty
  }

  test("should be able to use external variables inside foreach") {
    createNode()
    val result = executeWith(Configs.UpdateConf, "match (a), (b) where id(a) = 0 AND id(b) = 0 foreach(x in [b] | create (x)-[:FOO]->(a)) ")

    result.toList shouldBe empty
  }

  test("should be able to create node with labels") {
    val result = executeWith(Configs.UpdateConf, "create (n:FOO:BAR) return labels(n) as l")

    assertStats(result, nodesCreated = 1, labelsAdded = 2)
    result.toList should equal(List(Map("l" -> List("FOO", "BAR"))))
  }

  test("complete graph") {
    val result =
      executeWith(Configs.UpdateConf, """CREATE (center { count:0 })
                 FOREACH (x IN range(1,6) | CREATE (leaf { count : x }),(center)-[:X]->(leaf))
                 WITH center
                 MATCH (leaf1)<--(center)-->(leaf2)
                 WHERE id(leaf1)<id(leaf2)
                 CREATE (leaf1)-[:X]->(leaf2)
                 WITH center
                 MATCH (center)-[r]->()
                 DELETE center,r""")

    assertStats(result, nodesCreated = 7, propertiesWritten = 7, relationshipsCreated = 21, nodesDeleted = 1, relationshipsDeleted = 6)
  }

  test("for each applied to null should never execute") {
    val result = executeWith(Configs.UpdateConf, "foreach(x in null| create ())")

    assertStats(result, nodesCreated = 0)
  }

  test("should execute when null is contained in a collection") {
    val result = executeWith(Configs.UpdateConf, "foreach(x in [null]| create ())")

    assertStats(result, nodesCreated = 1)
  }

  test("should be possible to remove nodes created in the same query") {
    val result = executeWith(Configs.UpdateConf,
      """CREATE (a)-[:FOO]->(b)
         WITH *
         MATCH (x)-[r]-(y)
         DELETE x, r, y""".stripMargin)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, nodesDeleted = 2, relationshipsDeleted = 1)
  }

  test("all nodes scan after unwind is handled correctly") {
    createNode()
    createNode()
    createNode("prop" -> 42)
    createNode("prop" -> 42)
    val query = "UNWIND range(0, 1) as i MATCH (n) CREATE (m) WITH * MATCH (o) RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3)

    assertStats(result, nodesCreated = 8)
    val unwind = 2
    val firstMatch = 4
    val secondMatch = 12 // The already existing 4 nodes, plus the now created 8
    result.toList should equal(List(Map("count" -> unwind * firstMatch * secondMatch)))
  }
}
