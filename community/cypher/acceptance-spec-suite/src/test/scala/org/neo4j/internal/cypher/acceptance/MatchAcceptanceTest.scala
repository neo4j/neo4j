/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.PathImpl
import org.neo4j.graphdb._
import org.neo4j.helpers.collection.Iterators.single

import scala.collection.JavaConverters._

class MatchAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("should fail if columnAs refers to unknown column") {
    val n1 = createNode()
    val n2 = createNode()
    val r = relate(n1, n2)
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n)-[r]->() RETURN n, r")
    a[NotFoundException] should be thrownBy result.columnAs("m")
  }

  // Not TCK material -- only one integer type
  test("comparing numbers should work nicely") {
    val n1 = createNode(Map("x" -> 50))
    val n2 = createNode(Map("x" -> 50l))
    val n3 = createNode(Map("x" -> 50f))
    val n4 = createNode(Map("x" -> 50d))
    val n5 = createNode(Map("x" -> 50.toByte))

    val result = executeWithAllPlannersAndCompatibilityMode(
      s"match (n) where n.x < 100 return n"
    )

    result.columnAs[Node]("n").toList should equal(List(n1, n2, n3, n4, n5))
  }

  // Not TCK material -- no character type
  test("comparing string and chars should work nicely") {
    val n1 = createNode(Map("x" -> "Anders"))
    val n2 = createNode(Map("x" -> 'C'))
    createNode(Map("x" -> "Zzing"))
    createNode(Map("x" -> 'Ä'))

    val result = executeWithAllPlannersAndCompatibilityMode(
      s"match (n) where n.x < 'Z' AND n.x < 'z' return n"
    )

    result.columnAs("n").toList should equal(List(n1, n2))
  }

  // Not TCK material -- shortestPath(), allShortestPaths()

  test("should return shortest path") {
    createNodes("A", "B")
    val r1 = relate("A" -> "KNOWS" -> "B")

    val result = executeWithAllPlannersAndCompatibilityMode("match p = shortestPath((a {name:'A'})-[*..15]-(b {name:'B'})) return p").
      toList.head("p").asInstanceOf[Path]

    graph.inTx {
      val number_of_relationships_in_path = result.length()
      number_of_relationships_in_path should equal (1)
      result.startNode() should equal (node("A"))
      result.endNode() should equal (node("B"))
      result.lastRelationship() should equal (r1)
    }
  }

  test("should return shortest path unbound length") {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    //Checking that we don't get an exception
    executeWithAllPlannersAndCompatibilityMode("match p = shortestPath((a {name:'A'})-[*]-(b {name:'B'})) return p").toList
  }

  test("should not traverse same relationship twice in shortest path") {
    // given
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    // when
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a{name:'A'}), (b{name:'B'}) MATCH p=allShortestPaths((a)-[:KNOWS|KNOWS*]->(b)) RETURN p").
      toList

    // then
    graph.inTx {
      result.size should equal (1)
    }
  }

  test("finds a single path for paths of length one") {
    /*
       (a)-(b)-c
     */
    val nodeA = createLabeledNode("A")
    val nodeB = createLabeledNode("B")
    val nodeC = createLabeledNode("C")
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlannersAndCompatibilityMode("match p = shortestpath((a:A)-[r*..1]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA, nodeB)))
  }

  test("if asked for also return paths of length 0") {
    /*
       (a)-(b)-c
     */
    val nodeA = createLabeledNode("A")
    val nodeB = createLabeledNode("B")
    val nodeC = createLabeledNode("C")
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlannersAndCompatibilityMode("match p = shortestpath((a:A)-[r*0..1]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA), List(nodeA, nodeB)))
  }

  test("if asked for also return paths of length 0, even when no max length is speficied") {
    /*
       (a)-(b)-c
     */
    val nodeA = createLabeledNode("A")
    val nodeB = createLabeledNode("B")
    val nodeC = createLabeledNode("C")
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlannersAndCompatibilityMode("match p = shortestpath((a:A)-[r*0..]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA), List(nodeA, nodeB), List(nodeA, nodeB, nodeC)))
  }

  test("we can ask explicitly for paths of minimal length 1") {
    /*
       (a)-(b)-c
     */
    val nodeA = createLabeledNode("A")
    val nodeB = createLabeledNode("B")
    val nodeC = createLabeledNode("C")
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlannersAndCompatibilityMode("match p = shortestpath((a:A)-[r*1..1]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA, nodeB)))
  }

  test("finds a single path for non-variable length paths") {
    /*
       (a)-(b)-c
     */
    val nodeA = createLabeledNode("A")
    val nodeB = createLabeledNode("B")
    val nodeC = createLabeledNode("C")
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlannersAndCompatibilityMode("match p = shortestpath((a:A)-[r]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA, nodeB)))
  }

  test("should handle optional paths from graph algo") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = executeWithAllPlannersAndCompatibilityMode( """
match (a {name:'A'}), (x) where x.name in ['B', 'C']
optional match p = shortestPath((a)-[*]->(x))
return x, p""").toSet

    graph.inTx(assert(Set(
      Map("x" -> b, "p" -> PathImpl(a, r, b)),
      Map("x" -> c, "p" -> null)
    ) === result))
  }

  test("should handle all shortest paths") {
    createDiamond()

    val result = executeWithAllPlannersAndCompatibilityMode( """
match (a), (d) where id(a) = 0 and id(d) = 3
match p = allShortestPaths( (a)-[*]->(d) )
return p""")

    result.toList.size should equal (2)
  }

  test("shortest Path Direction Respected") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    val result = executeWithAllPlannersAndCompatibilityMode("match (a), (b) where id(a) = 0 and id(b) = 1 match p=shortestPath((b)<-[*]-(a)) return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal (b)
    result.endNode() should equal (a)
  }

  test("should return shortest paths when only one side is bound") {
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")
    val r1 = relate(a, b)

    val result = executeWithAllPlannersAndCompatibilityMode("match (a:A) match p = shortestPath( (a)-[*]->(b:B) ) return p").toList.head("p").asInstanceOf[Path]

    graph.inTx {
      result.startNode() should equal(a)
      result.endNode() should equal(b)
      result.length() should equal(1)
      result.lastRelationship() should equal (r1)
    }
  }

  test("should handle cartesian products even when same argument exists on both sides") {
    val node1 = createNode()
    val node2 = createNode()
    val r = relate(node1, node2)

    val query = """WITH [{0}, {1}] AS x, count(*) as y
                  |MATCH (n) WHERE ID(n) IN x
                  |MATCH (m) WHERE ID(m) IN x
                  |MATCH paths = allShortestPaths((n)-[*..1]-(m))
                  |RETURN paths""".stripMargin

    val result = executeWithAllPlannersAndCompatibilityMode(query, "0" -> node1.getId, "1" -> node2.getId)
    graph.inTx(
      result.toSet should equal(Set(Map("paths" -> new PathImpl(node1, r, node2)), Map("paths" -> new PathImpl(node2, r, node1))))
    )
  }

  // -- End of shortest path

  // Not TCK material -- filter()
  test("length on filter") {
    val q = "match (n) optional match (n)-[r]->(m) return length(filter(x in collect(r) WHERE x <> null)) as cn"

    executeWithAllPlannersAndCompatibilityMode(q).toList should equal (List(Map("cn" -> 0)))
  }

  // Not TCK material -- index hints

  test("should be able to use index hints") {
    // given
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    // when
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name = 'Jacob' RETURN n")

    // then
    result.toList should equal (List(Map("n" -> jake)))
    result.executionPlanDescription().toString should include("IndexSeek")
  }

  test("should be able to use index hints with STARTS WITH predicates") {
    // given
    (1 to 50) foreach (i => createLabeledNode(Map("name" -> ("Robot" + i)), "Person"))
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    // when
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name STARTS WITH 'Jac' RETURN n")

    // then
    result.toList should equal (List(Map("n" -> jake)))
    result.executionPlanDescription().toString should include("IndexSeek")
  }

  test("should be able to use index hints with inequality/range predicates") {
    // given
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    // when
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name > 'Jac' RETURN n")

    // then
    result.toList should equal (List(Map("n" -> jake)))
    result.executionPlanDescription().toString should include("IndexSeek")
  }

  // End of index hints

  // Not TCK material -- id()
  test("id in where leads to empty result") {
    // when
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n) WHERE id(n)=1337 RETURN n")

    // then DOESN'T THROW EXCEPTION
    result shouldBe empty
  }

  test("should not fail if asking for a non existent node id with WHERE") {
    executeWithAllPlanners("match (n) where id(n) in [0,1] return n").toList
    // should not throw an exception
  }

  test("should be able to set properties with a literal map twice in the same transaction") {
    val node = createLabeledNode("FOO")

    graph.inTx {
      executeWithCostPlannerOnly("MATCH (n:FOO) SET n = { first: 'value' }")
      executeWithCostPlannerOnly("MATCH (n:FOO) SET n = { second: 'value' }")
    }

    graph.inTx {
      node.getProperty("first", null) should equal (null)
      node.getProperty("second") should equal ("value")
    }
  }

  // Not TCK material -- indexes

  test("should handle queries that cant be index solved because expressions lack dependencies") {
    // given
    val a = createLabeledNode(Map("property" -> 42), "Label")
    val b = createLabeledNode(Map("property" -> 42), "Label")
    createLabeledNode(Map("property" -> 666), "Label")
    createLabeledNode(Map("property" -> 666), "Label")
    val e = createLabeledNode(Map("property" -> 1), "Label")
    relate(a, b)
    relate(a, e)
    graph.createIndex("Label", "property")

    // when
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (a:Label)-->(b:Label) where a.property = b.property return a, b")

    // then does not throw exceptions
    result.toList should equal (List(Map("a" -> a, "b" -> b)))
  }

  test("should handle queries that cant be index solved because expressions lack dependencies with two disjoin patterns") {
    // given
    val a = createLabeledNode(Map("property" -> 42), "Label")
    val b = createLabeledNode(Map("property" -> 42), "Label")
    val e = createLabeledNode(Map("property" -> 1), "Label")
    graph.createIndex("Label", "property")

    // when
    val result = executeWithAllPlannersAndCompatibilityMode("match (a:Label), (b:Label) where a.property = b.property return *")

    // then does not throw exceptions
    assert(result.toSet === Set(
      Map("a" -> a, "b" -> a),
      Map("a" -> a, "b" -> b),
      Map("a" -> b, "b" -> b),
      Map("a" -> b, "b" -> a),
      Map("a" -> e, "b" -> e)
    ))
  }

  test("should use the index for property existence queries (with exists) for cost when asked for it") {
    // given
    val n = createLabeledNode(Map("email" -> "me@mine"), "User")
    val m = createLabeledNode(Map("email" -> "you@yours"), "User")
    val p = createLabeledNode(Map("emailx" -> "youtoo@yours"), "User")
    graph.createIndex("User", "email")

    // when
    val result = executeWithCostPlannerOnly("MATCH (n:User) USING INDEX n:User(email) WHERE exists(n.email) RETURN n")

    // then
    result.toList should equal(List(Map("n" -> n), Map("n" -> m)))
    result.executionPlanDescription().toString should include("NodeIndexScan")
  }

  test("should use the index for property existence queries (with IS NOT NULL) for cost when asked for it") {
    // given
    val n = createLabeledNode(Map("email" -> "me@mine"), "User")
    val m = createLabeledNode(Map("email" -> "you@yours"), "User")
    val p = createLabeledNode(Map("emailx" -> "youtoo@yours"), "User")
    graph.createIndex("User", "email")

    // when
    val result = executeWithCostPlannerOnly("MATCH (n:User) USING INDEX n:User(email) WHERE n.email IS NOT NULL RETURN n")

    // then
    result.toList should equal(List(Map("n" -> n), Map("n" -> m)))
    result.executionPlanDescription().toString should include("NodeIndexScan")
  }

  private def setupIndexScanTest(): Seq[Node] = {
    for (i <- 1 to 100) {
      createLabeledNode(Map("name" -> ("Joe Soap " + i)), "User")
    }
    val n = createLabeledNode(Map("email" -> "me@mine"), "User")
    val m = createLabeledNode(Map("email" -> "you@yours"), "User")
    val p = createLabeledNode(Map("emailx" -> "youtoo@yours"), "User")
    graph.createIndex("User", "email")
    graph.createIndex("User", "name")
    Seq(n, m, p)
  }

  test("should use the index for property existence queries when cardinality prefers it") {
    // given
    val nodes = setupIndexScanTest()

    // when
    val result = executeWithCostPlannerOnly("MATCH (n:User) WHERE exists(n.email) RETURN n")

    // then
    result.toList should equal(List(Map("n" -> nodes(0)), Map("n" -> nodes(1))))
    result.executionPlanDescription().toString should include("NodeIndexScan")
  }

  test("should not use the index for property existence queries when property value predicate exists") {
    // given
    val nodes = setupIndexScanTest()

    // when
    val result = executeWithCostPlannerOnly("MATCH (n:User) WHERE exists(n.email) AND n.email = 'me@mine' RETURN n")

    // then
    result.toList should equal(List(Map("n" -> nodes(0))))
    result.executionPlanDescription().toString should include("NodeIndexSeek")
    result.executionPlanDescription().toString should not include "NodeIndexScan"
  }

  test("should use the index for property existence queries for rule when asked for it") {
    // given
    val n = createLabeledNode(Map("email" -> "me@mine"), "User")
    val m = createLabeledNode(Map("email" -> "you@yours"), "User")
    val p = createLabeledNode(Map("emailx" -> "youtoo@yours"), "User")
    graph.createIndex("User", "email")

    // when
    val result = innerExecute("CYPHER planner=rule MATCH (n:User) USING INDEX n:User(email) WHERE exists(n.email) RETURN n")

    // then
    result.toList should equal(List(Map("n" -> n), Map("n" -> m)))
    result.executionPlanDescription().toString should include("SchemaIndex")
  }

  test("should not use the index for property existence queries for rule when not asking for it") {
    // given
    val n = createLabeledNode(Map("email" -> "me@mine"), "User")
    val m = createLabeledNode(Map("email" -> "you@yours"), "User")
    val p = createLabeledNode(Map("emailx" -> "youtoo@yours"), "User")
    graph.createIndex("User", "email")

    // when
    val result = innerExecute("CYPHER planner=rule MATCH (n:User) WHERE exists(n.email) RETURN n")

    // then
    result.toList should equal(List(Map("n" -> n), Map("n" -> m)))
    result.executionPlanDescription().toString should not include "SchemaIndex"
  }

  test("index hints should work in optional match") {
    //GIVEN
    val subnet = createLabeledNode("Subnet")
    createLabeledNode("Subnet")//extra dangling subnet
    val host = createLabeledNode(Map("name" -> "host"), "Host")

    relate(subnet, host)

    graph.createIndex("Host", "name")

    val query =
      """MATCH (subnet: Subnet)
        |OPTIONAL MATCH (subnet)-->(host:Host)
        |USING INDEX host:Host(name)
        |WHERE host.name = 'host'
        |RETURN host""".stripMargin

    //WHEN
    val result = profile(query)

    //THEN
    result.toList should equal (List(Map("host" -> host), Map("host" -> null)))
  }

  // End of indexes

  // Non-deterministic -- requires new TCK design
  test("should only evaluate non-deterministic predicates after pattern is matched") {
    // given
    graph.inTx {
      (0 to 100) foreach {
        x => createNode()
      }
    }

    // when
    val count = executeScalar[Long]("match (a) where rand() < .5 return count(*)")

    // should give us a number in the middle, not all or nothing
    count should not equal 0
    count should not equal 100
  }

  // Not TCK material -- FOREACH
  test("Should be able to run delete/merge query multiple times") {
    //GIVEN
    createLabeledNode("User")

    val query = """MATCH (user:User)
                  |MERGE (project:Project {p: 'Test'})
                  |MERGE (user)-[:HAS_PROJECT]->(project)
                  |WITH project
                  |    // delete the current relations to be able to replace them with new ones
                  |OPTIONAL MATCH (project)-[hasFolder:HAS_FOLDER]->(:Folder)
                  |OPTIONAL MATCH (project)-[:HAS_FOLDER]->(folder:Folder)
                  |DELETE folder, hasFolder
                  |WITH project
                  |   // add the new relations and objects
                  |FOREACH (el in[{name:"Dir1"}, {name:"Dir2"}] |
                  |  MERGE (folder:Folder{ name: el.name })
                  |  MERGE (project)–[:HAS_FOLDER]->(folder))
                  |WITH DISTINCT project
                  |RETURN project.p""".stripMargin

    //WHEN
    val first = updateWithBothPlannersAndCompatibilityMode(query).length
    val second = updateWithBothPlannersAndCompatibilityMode(query).length
    val check = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (f:Folder) RETURN f.name").toSet

    //THEN
    first should equal(second)
    check should equal(Set(Map("f.name" -> "Dir1"), Map("f.name" -> "Dir2")))
  }

  // Not TCK material -- FOREACH
  test("Should be able to run delete/merge query multiple times, match on property") {
    //GIVEN
    createLabeledNode("User")


    val query = """MATCH (user:User)
                  |MERGE (project:Project {p: 'Test'})
                  |MERGE (user)-[:HAS_PROJECT]->(project)
                  |WITH project
                  |    // delete the current relations to be able to replace them with new ones
                  |OPTIONAL MATCH (project)-[hasFolder:HAS_FOLDER]->({name: "Dir2"})
                  |OPTIONAL MATCH (project)-[hasFolder2:HAS_FOLDER]->({name: "Dir1"})
                  |OPTIONAL MATCH (project)-[:HAS_FOLDER]->(folder {name: "Dir1"})
                  |DELETE folder, hasFolder, hasFolder2
                  |WITH project
                  |   // add the new relations and objects
                  |FOREACH (el in[{name:"Dir1"}, {name:"Dir2"}] |
                  |  MERGE (folder:Folder{ name: el.name })
                  |  MERGE (project)–[:HAS_FOLDER]->(folder))
                  |WITH DISTINCT project
                  |RETURN project.p""".stripMargin

    //WHEN

    val first = updateWithBothPlannersAndCompatibilityMode(query).length
    val second = updateWithBothPlannersAndCompatibilityMode(query).length
    val check = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (f:Folder) RETURN f.name").toSet

    //THEN
    first should equal(second)
    check should equal(Set(Map("f.name" -> "Dir1"), Map("f.name" -> "Dir2")))
  }

  // Not TCK material -- id()
  test("should return empty result when there are no relationship with the given id") {
    executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()-[r]->() WHERE id(r) = 42 RETURN r") shouldBe empty
    executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()<-[r]-() WHERE id(r) = 42 RETURN r") shouldBe empty
    executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH ()-[r]-() WHERE id(r) = 42 RETURN r") shouldBe empty
  }

  // Not TCK material -- id()
  test("should use NodeByIdSeek for id array in variables") {
    // given
    val a = createNode().getId
    val b = createNode().getId
    val c = createNode().getId
    val d = createNode().getId
    1.to(1000).foreach(_ => createNode())

    // when
    val result = executeWithCostPlannerOnly(s"profile WITH [$a,$b,$d] AS arr MATCH (n) WHERE id(n) IN arr return count(*)")

    // then
    result.toList should equal(List(Map("count(*)" -> 3)))
  }

  // Not sure if TCK material -- is this test just for `columns()`?
  test("columns should be in the provided order") {
    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (p),(o),(n),(t),(u),(s) RETURN p,o,n,t,u,s")

    result.columns should equal(List("p", "o", "n", "t", "u", "s"))
  }

  test("should produce the same amount of rows on all planners") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    relate(b, a)

    val query = "MATCH (a) MERGE (b) WITH * OPTIONAL MATCH (a)--(b) RETURN count(*)"

    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result.columnAs[Long]("count(*)").next shouldBe 6
  }

  test("should handle skip regardless of runtime") {
    createNode()
    createNode()
    createNode()
    createNode()
    createNode()

    executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n) RETURN n SKIP 0") should have size 5
    executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n) RETURN n SKIP 1") should have size 4
    executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n) RETURN n SKIP 2") should have size 3
    executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n) RETURN n SKIP 3") should have size 2
    executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n) RETURN n SKIP 4") should have size 1
    executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n) RETURN n SKIP 5") should have size 0
    executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (n) RETURN n SKIP 6") should have size 0

  }

  // Not TCK material -- cardinality estimation
  test("aliasing node names should not change estimations but it should simply introduce a projection") {
    val b = createLabeledNode("B")
    (0 to 10).foreach { i =>
      val a = createLabeledNode("A")
      relate(a, b)
    }

    val resultNoAlias = graph.execute("MATCH (a:A) with a SKIP 0 MATCH (a)-[]->(b:B) return a, b")
    resultNoAlias.asScala.toList.size should equal(11)
    val resultWithAlias = graph.execute("MATCH (a:A) with a as n SKIP 0 MATCH (n)-[]->(b:B) return n, b")
    resultWithAlias.asScala.toList.size should equal(11)

    var descriptionNoAlias = resultNoAlias.getExecutionPlanDescription
    var descriptionWithAlias = resultWithAlias.getExecutionPlanDescription
    descriptionWithAlias.getArguments.get("EstimatedRows") should equal(descriptionNoAlias.getArguments.get("EstimatedRows"))
    while (descriptionWithAlias.getChildren.isEmpty) {
      descriptionWithAlias = single(descriptionWithAlias.getChildren.iterator())
      if ( descriptionWithAlias.getName != "Projection" ) {
        descriptionNoAlias = single(descriptionNoAlias.getChildren.iterator())
        descriptionWithAlias.getArguments.get("EstimatedRows") should equal(descriptionNoAlias.getArguments.get("EstimatedRows"))
      }
    }

    resultNoAlias.close()
    resultWithAlias.close()
  }

  /**
   * Append variable to keys and transform value arrays to lists
   */
  private def asResult(data: Map[String, Any], id: String) =
    data.map {
      case (k, v) => (s"$id.$k", v)
    }.mapValues {
      case v: Array[_] => v.toList
      case v => v
    }
}
