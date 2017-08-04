/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.PathImpl
import org.neo4j.graphdb._
import org.neo4j.helpers.collection.Iterators.single

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class MatchAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  test("Do not count null elements in nodes without labels") {

    createNode("name" -> "a")
    createNode("name" -> "b")
    createNode("name" -> "c")
    createNode()
    createNode()

    val result = succeedWith(Configs.AllExceptSleipnir, "MATCH (n) RETURN count(n.name)")
    result.toList should equal(List(Map("count(n.name)" -> 3)))
  }

  test("Do not count null elements in nodes with labels") {

    createLabeledNode(Map("name" -> "a"), "Person")
    createLabeledNode(Map("name" -> "b"), "Person")
    createLabeledNode(Map("name" -> "c"), "Person")
    createLabeledNode("Person")
    createLabeledNode("Person")

    val count = executeScalar[Long]("MATCH (n:Person) RETURN count(n.name)")
    count should equal(3)
  }

  test("Should not use both pruning var expand and projections that need path info") {

    val n1 = createLabeledNode("Neo")
    val n2 = createLabeledNode()
    createLabeledNode()
    relate(n1, n2)

    val query =
      """
        |MATCH p=(source:Neo)-[rel *0..1]->(dest)
        |WITH nodes(p) as d
        |RETURN DISTINCT d""".stripMargin
    val result = succeedWith(Configs.CommunityInterpreted, query)

    result.toSet should equal(Set(Map("d" -> ArrayBuffer(n1)), Map("d" -> ArrayBuffer(n1, n2))))
  }

  test("OPTIONAL MATCH, DISTINCT and DELETE in an unfortunate combination") {
    val start = createLabeledNode("Start")
    createLabeledNode("End")
    val result = testWithUpdate(Configs.CommunityInterpreted - Configs.Cost2_3,
      """
        |MATCH (start:Start),(end:End)
        |OPTIONAL MATCH (start)-[rel]->(end)
        |DELETE rel
        |RETURN DISTINCT start""".stripMargin)

    result.toList should equal(List(Map("start" -> start)))
  }

  test("should allow for OPTONAL MATCH with horizon and aggregating function") {
    //This is a test to ensure that a bug does not return
    val query =
      """
        |MATCH (a)-[:rel]->(b:label1)
        |WITH a, COLLECT( DISTINCT(b) ) as b
        |OPTIONAL MATCH (a)-[:rel2]->(c:label2)-[:rel3]->(:label3)
        |RETURN a, b, COLLECT( DISTINCT c) as c
      """.stripMargin

    val result = succeedWith(Configs.CommunityInterpreted, query)
    result.size should be(0)
    result.hasNext should be(false)

  }

  // Not TCK material -- only one integer type
  test("comparing numbers should work nicely") {
    val n1 = createNode(Map("x" -> 50))
    val n2 = createNode(Map("x" -> 50l))
    val n3 = createNode(Map("x" -> 50f))
    val n4 = createNode(Map("x" -> 50d))
    val n5 = createNode(Map("x" -> 50.toByte))

    val result = succeedWith(Configs.Interpreted,
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

    val result = succeedWith(Configs.CommunityInterpreted,
      s"match (n) where n.x < 'Z' AND n.x < 'z' return n"
    )

    result.columnAs("n").toList should equal(List(n1, n2))
  }

  // Not TCK material -- shortestPath(), allShortestPaths()
  test("should return shortest path") {
    createNodes("A", "B")
    val r1 = relate("A" -> "KNOWS" -> "B")

    val result = succeedWith(Configs.CommunityInterpreted, "match p = shortestPath((a {name:'A'})-[*..15]-(b {name:'B'})) return p").
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
    succeedWith(Configs.CommunityInterpreted, "match p = shortestPath((a {name:'A'})-[*]-(b {name:'B'})) return p").toList
  }

  test("should not traverse same relationship twice in shortest path") {
    // given
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    // when
    val result = succeedWith(Configs.CommunityInterpreted, "MATCH (a{name:'A'}), (b{name:'B'}) MATCH p=allShortestPaths((a)-[:KNOWS|KNOWS*]->(b)) RETURN p").
      toList

    // then
    graph.inTx {
      result.size should equal (1)
    }
  }

  test("should handle optional match of non-existent path.") {

    val n = createLabeledNode("Client")

    val query =
      """ MATCH (n:Client)
        | OPTIONAL MATCH (n)-[rel1:relType]->(n1:label)
        | OPTIONAL MATCH (n1)-[rel2:relType2]->(n2:label2)
        | RETURN n, rel1, n1, rel2, n2;
        |""".stripMargin

    val result = succeedWith(Configs.CommunityInterpreted, query)
    result.toList should equal(List(Map("n" -> n, "rel1" -> null, "rel2" -> null, "n1" -> null, "n2" -> null)))
  }

  test("should handle optional paths from graph algo") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = succeedWith(Configs.CommunityInterpreted,  """
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

    val result = succeedWith(Configs.CommunityInterpreted,  """
match (a), (d) where id(a) = 0 and id(d) = 3
match p = allShortestPaths( (a)-[*]->(d) )
return p""")

    result.toList.size should equal (2)
  }

  test("shortest Path Direction Respected") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    val result = succeedWith(Configs.CommunityInterpreted, "match (a), (b) where id(a) = 0 and id(b) = 1 match p=shortestPath((b)<-[*]-(a)) return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal (b)
    result.endNode() should equal (a)
  }

  test("should return shortest paths when only one side is bound") {
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")
    val r1 = relate(a, b)

    val result = succeedWith(Configs.CommunityInterpreted, "match (a:A) match p = shortestPath( (a)-[*]->(b:B) ) return p").toList.head("p").asInstanceOf[Path]

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
                  |MATCH paths = (n)-[*..1]-(m)
                  |RETURN paths""".stripMargin

    val result = succeedWith(Configs.CommunityInterpreted, query, "0" -> node1.getId, "1" -> node2.getId)
    graph.inTx(
      result.toSet should equal(Set(Map("paths" -> PathImpl(node1, r, node2)), Map("paths" -> PathImpl(node2, r, node1))))
    )
  }

  // -- End of shortest path

  // Not TCK material -- filter()
  test("length on filter") {
    val q = "match (n) optional match (n)-[r]->(m) return length(filter(x in collect(r) WHERE x <> null)) as cn"

    succeedWith(Configs.CommunityInterpreted, q).toList should equal (List(Map("cn" -> 0)))
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
    val result = succeedWith(Configs.All, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name = 'Jacob' RETURN n")

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
    val result = succeedWith(Configs.Interpreted, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name STARTS WITH 'Jac' RETURN n")

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
    val result = succeedWith(Configs.Interpreted, "MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name > 'Jac' RETURN n")

    // then
    result.toList should equal (List(Map("n" -> jake)))
    result.executionPlanDescription().toString should include("IndexSeek")
  }

  // End of index hints

  test("filter on edge property") {
    val query = "MATCH ()-[r]->() WHERE r.prop > 5 RETURN r"

    val node1 = createNode()
    val node2 = createNode()
    val r1 = relate(node1, node2, "prop" -> 10)
    val r2 = relate(node1, node2, "prop" -> 0)

    val result = succeedWith(Configs.All - Configs.Compiled, query)

    result.toList should equal (List(Map("r" -> r1)))
  }

  // Not TCK material -- id()
  test("id in where leads to empty result") {
    // when
    val result = succeedWith(Configs.All, "MATCH (n) WHERE id(n)=1337 RETURN n")

    // then DOESN'T THROW EXCEPTION
    result shouldBe empty
  }

  test("should not fail if asking for a non existent node id with WHERE") {
    succeedWith(Configs.Interpreted, "match (n) where id(n) in [0,1] return n").toList
    // should not throw an exception
  }

  test("should be able to set properties with a literal map twice in the same transaction") {
    val node = createLabeledNode("FOO")

    succeedWith(Configs.CommunityInterpreted - Configs.Cost2_3, "MATCH (n:FOO) SET n = { first: 'value' }")
    succeedWith(Configs.CommunityInterpreted - Configs.Cost2_3, "MATCH (n:FOO) SET n = { second: 'value' }")

    graph.inTx {
      node.getProperty("first", null) should equal(null)
      node.getProperty("second") should equal("value")
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
    val result = succeedWith(Configs.All, "match (a:Label)-->(b:Label) where a.property = b.property return a, b")

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
    val result = succeedWith(Configs.Interpreted, "match (a:Label), (b:Label) where a.property = b.property return *")

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
    val result = succeedWith(Configs.Interpreted, "MATCH (n:User) USING INDEX n:User(email) WHERE exists(n.email) RETURN n")

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
    val result = succeedWith(Configs.Interpreted, "MATCH (n:User) USING INDEX n:User(email) WHERE n.email IS NOT NULL RETURN n")

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
    val result = succeedWith(Configs.Interpreted, "MATCH (n:User) WHERE exists(n.email) RETURN n")

    // then
    result.toList should equal(List(Map("n" -> nodes.head), Map("n" -> nodes(1))))
    result.executionPlanDescription().toString should include("NodeIndexScan")
  }

  test("should not use the index for property existence queries when property value predicate exists") {
    // given
    val nodes = setupIndexScanTest()

    // when
    val result = succeedWith(Configs.CommunityInterpreted, "MATCH (n:User) WHERE exists(n.email) AND n.email = 'me@mine' RETURN n")

    // then
    result.toList should equal(List(Map("n" -> nodes.head)))
    result.executionPlanDescription().toString should include("NodeIndexSeek")
    result.executionPlanDescription().toString should not include "NodeIndexScan"
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
    val first = testWithUpdate(Configs.CommunityInterpreted - Configs.Cost2_3, query).length
    val second = testWithUpdate(Configs.CommunityInterpreted - Configs.Cost2_3, query).length
    val check = succeedWith(Configs.All, "MATCH (f:Folder) RETURN f.name").toSet

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

    val first = testWithUpdate(Configs.CommunityInterpreted - Configs.Cost2_3, query).length
    val second = testWithUpdate(Configs.CommunityInterpreted - Configs.Cost2_3, query).length
    val check = succeedWith(Configs.All, "MATCH (f:Folder) RETURN f.name").toSet

    //THEN
    first should equal(second)
    check should equal(Set(Map("f.name" -> "Dir1"), Map("f.name" -> "Dir2")))
  }

  // Not TCK material -- id()
  test("should return empty result when there are no relationship with the given id") {
    succeedWith(Configs.AllExceptSleipnir, "MATCH ()-[r]->() WHERE id(r) = 42 RETURN r") shouldBe empty
    succeedWith(Configs.AllExceptSleipnir, "MATCH ()<-[r]-() WHERE id(r) = 42 RETURN r") shouldBe empty
    succeedWith(Configs.AllExceptSleipnir, "MATCH ()-[r]-() WHERE id(r) = 42 RETURN r") shouldBe empty
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
    val result = succeedWith(Configs.CommunityInterpreted, s"profile WITH [$a,$b,$d] AS arr MATCH (n) WHERE id(n) IN arr return count(*)")

    // then
    result.toList should equal(List(Map("count(*)" -> 3)))
  }

  // Not sure if TCK material -- is this test just for `columns()`?
  test("columns should be in the provided order") {
    val result = succeedWith(Configs.All, "MATCH (p),(o),(n),(t),(u),(s) RETURN p,o,n,t,u,s")

    result.columns should equal(List("p", "o", "n", "t", "u", "s"))
  }

  test("should produce the same amount of rows on all planners") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    relate(b, a)

    val query = "MATCH (a) MERGE (b) WITH * OPTIONAL MATCH (a)--(b) RETURN count(*)"

    val result = succeedWith(Configs.CommunityInterpreted - Configs.Cost2_3, query)

    result.columnAs[Long]("count(*)").next shouldBe 6
  }

  test("should handle skip regardless of runtime") {
    createNode()
    createNode()
    createNode()
    createNode()
    createNode()

    succeedWith(Configs.All, "MATCH (n) RETURN n SKIP 0") should have size 5
    succeedWith(Configs.All, "MATCH (n) RETURN n SKIP 1") should have size 4
    succeedWith(Configs.All, "MATCH (n) RETURN n SKIP 2") should have size 3
    succeedWith(Configs.All, "MATCH (n) RETURN n SKIP 3") should have size 2
    succeedWith(Configs.All, "MATCH (n) RETURN n SKIP 4") should have size 1
    succeedWith(Configs.All, "MATCH (n) RETURN n SKIP 5") should have size 0
    succeedWith(Configs.All, "MATCH (n) RETURN n SKIP 6") should have size 0

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

  test("should handle unwind on a list of nodes in both runtimes") {
    // Given
    val node1 = createNode()
    val node2 = createNode()
    relate(createLabeledNode("Ping"), node1, "PING_DAY")
    relate(createLabeledNode("Ping"), node2, "PING_DAY")
    relate(createLabeledNode("Ping"), createNode(), "PING_DAY")
    relate(createLabeledNode("Ping"), createNode(), "PING_DAY")

    // When
    val res =
      succeedWith(Configs.AllExceptSleipnir, "UNWIND {p} AS n MATCH (n)<-[:PING_DAY]-(p:Ping) RETURN count(p) as c", "p" -> List(node1, node2))

    //Then
    res.toList should equal(List(Map("c" -> 2)))
  }

  test("should handle unwind followed by expand into on a list of nodes in both runtimes") {
    // Given
    val node1 = createNode("prop" -> 1)
    val node2 = createLabeledNode(Map("prop" -> 2), "Ping")
    relate(node2, node1, "PING_DAY")
    relate(createLabeledNode("Ping"), createNode(), "PING_DAY")
    relate(createLabeledNode("Ping"), createNode(), "PING_DAY")
    relate(createLabeledNode("Ping"), createNode(), "PING_DAY")

    // When
    val res =
      succeedWith(Configs.AllExceptSleipnir,
        """UNWIND {p1} AS n1
          |UNWIND {p2} AS n2
          |MATCH (n1)<-[:PING_DAY]-(n2) RETURN n1.prop, n2.prop""".stripMargin,
        "p1" -> List(node1), "p2" -> List(node2))

    //Then
    res.toList should equal(List(Map("n1.prop" -> 1, "n2.prop" -> 2)))
  }

  test("should handle distinct, variable length and relationship predicate") {
    // Given
    val node1 = createNode()
    val node2 = createNode()
    val node3 = createNode()
    val node4 = createNode()
    relate(node1, node2, "T", Map("roles" -> "NEO"))
    relate(node2, node3, "T", Map("roles" -> "NEO"))
    relate(node2, node4, "T", Map("roles" -> "NEO"))

    // When
    val res = succeedWith(Configs.CommunityInterpreted, "MATCH (n)-[r:T*2]->() WHERE last(r).roles = 'NEO' RETURN DISTINCT n")

    // Then
    res.toSet should equal(Set(Map("n" -> node1)))
  }

  test("should handle distinct collect where the path is needed") {
    // Given
    val n1 = createNode()
    val n2 = createNode()
    val n3 = createNode()
    relate(n1, n2)
    relate(n3, createNode())

    // When
    val result = succeedWith(Configs.CommunityInterpreted, "MATCH p=(n)-[*0..3]-() RETURN size(COLLECT(DISTINCT p)) AS size")

    // Then
    result.toList should equal(List(Map("size" -> 8)))
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
