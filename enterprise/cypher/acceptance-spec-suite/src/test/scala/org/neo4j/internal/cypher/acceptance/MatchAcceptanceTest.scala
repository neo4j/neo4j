/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.PathImpl
import org.neo4j.graphdb._
import org.neo4j.helpers.collection.Iterators.single

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

class MatchAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("should not fail when planning independent optional matches") {
    // Should plan without throwing exception
    innerExecute(
      """MATCH (study:Study {UUID:$studyUUID})
        |MATCH (study)-[hasSubject:HAS_SUBJECT]->(subject:Study_Subject)-[:HAS_DATASET]->(dataset:Study_Dataset)<-[:HAS_DATASET]-(study)
        |WHERE toLower(COALESCE(subject.Archived,'false')) = 'false' AND toLower(COALESCE(dataset.Archived,'false')) = 'false'
        |WITH DISTINCT study, { RecordID: COALESCE(hasSubject.RecordID, subject.RecordID), SubjectUUID: subject.UUID } AS derivedData, subject
        |MATCH (subject)-[:HAS_DATASET]-(dataset:Study_Dataset)<-[:HAS_DATASET]-(study)
        |  WHERE size(FILTER(x in labels(dataset) WHERE x in ['YPQ','WASI'])) = 1 AND toLower(COALESCE(dataset.Archived,'false')) = 'false'
        |WITH study, derivedData, subject, collect(dataset) AS datasets
        |MATCH (stai:Study_Dataset:STAI) WHERE (stai IN datasets)
        |MATCH (pswq:Study_Dataset:PSWQ) WHERE (pswq IN datasets)
        |MATCH (pss:Study_Dataset:PSS) WHERE (pss IN datasets)
        |MATCH (njre_q_r:Study_Dataset:NJRE_Q_R) WHERE (njre_q_r IN datasets)
        |MATCH (iu:Study_Dataset:IU) WHERE (iu IN datasets)
        |MATCH (gad_7:Study_Dataset:GAD_7) WHERE (gad_7 IN datasets)
        |MATCH (fmps:Study_Dataset:FMPS) WHERE (fmps IN datasets)
        |MATCH (bdi:Study_Dataset:BDI) WHERE (bdi IN datasets)
        |MATCH (wdq:Study_Dataset:WDQ) WHERE (wdq IN datasets)
        |MATCH (treasurehunt:Study_Dataset:TREASUREHUNT) WHERE (treasurehunt IN datasets)
        |MATCH (scid_v2:Study_Dataset:SCID_V2) WHERE (scid_v2 IN datasets)
        |MATCH (ybocs:Study_Dataset:YBOCS) WHERE (ybocs IN datasets)
        |MATCH (bis:Study_Dataset:BIS) WHERE (bis IN datasets)
        |MATCH (sdq:Study_Dataset:SDQ) WHERE (sdq IN datasets)
        |MATCH (ehi:Study_Dataset:EHI) WHERE (ehi IN datasets)
        |MATCH (oci_r:Study_Dataset:OCI_R) WHERE (oci_r IN datasets)
        |MATCH (pi_wsur:Study_Dataset:PI_WSUR) WHERE (pi_wsur IN datasets)
        |MATCH (rfq:Study_Dataset:RFQ) WHERE (rfq IN datasets)
        |OPTIONAL MATCH (wasi:Study_Dataset:WASI) WHERE (wasi IN datasets)
        |OPTIONAL MATCH (ypq:Study_Dataset:YPQ) WHERE (ypq IN datasets)
        |RETURN DISTINCT derivedData AS DerivedData, subject , stai, pswq, pss, njre_q_r, iu, gad_7, fmps, bdi, wdq, treasurehunt, scid_v2, ybocs, bis, sdq, ehi, oci_r, pi_wsur, rfq, wasi, ypq""".stripMargin)
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
    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result.toSet should equal(Set(Map("d" -> ArrayBuffer(n1)), Map("d" -> ArrayBuffer(n1, n2))))
  }

  test("OPTIONAL MATCH, DISTINCT and DELETE in an unfortunate combination") {
    val start = createLabeledNode("Start")
    createLabeledNode("End")
    val result = executeWithAllPlannersAndCompatibilityMode(
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

    val result = executeWithAllPlannersAndCompatibilityMode(query)
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

  test("should handle optional match of non-existent path.") {

    val n = createLabeledNode("Client")

    val query =
      """ MATCH (n:Client)
        | OPTIONAL MATCH (n)-[rel1:relType]->(n1:label)
        | OPTIONAL MATCH (n1)-[rel2:relType2]->(n2:label2)
        | RETURN n, rel1, n1, rel2, n2;
        |""".stripMargin

    val result = executeWithAllPlannersAndCompatibilityMode(query)
    result.toList should equal(List(Map("n" -> n, "rel1" -> null, "rel2" -> null, "n1" -> null, "n2" -> null)))
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
                  |MATCH paths = (n)-[*..1]-(m)
                  |RETURN paths""".stripMargin

    val result = executeWithAllPlannersAndCompatibilityMode(query, "0" -> node1.getId, "1" -> node2.getId)
    graph.inTx(
      result.toSet should equal(Set(Map("paths" -> PathImpl(node1, r, node2)), Map("paths" -> PathImpl(node2, r, node1))))
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
      executeWithCostPlannerAndInterpretedRuntimeOnly("MATCH (n:FOO) SET n = { first: 'value' }")
      executeWithCostPlannerAndInterpretedRuntimeOnly("MATCH (n:FOO) SET n = { second: 'value' }")
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
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly("MATCH (n:User) USING INDEX n:User(email) WHERE exists(n.email) RETURN n")

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
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly("MATCH (n:User) USING INDEX n:User(email) WHERE n.email IS NOT NULL RETURN n")

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
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly("MATCH (n:User) WHERE exists(n.email) RETURN n")

    // then
    result.toList should equal(List(Map("n" -> nodes.head), Map("n" -> nodes(1))))
    result.executionPlanDescription().toString should include("NodeIndexScan")
  }

  test("should not use the index for property existence queries when property value predicate exists") {
    // given
    val nodes = setupIndexScanTest()

    // when
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly("MATCH (n:User) WHERE exists(n.email) AND n.email = 'me@mine' RETURN n")

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
    val result = executeWithCostPlannerAndInterpretedRuntimeOnly(s"profile WITH [$a,$b,$d] AS arr MATCH (n) WHERE id(n) IN arr return count(*)")

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
      executeWithAllPlannersAndRuntimesAndCompatibilityMode("UNWIND {p} AS n MATCH (n)<-[:PING_DAY]-(p:Ping) RETURN count(p) as c", "p" -> List(node1, node2))

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
      executeWithAllPlannersAndRuntimesAndCompatibilityMode( """UNWIND {p1} AS n1
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
    val res = executeWithAllPlannersAndCompatibilityMode("MATCH (n)-[r:T*2]->() WHERE last(r).roles = 'NEO' RETURN DISTINCT n")

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
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH p=(n)-[*0..3]-() RETURN size(COLLECT(DISTINCT p)) AS size")

    // Then
    result.toList should equal(List(Map("size" -> 8)))
  }

  test("Should handle multiple labels") {
    val n1 = createLabeledNode("A", "B", "C")
    val n2 = createLabeledNode("A", "B")
    createLabeledNode("A", "C")
    createLabeledNode("B", "C")
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a) WHERE a:A:B RETURN a")

    // Then
    result.toList should equal(List(Map("a" -> n1), Map("a" -> n2)))
  }

  test("Should handle multiple labels with OR") {
    val n1 = createLabeledNode("A", "B", "C")
    val n2 = createLabeledNode("A", "B")
    val n3 = createLabeledNode("A", "C")
    createLabeledNode("B", "C")
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a) WHERE a:A:B OR a:A:C RETURN a")

    // Then
    result.toList should equal(List(Map("a" -> n1), Map("a" -> n2), Map("a" -> n3)))
  }

  test("Should handle multiple labels with OR and AND") {
    val n1 = createLabeledNode("A", "B", "C")
    val n2 = createLabeledNode("A", "B")
    val n3 = createLabeledNode("A", "C")
    createLabeledNode("B", "C")
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a) WHERE (a:A AND a:B) OR (a:A AND a:C) RETURN a")

    // Then
    result.toList should equal(List(Map("a" -> n1), Map("a" -> n2), Map("a" -> n3)))
  }

  test("Should handle multiple labels with AND") {
    val n = createLabeledNode("A", "B", "C")
    createLabeledNode("A", "B")
    createLabeledNode("A", "C")
    createLabeledNode("B", "C")
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    val result = executeWithAllPlannersAndRuntimesAndCompatibilityMode("MATCH (a) WHERE a:A:B AND a:A:C RETURN a")

    // Then
    result.toList should equal(List(Map("a" -> n)))
  }

  test("Should handle optional match with null parts and distinct without NullPointerException") {

    val query =
      """
        |  OPTIONAL MATCH (req:Y)
        |  WITH req
        |  OPTIONAL MATCH (req)<-[*2]-(y)
        |  RETURN DISTINCT req.eid, y.eid
      """.stripMargin

    val result = executeWithAllPlannersAndCompatibilityMode(query)

    result.toList should equal(List(Map("req.eid" -> null, "y.eid" -> null)))
  }

  test("Reduce and concat gh #10978") {
    val result = executeWithAllPlannersAndCompatibilityMode("RETURN REDUCE(s = 0, p in [5,8,2,9] + [1,2] | s + p) as num")
    result.toList should be(List(Map("num" -> 27)))
  }
}
