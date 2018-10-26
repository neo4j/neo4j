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

import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.internal.cypher.acceptance.comparisonsupport.CypherComparisonSupport
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Planners
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Runtimes
import org.neo4j.internal.cypher.acceptance.comparisonsupport.Versions

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import org.neo4j.internal.cypher.acceptance.comparisonsupport.{Configs, TestConfiguration}

class CreateAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport
  with CreateTempFileTestSupport {

  private val BIG_TIMEOUT = 15 minutes
  private val BIG_N = 1000
  private val BIG_CREATE_CONFIGS =
    TestConfiguration(Versions.V3_5, Planners.all, Runtimes(Runtimes.Interpreted, Runtimes.Slotted)) +
    TestConfiguration(Versions.V3_4, Planners.all, Runtimes(Runtimes.Interpreted))

  test("handle big CREATE clause") {
    var query = "CREATE (x)"
    for (i <- 1 to BIG_N) {
      query += s" ,(a$i)-[:R]->(b$i)"
    }

    val futureResult = Future(executeWith(BIG_CREATE_CONFIGS, query, executeExpectedFailures = false))
    val result = Await.result(futureResult, BIG_TIMEOUT)
    assertStats(result, nodesCreated = BIG_N * 2 + 1, relationshipsCreated = BIG_N)
  }

  test("handle many CREATE clauses") {
    var query = "CREATE (x)"
    for (i <- 1 to BIG_N) {
      query += s" CREATE (a$i)-[:R]->(b$i)"
    }

    val futureResult = Future(executeWith(BIG_CREATE_CONFIGS, query, executeExpectedFailures = false))
    val result = Await.result(futureResult, BIG_TIMEOUT)
    assertStats(result, nodesCreated = BIG_N * 2 + 1, relationshipsCreated = BIG_N)
  }

  test("PROFILE big CREATE clause") {
    var query = "PROFILE CREATE (x)"
    for (i <- 1 to BIG_N) {
      query += s" ,(a$i)-[:R]->(b$i)"
    }

    val futureResult = Future(executeWith(BIG_CREATE_CONFIGS, query, executeExpectedFailures = false))
    val result = Await.result(futureResult, BIG_TIMEOUT)
    assertStats(result, nodesCreated = BIG_N * 2 + 1, relationshipsCreated = BIG_N)

    val planDescription = Await.result(Future(result.executionPlanDescription()), BIG_TIMEOUT)
    val creates = planDescription.find("Create")
    creates.size should equal(1)
    val expectedTotalDbHits: Long = BIG_N * 3
    creates.head.totalDbHits.get should be > expectedTotalDbHits
  }

  test("PROFILE many CREATE clauses") {
    var query = "PROFILE CREATE (x)"
    for (i <- 1 to BIG_N) {
      query += s" CREATE (a$i)-[:R]->(b$i)"
    }

    val futureResult = Future(executeWith(BIG_CREATE_CONFIGS, query, executeExpectedFailures = false))
    val result = Await.result(futureResult, BIG_TIMEOUT)
    assertStats(result, nodesCreated = BIG_N * 2 + 1, relationshipsCreated = BIG_N)

    val planDescription = Await.result(Future(result.executionPlanDescription()), BIG_TIMEOUT)
    val creates = planDescription.find("Create")
    creates.size should equal(1)
    val expectedTotalDbHits: Long = BIG_N * 3
    creates.head.totalDbHits.get should be > expectedTotalDbHits
  }

  test("handle null value in property map from parameter for create node") {
    val query = "CREATE (a {props}) RETURN a.foo, a.bar"

    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Version2_3, query, params = Map("props" -> Map("foo" -> null, "bar" -> "baz")))

    result.toSet should equal(Set(Map("a.foo" -> null, "a.bar" -> "baz")))
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
  }

  test("handle null value in property map from parameter for create node with SET") {
    createNode(("foo", 42), ("bar", "fu"))
    val query = "MATCH (a) SET a = {props} RETURN a.foo, a.bar"

    val result = executeWith(Configs.UpdateConf, query, params = Map("props" -> Map("foo" -> null, "bar" -> "baz")))

    result.toSet should equal(Set(Map("a.foo" -> null, "a.bar" -> "baz")))
    assertStats(result, propertiesWritten = 2)
  }

  test("handle null value in property map from parameter for create relationship") {
    val query = "CREATE (a)-[r:REL {props}]->() RETURN r.foo, r.bar"

    val result = executeWith(Configs.UpdateConf, query, params = Map("props" -> Map("foo" -> null, "bar" -> "baz")))

    result.toSet should equal(Set(Map("r.foo" -> null, "r.bar" -> "baz")))
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 1)
  }

  test("handle null value in property map from parameter") {
    val query = "CREATE (a {props})-[r:REL {props}]->() RETURN a.foo, a.bar, r.foo, r.bar"

    val result = executeWith(Configs.InterpretedAndSlotted - Configs.Version2_3, query, params = Map("props" -> Map("foo" -> null, "bar" -> "baz")))

    result.toSet should equal(Set(Map("a.foo" -> null, "a.bar" -> "baz", "r.foo" -> null, "r.bar" -> "baz")))
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 2)
  }

  //Not TCK material
  test("should have bound node recognized after projection with WITH + LOAD CSV") {
    val url = createCSVTempFileURL(writer => writer.println("Foo"))

    val query = s"CREATE (a) WITH a LOAD CSV FROM '$url' AS line CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = executeWith(Configs.UpdateConf, query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  //Not TCK material
  test("should have bound node recognized after projection with WITH + CALL") {
    val query = "CREATE (a:L) WITH a CALL db.labels() YIELD label CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = executeWith(Configs.UpdateConf - Configs.RulePlanner, query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, labelsAdded = 1)
  }

  //Not TCK material
  test("should have bound node recognized after projection with WITH + FOREACH") {
    val query = "CREATE (a) WITH a FOREACH (i in [] | SET a.prop = 1) CREATE (b) CREATE (a)<-[:T]-(b)"

    val result = executeWith(Configs.UpdateConf, query)

    assertStats(result, nodesCreated = 2, relationshipsCreated = 1)
  }

  //Not TCK material
  test("should handle pathological create query") {

    val amount = 200

    val query = "CREATE" + List.fill(amount)("(:Bar)-[:FOO]->(:Baz)").mkString(", ")

    val result = executeWith(Configs.UpdateConf, query)

    assertStats(result, nodesCreated = 2 * amount, relationshipsCreated = amount, labelsAdded = 2 * amount)

    // Should not get StackOverflowException
    result.executionPlanDescription()
  }

  test("should create nodes with label and property with slotted runtime") {
    //TODO: Remove this test once we can create relationships in slotted runtime
    val createdNumber = 1

    val query = "CREATE" + List.fill(createdNumber)("(:Bar{prop: 1})").mkString(", ")

    val result = executeWith(Configs.UpdateConf, query)

    assertStats(result, nodesCreated = createdNumber, labelsAdded = createdNumber, propertiesWritten = createdNumber)

    // Should not get StackOverflowException
    result.executionPlanDescription()
  }

  //Not TCK material
  // This test exposed a bug in the slotted runtime where it could mix up long slots with ref slots
  test("should not accidentally create relationship between the wrong nodes") {
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")

    val query =
      """
        |MATCH (a:A), (b:B)
        |WITH a, b as x
        |CREATE (x)-[r:T]->(x)
        |WITH r
        |MATCH (:B)-[:T]->(:B)
        |RETURN count(*) as c
      """.stripMargin

    val result = graph.execute(query)

    assert(result.hasNext)
    result.next.get("c") shouldEqual(1)
  }

  val MISSING_NODE_ERRORS = List("Failed to create relationship `r`, node `c` is missing. If you prefer to simply ignore rows " +
                                 "where a relationship node is missing, set 'cypher.lenient_create_relationship = true' in neo4j.conf",
                                 "Expected to find a node, but found instead: null",
                                 "Expected to find a node at c but found nothing Some(null)",
                                 "Other node is null")

  // No CLG decision on this AFAIK, so not TCK material
  test("should throw on CREATE relationship if start-point is missing") {
    graph.execute("CREATE (a), (b)")

    val config = Configs.All - Configs.Compiled - Configs.Cost2_3

    failWithError(config, """MATCH (a), (b)
                            |WHERE id(a)=0 AND id(b)=1
                            |OPTIONAL MATCH (b)-[:LINK_TO]->(c)
                            |CREATE (b)-[:LINK_TO]->(a)
                            |CREATE (c)-[r:MISSING_C]->(a)""".stripMargin,
      errorType = MISSING_NODE_ERRORS)
  }

  // No CLG decision on this AFAIK, so not TCK material
  test("should throw on CREATE relationship if end-point is missing") {
    graph.execute("CREATE (a), (b)")

    val config = Configs.All - Configs.Compiled - Configs.Cost2_3

    failWithError(config, """MATCH (a), (b)
                            |WHERE id(a)=0 AND id(b)=1
                            |OPTIONAL MATCH (b)-[:LINK_TO]->(c)
                            |CREATE (b)-[:LINK_TO]->(a)
                            |CREATE (a)-[r:MISSING_C]->(c)""".stripMargin,
      errorType = MISSING_NODE_ERRORS)
  }
}
