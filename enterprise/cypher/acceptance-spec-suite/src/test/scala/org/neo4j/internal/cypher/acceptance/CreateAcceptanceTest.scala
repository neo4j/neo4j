/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.{Configs, TestConfiguration}

class CreateAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport
  with CreateTempFileTestSupport {

  test("handle null value in property map from parameter for create node") {
    val query = "CREATE (a {props}) RETURN a.foo, a.bar"

    val result = executeWith(Configs.Interpreted - Configs.Version2_3, query, params = Map("props" -> Map("foo" -> null, "bar" -> "baz")))

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

    val result = executeWith(Configs.Interpreted - Configs.Version2_3, query, params = Map("props" -> Map("foo" -> null, "bar" -> "baz")))

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

    val result = executeWith(Configs.UpdateConf - Configs.AllRulePlanners, query)

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

  test("should allow create, delete and return in one go (relationship)") {
    val typ = "ThisIsTheRelationshipType"
    val query = s"CREATE ()-[r:$typ]->() DELETE r RETURN type(r)"
    val result = executeWith(Configs.UpdateConf, query)
    result.toList should equal(List(Map("type(r)" -> typ)))
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

  // No CLG decision on this AFAIK, so not TCK material
  test("should throw on CREATE relationship if start-point is missing") {
    graph.execute("CREATE (a), (b)")

    val config = Configs.AbsolutelyAll - Configs.Compiled - Configs.OldAndRule

    failWithError(config, """MATCH (a), (b)
                            |WHERE id(a)=0 AND id(b)=1
                            |OPTIONAL MATCH (b)-[:LINK_TO]->(c)
                            |CREATE (b)-[:LINK_TO]->(a)
                            |CREATE (c)-[r:MISSING_C]->(a)""".stripMargin,
      errorType = List(s"Failed to create relationship `r`, node `c` is missing. If you prefer to simply ignore rows " +
                       s"where a relationship node is missing, set 'cypher.lenient_create_relationship = true' in neo4j.conf"))
  }

  // No CLG decision on this AFAIK, so not TCK material
  test("should throw on CREATE relationship if end-point is missing") {
    graph.execute("CREATE (a), (b)")

    val config = Configs.AbsolutelyAll - Configs.Compiled - Configs.OldAndRule

    failWithError(config, """MATCH (a), (b)
                            |WHERE id(a)=0 AND id(b)=1
                            |OPTIONAL MATCH (b)-[:LINK_TO]->(c)
                            |CREATE (b)-[:LINK_TO]->(a)
                            |CREATE (a)-[r:MISSING_C]->(c)""".stripMargin,
      errorType = List(s"Failed to create relationship `r`, node `c` is missing. If you prefer to simply ignore rows " +
        s"where a relationship node is missing, set 'cypher.lenient_create_relationship = true' in neo4j.conf"))
  }
}
