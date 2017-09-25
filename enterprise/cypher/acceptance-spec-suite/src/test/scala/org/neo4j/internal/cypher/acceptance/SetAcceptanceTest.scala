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
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class SetAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  val expectedToSucceed = Configs.CommunityInterpreted - Configs.Cost2_3
  val expectedToFail = Configs.CommunityInterpreted - Configs.Cost2_3 + TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.ProcedureOrSchema))
  val expectedToFail2 = Configs.CommunityInterpreted - Configs.Version2_3 + TestConfiguration(Versions.Default, Planners.Default, Runtimes(Runtimes.ProcedureOrSchema))

  test("optional match and set") {
    val n1 = createLabeledNode("L1")
    val n2 = createLabeledNode("L2")
    relate(n1, n2, "R1")

    // only fails when returning distinct...
    val result = executeWith(expectedToSucceed,
      """
        |MATCH (n1:L1)-[:R1]->(n2:L2)
        |OPTIONAL MATCH (n3)<-[r:R2]-(n2)
        |SET n3.prop = false
        |RETURN distinct n2
      """.stripMargin)

    result.toList should be(List(Map("n2" -> n2)))
  }

  test("should be able to set property to collection") {
    // given
    val node = createNode()

    // when
    val result = executeWith(expectedToSucceed, "MATCH (n) SET n.property = ['foo','bar'] RETURN n.property")

    // then
    assertStats(result, propertiesWritten = 1)
    node should haveProperty("property")

    // and
    val result2 = executeWith(Configs.Interpreted, "MATCH (n) WHERE n.property = ['foo','bar'] RETURN count(*)")
    result2.columnAs("count(*)").toList should be(List(1))
  }

  test("should not be able to set property to collection of collections") {
    // given
    createNode()

    // when
    failWithError(expectedToFail, "MATCH (n) SET n.property = [['foo'],['bar']] RETURN n.property",
      List("Collections containing collections can not be stored in properties."))
  }

  test("should not be able to set property to collection with null value") {
    // given
    createNode()

    // when
    failWithError(expectedToFail, "MATCH (n) SET n.property = [null,null] RETURN n.property",
      List("Collections containing null values can not be stored in properties."))

  }

  //Not suitable for the TCK
  test("set a property by selecting the node through an expression") {
    // given
    val a = createNode()

    // when
    val result = executeWith(expectedToSucceed, "MATCH (n) SET (CASE WHEN true THEN n END).name = 'neo4j' RETURN count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    a should haveProperty("name").withValue("neo4j")
  }

  //Not suitable for the TCK
  test("set a property by selecting the relationship through an expression") {
    // given
    val r = relate(createNode(), createNode())

    // when
    val result = executeWith(expectedToSucceed, "MATCH ()-[r]->() SET (CASE WHEN true THEN r END).name = 'neo4j' RETURN count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    r should haveProperty("name").withValue("neo4j")
  }

  //Not suitable for the TCK
  test("should set properties on nodes with foreach and indexes") {
    val n1 = createNode()
    val n2 = createNode()
    val n3 = createNode()

    val query = "MATCH (n) WITH collect(n) AS nodes, {param} AS data FOREACH (idx IN range(0,size(nodes)-1) | SET (nodes[idx]).num = data[idx])"
    val result = executeWith(expectedToSucceed, query, params = Map("param" ->  Array("1", "2", "3")))

    assertStats(result, propertiesWritten = 3)
    n1 should haveProperty("num").withValue("1")
    n2 should haveProperty("num").withValue("2")
    n3 should haveProperty("num").withValue("3")
  }

  //Not suitable for the TCK
  test("should set properties on relationships with foreach and indexes") {
    val r1 = relate(createNode(), createNode())
    val r2 = relate(createNode(), createNode())
    val r3 = relate(createNode(), createNode())

    val query = "MATCH ()-[r]->() WITH collect(r) AS rels, {param} as data FOREACH (idx IN range(0,size(rels)-1) | SET (rels[idx]).num = data[idx])"
    val result = executeWith(expectedToSucceed, query, params = Map("param" ->  Array("1", "2", "3")))

    assertStats(result, propertiesWritten = 3)
    r1 should haveProperty("num").withValue("1")
    r2 should haveProperty("num").withValue("2")
    r3 should haveProperty("num").withValue("3")
  }

  //Not suitable for the TCK
  test("should fail at runtime when the expression is not a node or a relationship") {
    failWithError(expectedToFail2, "SET (CASE WHEN true THEN {node} END).name = 'neo4j' RETURN count(*)",
      List("The expression GenericCase(Vector((true,{node})),None) should have been a node or a relationship"), params= "node" -> 42)
  }

  //Not suitable for the TCK
  test("mark nodes in path") {
    // given
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b)
    relate(b, c)

    // when
    val q = "MATCH p=(a)-->(b)-->(c) WHERE id(a) = 0 AND id(c) = 2 WITH p FOREACH(n IN nodes(p) | SET n.marked = true)"

    executeWith(expectedToSucceed, q)

    // then
    a should haveProperty("marked").withValue(true)
    b should haveProperty("marked").withValue(true)
    c should haveProperty("marked").withValue(true)
  }

  //Not suitable for the TCK
  test("set += works well inside foreach") {
    // given
    val a = createNode("a"->"A")
    val b = createNode("b"->"B")
    val c = createNode("c"->"C")

    // when
    val result = executeWith(expectedToSucceed, "MATCH (n) WITH collect(n) AS nodes FOREACH(x IN nodes | SET x += {x:'X'})")

    // then
    a should haveProperty("a").withValue("A")
    b should haveProperty("b").withValue("B")
    c should haveProperty("c").withValue("C")
    a should haveProperty("x").withValue("X")
    b should haveProperty("x").withValue("X")
    c should haveProperty("x").withValue("X")
  }

  //Not suitable for the TCK
  test("set = works well inside foreach") {
    // given
    val a = createNode("a"->"A")
    val b = createNode("b"->"B")
    val c = createNode("c"->"C")

    // when
   executeWith(expectedToSucceed, "MATCH (n) WITH collect(n) as nodes FOREACH(x IN nodes | SET x = {a:'D', x:'X'})")

    // then
    a should haveProperty("a").withValue("D")
    b should haveProperty("a").withValue("D")
    c should haveProperty("a").withValue("D")
    b should not(haveProperty("b"))
    c should not(haveProperty("c"))
    a should haveProperty("x").withValue("X")
    b should haveProperty("x").withValue("X")
    c should haveProperty("x").withValue("X")
  }

  //Not suitable for the TCK
  test("Lost updates should not happen on set node property") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) SET n.prop = n.prop + 1"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen on set node property with an entangled expression") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) SET n.prop = 2 + (10 * n.prop) / 10 - 1"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen for set node property with map") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) SET n = {prop: n.prop + 1}"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen on set relationship property") {
    val init: () => Unit = () => relate(createNode(), createNode(), "prop" -> 0)
    val query = "MATCH ()-[r]->() SET r.prop = r.prop + 1"
    val resultQuery = "MATCH ()-[r]->() RETURN r.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen on set relationship property with an entangled expression") {
    val init: () => Unit = () => relate(createNode(), createNode(), "prop" -> 0)
    val query = "MATCH ()-[r]->() SET r.prop = 2 + (10 * r.prop) / 10 - 1"
    val resultQuery = "MATCH ()-[r]->() RETURN r.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen for set relationship property with map") {
    val init: () => Unit = () => relate(createNode(), createNode(), "prop" -> 0)
    val query = "MATCH ()-[r]->() SET r = {prop: r.prop + 1}"
    val resultQuery = "MATCH ()-[r]->() RETURN r.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  // Lost updates are still an issue, and it's hard to identify some cases.
  // We try to document some typical cases below

  // We do not support this because, even though the test is just a simple case,
  // in general we would have to solve a complex data flow equation in order
  // to solve this without being too conservative and do unnecessary exclusive locking
  // (which could be really bad for concurrency in bad cases)
  //Not suitable for the TCK
  ignore("Lost updates should not happen on set node property with the read in a preceding statement") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) WITH n.prop as p SET n.prop = p + 1"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdates(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  ignore("lost updates should not happen on set node properties from map with circular dependencies") {
    val init: () => Unit = () => createNode("prop" -> 0, "prop2" -> 0)
    val query = "match (n) set n += { prop: n.prop2 + 1, prop2: n.prop + 1 }"
    val resultQuery = "MATCH (n) RETURN n.prop + n.prop2"
    testLostUpdates(init, query, resultQuery, 10, 20)
  }

  private def testLostUpdates(init: () => Unit,
                              query: String,
                              resultQuery: String,
                              updates: Int,
                              resultValue: Int) = {
    init()
    val threads = (0 until updates).map { i =>
      new Thread(new Runnable {
        override def run(): Unit = {
          eengine.execute(s"$query", Map.empty[String, Any])
        }
      })
    }
    threads.foreach(_.start())
    threads.foreach(_.join())

    val result = executeScalar[Long](resultQuery)
    assert(result == resultValue, s": we lost updates!")

    // Reset for run on next planner
    eengine.execute("MATCH (n) DETACH DELETE n", Map.empty[String, Any])
  }
}
