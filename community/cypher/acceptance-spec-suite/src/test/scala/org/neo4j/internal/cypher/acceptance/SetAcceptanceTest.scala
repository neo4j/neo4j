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

import org.neo4j.cypher.{ExecutionEngineFunSuite, InvalidArgumentException, NewPlannerTestSupport, QueryStatisticsTestSupport}

class SetAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  //Not suitable for the TCK
  test("set a property by selecting the node through an expression") {
    // given
    val a = createNode()

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) SET (CASE WHEN true THEN n END).name = 'neo4j' RETURN count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    a should haveProperty("name").withValue("neo4j")
  }

  //Not suitable for the TCK
  test("set a property by selecting the relationship through an expression") {
    // given
    val r = relate(createNode(), createNode())

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH ()-[r]->() SET (CASE WHEN true THEN r END).name = 'neo4j' RETURN count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    r should haveProperty("name").withValue("neo4j")
  }

  //Not suitable for the TCK
  test("should set properties on nodes with foreach and indexes") {
    val n1 = createNode()
    val n2 = createNode()
    val n3 = createNode()

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) WITH collect(n) AS nodes, {param} AS data FOREACH (idx IN range(0,size(nodes)-1) | SET (nodes[idx]).num = data[idx])", "param" ->  Array("1", "2", "3"))

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

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH ()-[r]->() WITH collect(r) AS rels, {param} as data FOREACH (idx IN range(0,size(rels)-1) | SET (rels[idx]).num = data[idx])", "param" ->  Array("1", "2", "3"))

    assertStats(result, propertiesWritten = 3)
    r1 should haveProperty("num").withValue("1")
    r2 should haveProperty("num").withValue("2")
    r3 should haveProperty("num").withValue("3")
  }

  //Not suitable for the TCK
  test("should fail at runtime when the expression is not a node or a relationship") {
    an [InvalidArgumentException] should be thrownBy
      updateWithBothPlanners("SET (CASE WHEN true THEN {node} END).name = 'neo4j' RETURN count(*)", "node" -> 42)
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

    updateWithBothPlanners(q)

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
    val result = updateWithBothPlanners("MATCH (n) WITH collect(n) AS nodes FOREACH(x IN nodes | SET x += {x:'X'})")

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
    updateWithBothPlanners("MATCH (n) WITH collect(n) as nodes FOREACH(x IN nodes | SET x = {a:'D', x:'X'})")

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
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen on set node property with an entangled expression") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) SET n.prop = 2 + (10 * n.prop) / 10 - 1"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen for set node property with map") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) SET n = {prop: n.prop + 1}"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen on set relationship property") {
    val init: () => Unit = () => relate(createNode(), createNode(), "prop" -> 0)
    val query = "MATCH ()-[r]->() SET r.prop = r.prop + 1"
    val resultQuery = "MATCH ()-[r]->() RETURN r.prop"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen on set relationship property with an entangled expression") {
    val init: () => Unit = () => relate(createNode(), createNode(), "prop" -> 0)
    val query = "MATCH ()-[r]->() SET r.prop = 2 + (10 * r.prop) / 10 - 1"
    val resultQuery = "MATCH ()-[r]->() RETURN r.prop"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  test("Lost updates should not happen for set relationship property with map") {
    val init: () => Unit = () => relate(createNode(), createNode(), "prop" -> 0)
    val query = "MATCH ()-[r]->() SET r = {prop: r.prop + 1}"
    val resultQuery = "MATCH ()-[r]->() RETURN r.prop"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
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
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

  //Not suitable for the TCK
  ignore("lost updates should not happen on set node properties from map with circular dependencies") {
    val init: () => Unit = () => createNode("prop" -> 0, "prop2" -> 0)
    val query = "match (n) set n += { prop: n.prop2 + 1, prop2: n.prop + 1 }"
    val resultQuery = "MATCH (n) RETURN n.prop + n.prop2"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 20)
  }

  private def testLostUpdatesWithBothPlanners(init: () => Unit,
                                              query: String,
                                              resultQuery: String,
                                              updates: Int,
                                              resultValue: Int) = {
    Seq("rule", "cost").foreach { planner =>
      init()
      val queryWithPlanner = s"CYPHER planner=$planner $query"
      val threads = (0 until updates).map { i =>
        new Thread(new Runnable {
          override def run(): Unit = {
            eengine.execute(queryWithPlanner, Map.empty[String, Any], graph.session())
          }
        })
      }
      threads.foreach(_.start())
      threads.foreach(_.join())

      val result = executeScalar[Long](resultQuery)
      assert(result == resultValue, s": we lost updates with $planner planner!")

      // Reset for run on next planner
      eengine.execute("MATCH (n) DETACH DELETE n", Map.empty[String, Any], graph.session())
    }
  }
}
