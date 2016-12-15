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
import org.neo4j.graphdb.Node

class SetAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("should be able to set property to collection") {
    // given
    val node = createNode()

    // when
    val result = updateWithBothPlanners("MATCH (n) SET n.property = ['foo','bar'] RETURN n.property")

    // then
    assertStats(result, propertiesWritten = 1)
    node should haveProperty("property")

    // and
    val result2 = executeWithAllPlannersAndCompatibilityMode("MATCH (n) WHERE n.property = ['foo','bar'] RETURN count(*)")
    result2.columnAs("count(*)").toList should be(List(1))
  }

  test("should not be able to set property to collection of collections") {
    // given
    createNode()

    // when
    val result = intercept[CypherTypeException](
      updateWithBothPlanners("MATCH (n) SET n.property = [['foo'],['bar']] RETURN n.property")
    )

    //then
    result.toString should equal("org.neo4j.cypher.CypherTypeException: Collections containing collections can not be stored in properties.")
  }

  test("should not be able to set property to collection with null value") {
    // given
    createNode()

    // when
    val result = intercept[CypherTypeException](
      updateWithBothPlanners("MATCH (n) SET n.property = [null,null] RETURN n.property")
    )

    //then
    result.toString should equal("org.neo4j.cypher.CypherTypeException: Collections containing null values can not be stored in properties.")
  }

  test("set node property to null will remove existing property") {
    // given
    val node = createNode("property" -> 12)

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) SET n.property = null RETURN count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    node should not(haveProperty("property"))
  }

  test("set relationship property to null will remove existing property") {
    // given
    val relationship = relate(createNode(), createNode(), "property" -> 12)

    // when
    val result = updateWithBothPlanners("MATCH ()-[r]->() SET r.property = null RETURN count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    relationship should not(haveProperty("property"))
  }

  test("set a property") {
    // given
    val a = createNode("name" -> "Andres")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("match (n) where n.name = 'Andres' set n.name = 'Michael'")

    // then
    assertStats(result, propertiesWritten = 1)
    a should haveProperty("name").withValue("Michael")
  }

  test("set a property to an expression") {
    // given
    val a = createNode("name" -> "Andres")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("match (n) where n.name = 'Andres' set n.name = n.name + ' was here' return count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    a should haveProperty("name").withValue("Andres was here")
  }

  test("set a property by picking the node trough a simple expression") {
    // given
    val a = createNode()

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("match (n) set (n).name = 'neo4j' return count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    a should haveProperty("name").withValue("neo4j")
  }

  test("set a property by picking the node trough an expression") {
    // given
    val a = createNode()

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("match (n) set (CASE WHEN true THEN n END).name = 'neo4j' return count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    a should haveProperty("name").withValue("neo4j")
  }

  test("set a property by picking the relationship trough a simple expression") {
    // given
    val r = relate(createNode(), createNode())

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("match ()-[r]->() set (r).name = 'neo4j' return count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    r should haveProperty("name").withValue("neo4j")
  }

  test("set a property by picking the relationship trough an expression") {
    // given
    val r = relate(createNode(), createNode())

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("match ()-[r]->() set (CASE WHEN true THEN r END).name = 'neo4j' return count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    r should haveProperty("name").withValue("neo4j")
  }

  test("should set properties on nodes with foreach and indexes") {
    val n1 = createNode()
    val n2 = createNode()
    val n3 = createNode()

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n) WITH collect(n) as nodes, {param} as data FOREACH (idx IN range(0,size(nodes)-1) | SET (nodes[idx]).num = data[idx])", "param" ->  Array("1", "2", "3"))

    assertStats(result, propertiesWritten = 3)
    n1 should haveProperty("num").withValue("1")
    n2 should haveProperty("num").withValue("2")
    n3 should haveProperty("num").withValue("3")
  }

  test("should set properties on relationships with foreach and indexes") {
    val r1 = relate(createNode(), createNode())
    val r2 = relate(createNode(), createNode())
    val r3 = relate(createNode(), createNode())

    val result = updateWithBothPlannersAndCompatibilityMode("MATCH ()-[r]->() WITH collect(r) as rels, {param} as data FOREACH (idx IN range(0,size(rels)-1) | SET (rels[idx]).num = data[idx])", "param" ->  Array("1", "2", "3"))

    assertStats(result, propertiesWritten = 3)
    r1 should haveProperty("num").withValue("1")
    r2 should haveProperty("num").withValue("2")
    r3 should haveProperty("num").withValue("3")
  }

  test("should fail at runtime when the expression is not a node or a relationship") {
    an [InvalidArgumentException] should be thrownBy
      updateWithBothPlanners("set (CASE WHEN true THEN {node} END).name = 'neo4j' return count(*)", "node" -> 42)
  }

  test("set property for null removes the property") {
    // given
    val n = createNode("name" -> "Michael", "age" -> 35)

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("match (n) where n.name = 'Michael' set n.name = null return n.age")

    // then
    assertStats(result, propertiesWritten = 1)
    n should not(haveProperty("name"))
  }

  test("mark nodes in path") {
    // given
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b)
    relate(b, c)

    // when
    val q = "MATCH p=(a)-->(b)-->(c) WHERE id(a) = 0 AND id(c) = 2 WITH p FOREACH(n in nodes(p) | SET n.marked = true)"

    updateWithBothPlanners(q)

    // then
    a should haveProperty("marked").withValue(true)
    b should haveProperty("marked").withValue(true)
    c should haveProperty("marked").withValue(true)
  }

  test("should be able to add label to node") {
    // given
    createNode()

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("match (n) where id(n) = 0 set n:FOO return n")

    // then
    val createdNode = result.columnAs[Node]("n").next()
    createdNode should haveLabels("FOO")
    assertStats(result, labelsAdded = 1)
  }

  test("extract on arrays") {
    // given
    createNode()

    // when
    val result = updateWithBothPlannersAndCompatibilityMode( "match (n) where id(n) = 0 set n.x=[1,2,3] return extract (i in n.x | i/2.0) as x")

    // then
    result.toList should equal(List(Map("x" -> List(0.5, 1.0, 1.5))))
  }

  test("concatenate to a collection") {
    // given

    // when
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Array[Long]]("create (a {foo:[1,2,3]}) set a.foo = a.foo + [4,5] return a.foo")

    // then
    result.toList should equal(List(1, 2, 3, 4, 5))
  }

  test("concatenate to a collection in reverse") {
    // given

    // when
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Array[Long]]("create (a {foo:[3,4,5]}) set a.foo = [1,2] + a.foo return a.foo")

    // then
    result.toList should equal(List(1, 2, 3, 4, 5))
  }

  test("overwrites values when using +=") {
    // given
    val a = createNode("foo"->"A", "bar"->"B")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n {foo:'A'}) SET n += {bar:'C'} RETURN count(*)")

    // then
    a should haveProperty("foo").withValue("A")
    a should haveProperty("bar").withValue("C")
  }

  test("old values are kept when using +=") {
    // given
    val a = createNode("foo"->"A")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n {foo:'A'}) SET n += {bar:'B'} RETURN count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    a should haveProperty("foo").withValue("A")
    a should haveProperty("bar").withValue("B")
  }

  test("explicit null values in map removes old values") {
    // given
    val a = createNode("foo"->"A", "bar"->"B")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n {foo:'A'}) SET n += {foo:null} RETURN count(*)")

    // then
    assertStats(result, propertiesWritten = 1)
    a should not(haveProperty("foo"))
    a should haveProperty("bar").withValue("B")
  }

  test("set += works well inside foreach") {
    // given
    val a = createNode("a"->"A")
    val b = createNode("b"->"B")
    val c = createNode("c"->"C")

    // when
    val result = updateWithBothPlanners("MATCH (n) WITH collect(n) as nodes FOREACH(x IN nodes | SET x += {x:'X'})")

    // then
    a should haveProperty("a").withValue("A")
    b should haveProperty("b").withValue("B")
    c should haveProperty("c").withValue("C")
    a should haveProperty("x").withValue("X")
    b should haveProperty("x").withValue("X")
    c should haveProperty("x").withValue("X")
  }

  test("non-existing values in an exact property map are removed with set =") {
    // given
    val a = createNode("foo"->"A", "bar"->"B")

    // when
    val result = updateWithBothPlannersAndCompatibilityMode("MATCH (n {foo:'A'}) SET n = {foo:'B', baz:'C'} RETURN count(*)")

    assertStats(result, propertiesWritten = 3)
    // then
    a should not(haveProperty("bar"))
    a should haveProperty("foo").withValue("B")
    a should haveProperty("baz").withValue("C")
  }

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

  test("Lost updates should not happen on set node property") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) SET n.prop = n.prop + 1"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

  test("Lost updates should not happen on set node property with an entangled expression") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) SET n.prop = 2 + (10 * n.prop) / 10 - 1"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

  test("Lost updates should not happen for set node property with map") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) SET n = {prop: n.prop + 1}"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

  test("Lost updates should not happen on set relationship property") {
    val init: () => Unit = () => relate(createNode(), createNode(), "prop" -> 0)
    val query = "MATCH ()-[r]->() SET r.prop = r.prop + 1"
    val resultQuery = "MATCH ()-[r]->() RETURN r.prop"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

  test("Lost updates should not happen on set relationship property with an entangled expression") {
    val init: () => Unit = () => relate(createNode(), createNode(), "prop" -> 0)
    val query = "MATCH ()-[r]->() SET r.prop = 2 + (10 * r.prop) / 10 - 1"
    val resultQuery = "MATCH ()-[r]->() RETURN r.prop"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

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
  ignore("Lost updates should not happen on set node property with the read in a preceding statement") {
    val init: () => Unit = () => createNode("prop" -> 0)
    val query = "MATCH (n) WITH n.prop as p SET n.prop = p + 1"
    val resultQuery = "MATCH (n) RETURN n.prop"
    testLostUpdatesWithBothPlanners(init, query, resultQuery, 10, 10)
  }

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
