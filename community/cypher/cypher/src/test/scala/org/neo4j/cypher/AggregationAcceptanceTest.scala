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

import org.neo4j.graphdb.Node

class AggregationAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {
  test("should handle aggregates inside non aggregate expressions") {
    executeWithAllPlanners(
      "MATCH (a { name: 'Andres' })<-[:FATHER]-(child) RETURN {foo:a.name='Andres',kids:collect(child.name)}"
    ).toList
  }

  test ("should be able to count nodes") {
    val a = createLabeledNode("Start")
    val b1 = createNode()
    val b2 = createNode()
    relate(a, b1, "A")
    relate(a, b2, "A")

    val result = executeWithAllPlanners(
      s"match (a:Start)-[rel]->(b) return a, count(*)"
    )

    result.toList should equal(List(Map("a" -> a, "count(*)" -> 2)))
  }

  test("should sort on aggregated function and normal property") {
    createNode(Map("name" -> "andres", "division" -> "Sweden"))
    createNode(Map("name" -> "michael", "division" -> "Germany"))
    createNode(Map("name" -> "jim", "division" -> "England"))
    createNode(Map("name" -> "mattias", "division" -> "Sweden"))

    val result = executeWithAllPlanners(
      """match n
        |return n.division, count(*)
        |order by count(*) DESC, n.division ASC""".stripMargin
    )
    result.toList should equal(List(
      Map("n.division" -> "Sweden", "count(*)" -> 2),
      Map("n.division" -> "England", "count(*)" -> 1),
      Map("n.division" -> "Germany", "count(*)" -> 1)))
  }

  test("should aggregate on properties") {
    createNode(Map("x" -> 33))
    createNode(Map("x" -> 33))
    createNode(Map("x" -> 42))

    val result = executeWithAllPlanners("match n return n.x, count(*)")

    result.toList should equal(List(Map("n.x" -> 42, "count(*)" -> 1), Map("n.x" -> 33, "count(*)" -> 2)))
  }

  test("should count non null values") {
    createNode(Map("y" -> "a", "x" -> 33))
    createNode(Map("y" -> "a"))
    createNode(Map("y" -> "b", "x" -> 42))

    val result = executeWithAllPlanners("match n return n.y, count(n.x)")

    result.toSet should equal(Set(Map("n.y" -> "a", "count(n.x)" -> 1), Map("n.y" -> "b", "count(n.x)" -> 1)))
  }

  test("should sum non null values") {
    createNode(Map("y" -> "a", "x" -> 33))
    createNode(Map("y" -> "a"))
    createNode(Map("y" -> "a", "x" -> 42))

    val result = executeWithAllPlanners("match n return n.y, sum(n.x)")

    result.toList should contain(Map("n.y" -> "a", "sum(n.x)" -> 75))
  }

  test("should handle aggregation on functions") {
    val a = createLabeledNode("Start")
    val b = createNode()
    val c = createNode()
    relate(a, b)
    relate(a, c)

    val result = executeWithAllPlanners(
      """match p = (a:Start)-[*]-> (b)
        |return b, avg(length(p))""".stripMargin)

    result.columnAs[Node]("b").toSet should equal (Set(b, c))
  }

  test("should be able to do distinct on unbound node") {
    val result = executeWithAllPlanners("optional match a return count(distinct a)")
    result.toList should equal (List(Map("count(distinct a)" -> 0)))
  }

  test("shouldBeAbleToDoDistinctOnNull") {
    createNode()

    val result = executeWithAllPlanners("match a return count(distinct a.foo)")
    result.toList should equal (List(Map("count(distinct a.foo)" -> 0)))
  }

  test("should aggregate on array values") {
    createNode("color" -> Array("red"))
    createNode("color" -> Array("blue"))
    createNode("color" -> Array("red"))

    val result = executeWithAllPlanners("match a return distinct a.color, count(*)").toList
    result.foreach { x =>
      val c = x("a.color").asInstanceOf[Array[_]]

      c.toList match {
        case List("red")  => x("count(*)") should equal (2)
        case List("blue") => x("count(*)") should equal (1)
        case _            => fail("wut?")
      }
    }
  }

  test("aggregates in aggregates should fail") {
    createNode()

    intercept[SyntaxException](executeWithAllPlanners("match a return count(count(*))").toList)
  }

  test("aggregates should be possible to use with arithmetics") {
    createNode()

    val result = executeWithAllPlanners("match a return count(*) * 10").toList
    result should equal (List(Map("count(*) * 10" -> 10)))
  }

  test("aggregates should be possible to order by arithmetics") {
    createLabeledNode("A")
    createLabeledNode("X")
    createLabeledNode("X")

    val result = executeWithAllPlanners("match (a:A), (b:X) return count(a) * 10 + count(b) * 5 as X order by X").toList
    result should equal (List(Map("X" -> 30)))
  }

  test("should handle multiple aggregates on the same node") {
    //WHEN
    val a = createNode()
    val result = executeWithAllPlanners("match n return count(n), collect(n)")

    //THEN
    result.toList should equal (List(Map("count(n)" -> 1, "collect(n)" -> Seq(a))))
  }

  test("simple counting of nodes works as expected") {

    graph.inTx {
      (1 to 100).foreach {
        x => createNode()
      }
    }

    //WHEN
    val result = executeWithAllPlanners("match n return count(*)")

    //THEN
    result.toList should equal (List(Map("count(*)" -> 100)))
  }

  test("aggregation around named paths works") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val e = createNode()
    val f = createNode()

    relate(a, b)

    relate(c, d)
    relate(d, e)
    relate(e, f)

    val result = executeWithAllPlanners(
      """match p = a-[*]->b
        |return collect(nodes(p)) as paths, length(p) as l order by length(p)""".stripMargin)

    val expected =
      List(Map("l" -> 1, "paths" -> List(List(a, b), List(c, d), List(d, e), List(e, f))),
           Map("l" -> 2, "paths" -> List(List(c, d, e), List(d, e, f))),
           Map("l" -> 3, "paths" -> List(List(c, d, e, f))))

    result.toList should be (expected)
  }
}
