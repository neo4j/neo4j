/**
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

class AggregationAcceptanceTest extends ExecutionEngineFunSuite {
  test("should handle aggregates inside non aggregate expressions") {
    execute(
      "MATCH (a { name: 'Andres' })<-[:FATHER]-(child) RETURN {foo:a.name='Andres',kids:collect(child.name)}"
    ).toList
  }

  test ("should be able to count nodes") {
    val a = createNode()
    val b1 = createNode() //start a = (0) match (a) --> (b) return a, count(*)
    val b2 = createNode()
    relate(a, b1, "A")
    relate(a, b2, "A")

    val result = execute(
      s"start a=node(${a.getId}) match (a)-[rel]->(b) return a, count(*)"
    )

    result.toList should equal(List(Map("a" -> a, "count(*)" -> 2)))
  }

  test("should sort on aggregated function and normal property") {
    val n1 = createNode(Map("name" -> "andres", "division" -> "Sweden"))
    val n2 = createNode(Map("name" -> "michael", "division" -> "Germany"))
    val n3 = createNode(Map("name" -> "jim", "division" -> "England"))
    val n4 = createNode(Map("name" -> "mattias", "division" -> "Sweden"))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}, ${n3.getId}, ${n4.getId})" +
        """return n.division, count(*)
        order by count(*) DESC, n.division ASC
        """
    )
    result.toList should equal(List(
      Map("n.division" -> "Sweden", "count(*)" -> 2),
      Map("n.division" -> "England", "count(*)" -> 1),
      Map("n.division" -> "Germany", "count(*)" -> 1)))
  }

  test("should aggregate on properties") {
    val n1 = createNode(Map("x" -> 33))
    val n2 = createNode(Map("x" -> 33))
    val n3 = createNode(Map("x" -> 42))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}, ${n3.getId}) return n.x, count(*)"
    )

    result.toList should equal(List(Map("n.x" -> 42, "count(*)" -> 1), Map("n.x" -> 33, "count(*)" -> 2)))
  }

  test("should count non null values") {
    createNode(Map("y" -> "a", "x" -> 33))
    createNode(Map("y" -> "a"))
    createNode(Map("y" -> "b", "x" -> 42))

    val result = execute("start n=node(0,1,2) return n.y, count(n.x)")

    result.toSet should equal(Set(Map("n.y" -> "a", "count(n.x)" -> 1), Map("n.y" -> "b", "count(n.x)" -> 1)))
  }

  test("should sum non null values") {
    createNode(Map("y" -> "a", "x" -> 33))
    createNode(Map("y" -> "a"))
    createNode(Map("y" -> "a", "x" -> 42))

    val result = execute("start n = node(0,1,2) return n.y, sum(n.x)")

    result.toList should contain(Map("n.y" -> "a", "sum(n.x)" -> 75))
  }

  test("should handle aggregation on functions") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    relate(a, b, "X")
    relate(a, c, "X")

    val result = execute( """
start a  = node(0)
match p = a -[*]-> b
return b, avg(length(p))""")

    result.columnAs[Node]("b").toSet should equal (Set(b, c))
  }

  test("should be able to do distinct on unbound node") {
    createNode()

    val result = execute("start a=node(0) optional match a-->b return count(distinct b)")
    result.toList should equal (List(Map("count(distinct b)" -> 0)))
  }

  test("shouldBeAbleToDoDistinctOnNull") {
    createNode()

    val result = execute("start a=node(0) return count(distinct a.foo)")
    result.toList should equal (List(Map("count(distinct a.foo)" -> 0)))
  }

  test("should aggregate on array values") {
    createNode("color" -> Array("red"))
    createNode("color" -> Array("blue"))
    createNode("color" -> Array("red"))

    val result = execute("start a=node(0,1,2) return distinct a.color, count(*)").toList
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

    intercept[SyntaxException](execute("start a=node(0) return count(count(*))").toList)
  }

  test("aggregates should be possible to use with arithmetics") {
    createNode()

    val result = execute("start a=node(0) return count(*) * 10").toList
    result should equal (List(Map("count(*) * 10" -> 10)))
  }

  test("aggregates should be possible to order by arithmetics") {
    createNode()
    createNode()
    createNode()

    val result = execute("start a=node(0),b=node(1,2) return count(a) * 10 + count(b) * 5 as X order by X").toList
    result should equal (List(Map("X" -> 30)))
  }

  test("should handle multiple aggregates on the same node") {
    //WHEN
    val a = createNode()
    val result = execute("start n=node(*) return count(n), collect(n)")

    //THEN
    result.toList should equal (List(Map("count(n)" -> 1, "collect(n)" -> Seq(a))))
  }

}
