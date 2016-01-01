/**
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
package org.neo4j.cypher

class WithAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("only passing on pattern nodes work") {
    val a = createNode()
    val b = createNode()
    relate(a, b)

    val result = executeWithNewPlanner(
      "MATCH a WITH a MATCH a-->b RETURN *"
    )
    result.toList should equal(List(Map("a" -> a, "b" -> b)))
  }

  test("order by and limit can be used") {
    val a = createNode("A")
    createNode("B")
    createNode("C")

    relate(a,createNode())

    val result = executeWithNewPlanner(
      "MATCH a WITH a ORDER BY a.name LIMIT 1 MATCH a-->b RETURN a"
    )
    result.toList should equal(List(Map("a" -> a)))
  }

  test("without dependencies between the query parts works") {
    val a = createNode()
    val b = createNode()

    val result = executeWithNewPlanner(
      "MATCH a WITH a MATCH b RETURN *"
    )
    result.toSet should equal(Set(
      Map("a" -> a, "b" -> b),
      Map("a" -> b, "b" -> a),
      Map("a" -> a, "b" -> a),
      Map("a" -> b, "b" -> b)
    ))
  }

  test("with aliasing") {
    createLabeledNode(Map("prop"->42), "Start")
    val b = createLabeledNode(Map("prop"->42), "End")
    createLabeledNode(Map("prop"->3), "End")

    val result = executeWithNewPlanner(
      "MATCH (a:Start) WITH a.prop AS property MATCH (b:End) WHERE property = b.prop RETURN b"
    )
    result.toSet should equal(Set(Map("b" -> b)))
  }

  test("should handle dependencies across WITH") {
    val b = createLabeledNode(Map("prop" -> 42), "End")
    createLabeledNode(Map("prop" -> 3), "End")
    createLabeledNode(Map("prop" -> b.getId), "Start")

    val result = executeWithNewPlanner(
      "MATCH (a:Start) WITH a.prop AS property LIMIT 1 MATCH (b) WHERE id(b) = property RETURN b"
    )
    result.toSet should equal(Set(Map("b" -> b)))
  }

  test("should handle dependencies across WITH with SKIP") {
    val a = createNode("prop" -> "A", "id" -> 0)
    createNode("prop" -> "B", "id" -> a.getId)
    createNode("prop" -> "C", "id" -> 0)

    val result = executeWithNewPlanner(
      """MATCH (a)
        |WITH a.prop AS property, a.id as idToUse
        |ORDER BY property
        |SKIP 1
        |MATCH (b)
        |WHERE id(b) = idToUse
        |RETURN b""".stripMargin
    )
    result.toSet should equal(Set(Map("b" -> a)))
  }

  test("WHERE after WITH filters as expected") {
    createNode("A")
    val b = createNode("B")
    createNode("C")

    val result = executeWithNewPlanner(
      "MATCH (a) WITH a WHERE a.name = 'B' RETURN a"
    )
    result.toSet should equal(Set(Map("a" -> b)))
  }

  test("WHERE after WITH can filter on top of aggregation") {
    val a = createNode("A")
    val b = createNode("B")

    relate(a, createNode())
    relate(a, createNode())
    relate(a, createNode())
    relate(b, createNode())

    val result = executeWithNewPlanner(
      "MATCH (a)-->() WITH a, count(*) as relCount WHERE relCount > 1 RETURN a"
    )
    result.toSet should equal(Set(Map("a" -> a)))
  }

  test("nulls passing through WITH") {
    executeWithNewPlanner("optional match (a:Start) with a match a-->b return *") should be (empty)
  }

  test("WITH {foo: {bar: 'baz'}} AS nestedMap RETURN nestedMap.foo.bar") {

    val result = executeWithNewPlanner(
      "WITH {foo: {bar: 'baz'}} AS nestedMap RETURN nestedMap.foo.bar"
    )
    result.toSet should equal(Set(Map("nestedMap.foo.bar" -> "baz")))
  }

}
