/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class WithAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("only passing on pattern nodes work") {
    val a = createNode()
    val b = createNode()
    relate(a, b)

    val result = executeWithAllPlannersAndCompatibilityMode(
      "MATCH (a) WITH a MATCH (a)-->(b) RETURN *"
    )
    result.toList should equal(List(Map("a" -> a, "b" -> b)))
  }

  test("order by and limit can be used") {
    val a = createNode("A")
    createNode("B")
    createNode("C")

    relate(a,createNode())

    val result = executeWithAllPlannersAndCompatibilityMode(
      "MATCH (a) WITH a ORDER BY a.name LIMIT 1 MATCH (a)-->(b) RETURN a"
    )
    result.toList should equal(List(Map("a" -> a)))
  }

  test("without dependencies between the query parts works") {
    val a = createNode()
    val b = createNode()

    val result = executeWithAllPlannersAndCompatibilityMode(
      "MATCH (a) WITH a MATCH (b) RETURN *"
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

    val result = executeWithAllPlannersAndCompatibilityMode(
      "MATCH (a:Start) WITH a.prop AS property MATCH (b:End) WHERE property = b.prop RETURN b"
    )
    result.toSet should equal(Set(Map("b" -> b)))
  }

  test("should handle dependencies across WITH") {
    val b = createLabeledNode(Map("prop" -> 42), "End")
    createLabeledNode(Map("prop" -> 3), "End")
    createLabeledNode(Map("prop" -> b.getId), "Start")

    val result = executeWithAllPlannersAndCompatibilityMode(
      "MATCH (a:Start) WITH a.prop AS property LIMIT 1 MATCH (b) WHERE id(b) = property RETURN b"
    )
    result.toSet should equal(Set(Map("b" -> b)))
  }

  test("should handle dependencies across WITH with SKIP") {
    val a = createNode("prop" -> "A", "id" -> 0)
    createNode("prop" -> "B", "id" -> a.getId)
    createNode("prop" -> "C", "id" -> 0)

    val result = executeWithAllPlannersAndCompatibilityMode(
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

    val result = executeWithAllPlannersAndCompatibilityMode(
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

    val result = executeWithAllPlannersAndCompatibilityMode(
      "MATCH (a)-->() WITH a, count(*) as relCount WHERE relCount > 1 RETURN a"
    )
    result.toSet should equal(Set(Map("a" -> a)))
  }

  test("Can ORDER BY an aggregating key") {
    createNode("bar" -> "A")
    createNode("bar" -> "A")
    createNode("bar" -> "B")

    val result = executeWithAllPlannersAndCompatibilityMode(
      "MATCH (a) WITH a.bar as bars, count(*) as relCount ORDER BY a.bar RETURN *"
    )

    result should not be empty
  }

  test("Can ORDER BY a DISTINCT column") {
    createNode("bar" -> "A")
    createNode("bar" -> "A")
    createNode("bar" -> "B")

    val result = executeWithAllPlannersAndCompatibilityMode(
      "MATCH (a) WITH DISTINCT a.bar as bars ORDER BY a.bar RETURN *"
    )

    result should not be empty
    result should not(use("Projection"))
  }

  test("Can use WHERE on distinct columns") {
    createNode("bar" -> "A")
    createNode("bar" -> "A")
    createNode("bar" -> "B")

    val result = executeWithAllPlannersAndCompatibilityMode(
      "MATCH (a) WITH DISTINCT a.bar as bars WHERE a.bar = 'B' RETURN *"
    )

    result should not be empty
  }

  test("Can solve a simple pattern with the relationship and one endpoint bound") {
    val node1 = createNode()
    val node2 = createNode()
    val rel = relate(node1, node2)

    val result = executeWithAllPlannersAndCompatibilityMode(
      "WITH {a} AS b, {b} AS tmp, {r} AS r WITH b AS a, r LIMIT 1 MATCH (a)-[r]->(b) RETURN a, r, b",
      "a" -> node1, "b" -> node2, "r" -> rel
    )

    result.toList should equal(List(
      Map("a" -> node1, "b" -> node2, "r" -> rel)
    ))
  }

  test("nulls passing through WITH") {
    executeWithAllPlannersAndCompatibilityMode("optional match (a:Start) with a match (a)-->(b) return *") should be (empty)
  }

  test("WITH {foo: {bar: 'baz'}} AS nestedMap RETURN nestedMap.foo.bar") {

    val result = executeWithAllPlannersAndCompatibilityMode(
      "WITH {foo: {bar: 'baz'}} AS nestedMap RETURN nestedMap.foo.bar"
    )
    result.toSet should equal(Set(Map("nestedMap.foo.bar" -> "baz")))
  }

  test("connected components after WITH") {
    val n = createLabeledNode("A")
    val m = createLabeledNode("B")
    val x = createNode()
    relate(n, x)

    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:A) WITH n LIMIT 1 MATCH (m:B), (n)-->(x) RETURN *")

    result.toList should equal(List(Map("m" -> m, "n" -> n, "x" -> x)))
  }

  test("WITH with predicate and aggregation should be handled by all planners") {
    // GIVEN
    createNode(Map("prop" -> 43))
    createNode(Map("prop" -> 42))

    // WHEN
    val query = "MATCH (n) WITH n WHERE n.prop = 42 RETURN count(*)"
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Long](query)

    // THEN
    result should equal(1)
  }

  test("multiple WITHs with predicates and aggregation should be handled by all planners") {
    // GIVEN
    val david = createNode("name" -> "David")
    val other = createNode("name" -> "Other")
    val notOther = createNode("name" -> "NotOther")
    val notOther2 = createNode("name" -> "NotOther2")
    relate(david, other)
    relate(david, notOther)
    relate(david, notOther2)
    relate(other, createNode())
    relate(other, createNode())
    relate(notOther, createNode())
    relate(notOther, createNode())
    relate(notOther2, createNode())

    // WHEN
    val query = """MATCH (david { name: "David" })--(otherPerson)-->()
      |WITH otherPerson, count(*) AS foaf WHERE foaf > 1
      |WITH otherPerson WHERE otherPerson.name <> 'NotOther'
      |RETURN count(*)""".stripMargin
    val result = executeScalarWithAllPlannersAndCompatibilityMode[Long](query)

    //THEN
    result should equal(1)
  }
}
