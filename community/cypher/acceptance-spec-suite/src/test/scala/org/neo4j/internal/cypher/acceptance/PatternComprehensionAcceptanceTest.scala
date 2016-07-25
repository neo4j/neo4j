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

import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.PathImpl
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class PatternComprehensionAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("with named path") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val r = relate(n1, n2)

    val query = "MATCH (n:Start) RETURN [p = (n)-->() | p] AS list"

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("list" -> List(PathImpl(n1, r, n2)))))
  }

  test("with named path and predicate") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val n3 = createLabeledNode("NotEnd")
    val r = relate(n1, n2)
    relate(n2, n3)

    val query = "MATCH (n:Start) RETURN [p = (n)-->() WHERE last(nodes(p)):End | p] AS list"

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("list" -> List(PathImpl(n1, r, n2)))))
  }

  test("with named path and shadowed variable in predicate") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val r = relate(n1, n2)

    val query = "MATCH (n:Start) RETURN [p = (n)-->(b) WHERE head([p IN ['foo'] | true ]) | p] AS list"

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("list" -> List(PathImpl(n1, r, n2)))))
  }

  test("with named path and shadowed variable in projection") {
    val n1 = createLabeledNode("Start")
    val n2 = createLabeledNode("End")
    val r = relate(n1, n2)

    val query = "MATCH (n:Start) RETURN [p = (n)-->() | {path: p, other: [p IN ['foo'] | true ]} ] AS list"

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("list" -> List(Map("path" -> PathImpl(n1, r, n2), "other" -> List(true))))))
  }

  test("one relationship out") {
    val n1 = createLabeledNode(Map("x" -> 1), "START")
    val n2 = createLabeledNode(Map("x" -> 2), "START")
    val n3 = createNode("x" -> 3)
    val n4 = createNode("x" -> 4)
    val n5 = createNode("x" -> 5)

    relate(n1, n3)
    relate(n1, n4)
    relate(n1, n5)
    relate(n2, n4)
    relate(n2, n5)

    val result = executeWithCostPlannerOnly("match (n:START) return n.x, [(n)-->(other) | other.x] as coll")

    result.toList should equal(List(
      Map("n.x" -> 1, "coll" -> Seq(5, 4, 3)),
      Map("n.x" -> 2, "coll" -> Seq(5, 4))
    ))
    result should use("RollUpApply")
  }

  test("one relationship out with filtering") {
    val n1 = createLabeledNode(Map("x" -> 1), "START")
    val n2 = createLabeledNode(Map("x" -> 2), "START")
    val n3 = createNode("x" -> 3)
    val n4 = createNode("x" -> 4)
    val n5 = createNode("x" -> 5)
    val n6 = createNode("x" -> 6)

    relate(n1, n3)
    relate(n1, n4)
    relate(n1, n5)
    relate(n1, n6)
    relate(n2, n4)
    relate(n2, n6)

    val result = executeWithCostPlannerOnly("match (n:START) return n.x, [(n)-->(other) WHERE other.x % 2 = 0 | other.x] as coll")
    result.toList should equal(List(
      Map("n.x" -> 1, "coll" -> Seq(6, 4)),
      Map("n.x" -> 2, "coll" -> Seq(6, 4))
    ))
    result should use("RollUpApply")
  }

  test("find self relationships") {
    val n1 = createLabeledNode(Map("x" -> 1), "START")

    relate(n1, n1, "x"->"A")
    relate(n1, n1, "x"->"B")
    val result = executeWithCostPlannerOnly("match (n:START) return n.x, [(n)-[r]->(n) | r.x] as coll")

    result.toList should equal(List(
      Map("n.x" -> 1, "coll" -> Seq("B", "A"))
    ))
    result should use("RollUpApply")
  }

  test("pattern comprehension built on a null yields null") {
    val result = executeWithCostPlannerOnly("optional match (n:MISSING) return [(n)-->(n) | n.x] as coll")
    result.toList should equal(List(
      Map("coll" -> null)
    ))
    result should use("RollUpApply")
  }

  test("pattern comprehension used in a WHERE query should work") {

    val a = createLabeledNode("START")
    val b = createLabeledNode("START")

    relate(a, createNode("x" -> 1))
    relate(a, createNode("x" -> 2))
    relate(a, createNode("x" -> 3))

    relate(b, createNode("x" -> 2))
    relate(b, createNode("x" -> 4))
    relate(b, createNode("x" -> 6))


    val result = executeWithCostPlannerOnly(
      """match (n:START)
        |where [(n)-->(other) | other.x] = [3,2,1]
        |return n""".stripMargin)

    result.toList should equal(List(
      Map("n" -> a)
    ))
    result should use("RollUpApply")
  }

  test("using pattern comprehension as grouping key") {
    val n1 = createLabeledNode("START")
    val n2 = createLabeledNode("START")
    val n3 = createNode("x" -> 3)
    val n4 = createNode("x" -> 4)
    val n5 = createNode("x" -> 5)

    relate(n1, n3)
    relate(n1, n4)
    relate(n1, n5)

    relate(n2, n3)
    relate(n2, n4)
    relate(n2, n5)

    val result = executeWithCostPlannerOnly("match (n:START) return count(*), [(n)-->(other) | other.x] as coll")
    result.toList should equal(List(
      Map("count(*)" -> 2, "coll" -> Seq(5, 4, 3))
    ))
    result should use("RollUpApply")
  }

  test("aggregating pattern comprehensions") {
    val n1 = createLabeledNode("START")
    val n2 = createLabeledNode("START")
    val n3 = createNode("x" -> 3)
    val n4 = createNode("x" -> 4)
    val n5 = createNode("x" -> 5)
    val n6 = createNode("x" -> 6)

    relate(n1, n3)
    relate(n1, n4)
    relate(n1, n5)

    relate(n2, n3)
    relate(n2, n4)
    relate(n2, n6)

    val result = executeWithCostPlannerOnly(
      """match (n:START)
        |return collect( [(n)-->(other) | other.x] ) as coll""".stripMargin)
    result.toList should equal(List(
      Map("coll" -> Seq(Seq(5, 4, 3), Seq(6, 4, 3)))
    ))
  }

  test("simple expansion using pattern comprehension") {
    val a = createNode("name" -> "Mats")
    val b = createNode("name" -> "Max")
    val c = createNode()
    relate(a, b)
    relate(b, c)
    relate(c, a)

    val query = "MATCH (a) RETURN [(a)-->() | a.name] AS list"

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("list" -> List("Mats")), Map("list" -> List("Max")), Map("list" -> List(null))))
  }
}
