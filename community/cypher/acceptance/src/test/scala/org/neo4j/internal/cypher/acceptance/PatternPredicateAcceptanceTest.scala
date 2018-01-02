/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.graphdb.Node
import org.scalatest.Matchers

class PatternPredicateAcceptanceTest extends ExecutionEngineFunSuite with Matchers with NewPlannerTestSupport {

  test("should filter relationships with properties") {
    // given
    val node = createNode()
    relate(node, createNode(), "id" -> 1)
    relate(createNode(), createNode(), "id" -> 2)

    // when
    val result = executeScalarWithAllPlanners[Node]("match (n) where (n)-[{id: 1}]->() return n")

    // then
    result should equal(node)
  }

  test("should support negated pattern predicate") {
    // given
    val node = createNode()
    relate(node, createNode(), "id" -> 1)
    relate(createNode(), createNode(), "id" -> 2)

    // when
    val result = executeWithAllPlanners("match (n) where NOT (n)-[{id: 1}]->() return n").columnAs[Node]("n").toList

    // then
    result.size should be(3)
    result should not contain(node)
  }

  test("should filter var length relationships with properties") {
    // given
    val start1 = createPath(12, 42)
    createPath(324234,666)

    // when
    val result = executeScalarWithAllPlanners[Node]("match (n:Start) where (n)-[*2 {prop: 42}]->() return n")

    // then
    assert(start1 == result)
  }

  test("should handle or between an expression and a subquery") {
    // given
    val start1 = createPath(33, 42)
    val start2 = createPath(12, 666)
    createPath(55555, 7777)

    // when
    val result = executeWithAllPlanners("match (n:Start) where n.p = 12 OR (n)-[*2 {prop: 42}]->() return n").columnAs[Node]("n").toList

    // then
    assert(Seq(start1, start2) == result)
  }

  test("should handle or between 2 expressions and a subquery") {
    // Given
    val start1 = createPath(33, 42)
    val start2 = createPath(12, 666)
    val start3 = createPath(25, 444)
    createPath(55555, 7777)

    // when
    val result = executeWithAllPlanners("match (n:Start) where n.p = 12 OR (n)-[*2 {prop: 42}]->() OR n.p = 25 return n").columnAs[Node]("n").toList

    // then
    assert(Seq(start1, start2, start3) == result)
  }

  test("should handle or between one expression and a negated subquery") {
    // given
    val start1 = createPath(nodePropertyValue = 25, relPropertyValue = 444)
    val start2 = createPath(nodePropertyValue = 12, relPropertyValue = 42)
    createPath(nodePropertyValue = 25, relPropertyValue = 42)

    // when
    val result = executeWithAllPlanners("match (n:Start) where n.p = 12 OR NOT (n)-[*2 {prop: 42}]->() return n").columnAs[Node]("n").toList

    // then
    assert(Seq(start1, start2) == result)
  }

  test("should handle or between one subquery and a negated subquery") {
    // given
    val start1 = createLabeledNode("Start")
    relate(start1, createNode(Map("prop" -> 42)))
    val start2 = createLabeledNode("Start")
    relate(start2, createNode(Map("prop" -> 411)))
    val start3 = createLabeledNode("Start")

    // when
    val result = executeWithAllPlanners("match (n:Start) where (n)-->({prop: 42}) OR NOT (n)-->() return n").columnAs[Node]("n").toList

    // then
    assert(Seq(start1, start3) == result)
  }

  test("should handle or between two subqueries") {
    // given
    val start1 = createLabeledNode("Start")
    relate(start1, createNode(Map("prop" -> 42)))
    val start2 = createLabeledNode("Start")
    relate(start2, createNode(Map("prop" -> 411)))
    createLabeledNode("Start")

    // when
    val result = executeWithAllPlanners("match (n:Start) where (n)-->({prop: 42}) OR (n)-->({prop: 411}) return n").columnAs[Node]("n").toList

    // then
    assert(Seq(start1, start2) == result)
  }

  test("should handle or between one negated subquery and a subquery") {
    // given
    val start1 = createLabeledNode("Start")
    relate(start1, createNode(Map("prop" -> 42)))
    val start2 = createLabeledNode("Start")
    relate(start2, createNode(Map("prop" -> 411)))
    val start3 = createLabeledNode("Start")

    // when
    val result = executeWithAllPlanners("match (n:Start) where NOT (n)-->() OR (n)-->({prop: 42}) return n").columnAs[Node]("n").toList

    // then
    assert(Seq(start1, start3) == result)
  }

  test("should handle or between one negated subquery, a subquery and a regular expression") {
    // given
    val start1 = createLabeledNode("Start")
    relate(start1, createNode(Map("prop" -> 42)))
    val start2 = createLabeledNode("Start")
    relate(start2, createNode(Map("prop" -> 411)))
    val start3 = createLabeledNode(Map("prop" -> 21), "Start")
    createNode()

    // when
    val result = executeWithAllPlanners("match (n:Start) where n.prop = 21 OR NOT (n)-->() OR (n)-->({prop: 42}) return n").columnAs[Node]("n").toList

    // then
    assert(Seq(start1, start3) == result)
  }

  test("should handle or between one negated subquery, two subqueries and a regular expression") {
    // given
    val start1 = createLabeledNode("Start")
    relate(start1, createNode(Map("prop" -> 42)))
    val start2 = createLabeledNode("Start")
    relate(start2, createNode(Map("prop" -> 411)))
    val start3 = createLabeledNode(Map("prop" -> 21), "Start")
    val start4 = createLabeledNode("Start")
    relate(start4, createNode(Map("prop" -> 1)))

    // when
    val result = executeWithAllPlanners("match (n:Start) where n.prop = 21 OR NOT (n)-->() OR (n)-->({prop: 42}) OR (n)-->({prop: 1}) return n").columnAs[Node]("n").toList

    // then
    assert(Seq(start1, start3, start4) == result)
  }

  test("should handle or between one negated subquery, two subqueries and a regular expression 2") {
    // given
    val start1 = createLabeledNode("Start")
    relate(start1, createNode(Map("prop" -> 42)))
    val start2 = createLabeledNode("Start")
    relate(start2, createNode(Map("prop" -> 411)))
    val start3 = createLabeledNode(Map("prop" -> 21), "Start")
    val start4 = createLabeledNode("Start")
    relate(start4, createNode(Map("prop" -> 1)))

    // when
    val result = executeWithAllPlanners("match (n:Start) where n.prop = 21 OR (n)-->({prop: 42}) OR NOT (n)-->() OR (n)-->({prop: 1}) return n").columnAs[Node]("n").toList

    // then
    assert(Seq(start1, start3, start4) == result)
  }

  private def createPath(nodePropertyValue: Any, relPropertyValue: Any): Node = {
    val node0 = createLabeledNode(Map("p" -> nodePropertyValue), "Start")
    val node1 = createNode()

    relate(node0, node1, "prop" -> relPropertyValue)
    relate(node1, createNode(), "prop" -> relPropertyValue)

    node0
  }
}
