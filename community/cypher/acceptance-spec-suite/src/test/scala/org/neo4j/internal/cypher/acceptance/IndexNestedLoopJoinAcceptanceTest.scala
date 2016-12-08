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

import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.NodeIndexSeek
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.Node

class IndexNestedLoopJoinAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {
  test("test that index seek is planned on the RHS using information from the LHS") {
    // given

    def relateToThreeNodes(n: Node) = {
      relate(n, createNode("id" -> 1))
      relate(n, createNode("id" -> 2))
      relate(n, createNode("id" -> 3))
    }

    (0 to 100) foreach (i => relateToThreeNodes(createLabeledNode(Map("id" -> i), "A")))
    (0 to 100) foreach (i => createLabeledNode(Map("id" -> i), "C"))

    graph.createIndex("A", "id")
    graph.createIndex("C", "id")

    // when
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:A)-->(b), (c:C) WHERE b.id = c.id AND a.id = 42 RETURN count(*)")

    result.toList should equal(List(Map("count(*)" -> 3)))

    // then
    result should use("Apply", "NodeIndexSeek")
    result should not(use("ValueHashJoin", "CartesianProduct", "NodeByLabelScan", "Filter"))
  }

  test("index seek planned in the presence of optional matches") {
    // given

    def relateToThreeNodes(n: Node) = {
      relate(n, createNode("id" -> 1))
      relate(n, createNode("id" -> 2))
      relate(n, createNode("id" -> 3))
    }

    (0 to 100) foreach (i => relateToThreeNodes(createLabeledNode(Map("id" -> i), "A")))
    (0 to 100) foreach (i => createLabeledNode(Map("id" -> i), "C"))

    graph.createIndex("A", "id")
    graph.createIndex("C", "id")

    // when
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (a:A)-->(b), (c:C) WHERE b.id = c.id AND a.id = 42 OPTIONAL MATCH (a)-[:T]->() RETURN count(*)")

    result.toList should equal(List(Map("count(*)" -> 3)))

    // then
    result should use("Apply", "NodeIndexSeek")
    result should not(use("ValueHashJoin", "CartesianProduct", "NodeByLabelScan", "Filter"))
  }

  test("should use index on literal value") {
    val node = createLabeledNode(Map("id" -> 123), "Foo")
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id=123
        | RETURN f
      """.stripMargin
    val result = executeWithCostPlannerOnly(query)
    result.columnAs[Node]("f").toList should equal(List(node))
    result.executionPlanDescription() should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f")
  }

  test("should use index on variable defined from literal value") {
    val node = createLabeledNode(Map("id" -> 123), "Foo")
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | WITH 123 AS row
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id=row
        | RETURN f
      """.stripMargin
    val result = executeWithCostPlannerOnly(query)
    result.columnAs[Node]("f").toList should equal(List(node))
    result.executionPlanDescription() should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f")
  }

  test("should use index on literal map expression") {
    val nodes = Range(0,125).map(i => createLabeledNode(Map("id" -> i), "Foo"))
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id={id: 123}.id
        | RETURN f
      """.stripMargin
    val result = executeWithCostPlannerOnly(query)
    result.columnAs[Node]("f").toSet should equal(Set(nodes(123)))
    result.executionPlanDescription() should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f")
  }

  test("should use index on variable defined from literal map") {
    val nodes = Range(0,125).map(i => createLabeledNode(Map("id" -> i), "Foo"))
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | WITH [{id: 123}, {id: 122}] AS rows
        | UNWIND rows AS row
        | MATCH (f:Foo)
        | USING INDEX f:Foo(id)
        | WHERE f.id=row.id
        | RETURN f
      """.stripMargin
    val result = executeWithCostPlannerOnly(query)
    result.columnAs[Node]("f").toSet should equal(Set(nodes(122),nodes(123)))
    result.executionPlanDescription() should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f")
  }

  test("should use index on other node property value where there is no incoming horizon") {
    val nodes = Range(0,125).map(i => createLabeledNode(Map("id" -> i), "Foo"))
    val n1=createLabeledNode(Map("id"->122),"Bar")
    val n2=createLabeledNode(Map("id"->123),"Bar")
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | MATCH (b:Bar) WHERE b.id = 123
        | MATCH (f:Foo) WHERE f.id = b.id
        | RETURN f
      """.stripMargin
    val result = executeWithCostPlannerOnly(query)
    result.columnAs[Node]("f").toList should equal(List(nodes(123)))
    result.executionPlanDescription() should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f")
  }

  test("should use index on other node property value where there is an incoming horizon") {
    val nodes = Range(0,125).map(i => createLabeledNode(Map("id" -> i), "Foo"))
    val n1=createLabeledNode(Map("id"->122),"Bar")
    val n2=createLabeledNode(Map("id"->123),"Bar")
    graph.createIndex("Foo", "id")
    val query =
      """
        |PROFILE
        | WITH [122, 123] AS rows
        | UNWIND rows AS row
        | MATCH (b:Bar) WHERE b.id = row
        | MATCH (f:Foo) WHERE f.id = b.id
        | RETURN f
      """.stripMargin
    val result = executeWithCostPlannerOnly(query)
    result.columnAs[Node]("f").toSet should equal(Set(nodes(122), nodes(123)))
    result.executionPlanDescription() should includeAtLeastOne(classOf[NodeIndexSeek], withVariable = "f")
  }
}
