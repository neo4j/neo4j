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

class ComparisonOperatorAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should handle numeric ranges") {
    val one = createNode(Map("value" -> 1))
    val two = createNode(Map("value" -> 2))
    val three = createNode(Map("value" -> 3))

    executeWithAllPlanners("MATCH n WHERE 1 < n.value < 3 RETURN n").toSet should equal(
      Set(Map("n" -> two)))

    executeWithAllPlanners("MATCH n WHERE 1 < n.value <= 3 RETURN n").toSet should equal(
      Set(Map("n" -> two), Map("n" -> three)))

    executeWithAllPlanners("MATCH n WHERE 1 <= n.value < 3 RETURN n").toSet should equal(
      Set(Map("n" -> one), Map("n" -> two)))

    executeWithAllPlanners("MATCH n WHERE 1 <= n.value <= 3 RETURN n").toSet should equal(
      Set(Map("n" -> one), Map("n" -> two), Map("n" -> three)))
  }

  test("should handle string ranges") {
    val a = createNode(Map("value" -> "a"))
    val b = createNode(Map("value" -> "b"))
    val c = createNode(Map("value" -> "c"))

    executeWithAllPlanners("MATCH n WHERE 'a' < n.value < 'c' RETURN n").toSet should equal(
      Set(Map("n" -> b)))

    executeWithAllPlanners("MATCH n WHERE 'a' < n.value <= 'c' RETURN n").toSet should equal(
      Set(Map("n" -> b), Map("n" -> c)))

    executeWithAllPlanners("MATCH n WHERE 'a' <= n.value < 'c' RETURN n").toSet should equal(
      Set(Map("n" -> a), Map("n" -> b)))

    executeWithAllPlanners("MATCH n WHERE 'a' <= n.value <= 'c' RETURN n").toSet should equal(
      Set(Map("n" -> a), Map("n" -> b), Map("n" -> c)))
  }

  test("should handle empty ranges") {
    createNode(Map("value" -> 3))

    executeWithAllPlanners("MATCH n WHERE 10 < n.value < 3 RETURN n").toSet shouldBe empty
  }

  test("should handle long chains of operators") {
    val node1 = createNode(Map("prop1" -> 3, "prop2" -> 4))
    val node2 = createNode(Map("prop1" -> 4, "prop2" -> 5))
    val node3 = createNode(Map("prop1" -> 4, "prop2" -> 4))
    relate(node1, node2)
    relate(node2, node3)
    relate(node3, node1)

    executeWithAllPlanners("MATCH (n)-->(m) WHERE n.prop1 < m.prop1 = n.prop2 <> m.prop2 RETURN m").toSet should equal(Set(Map("m" -> node2)))
  }

  test("should handle numerical literal on the left when using an index") {
    graph.createIndex("Product", "unitsInStock")
    val small = createLabeledNode(Map("unitsInStock" -> 8), "Product")
    val large = createLabeledNode(Map("unitsInStock" -> 12), "Product")

    val result = executeWithCostPlannerOnly(
      """
        |MATCH (p:Product)
        |USING index p:Product(unitsInStock)
        |WHERE 10 < p.unitsInStock
        |RETURN p
      """.stripMargin)

    result.toList should equal(List(Map("p" -> large)))
  }

  test("should handle numerical literal on the right when using an index") {
    graph.createIndex("Product", "unitsInStock")
    val small = createLabeledNode(Map("unitsInStock" -> 8), "Product")
    val large = createLabeledNode(Map("unitsInStock" -> 12), "Product")

    val result = executeWithCostPlannerOnly(
      """
        |MATCH (p:Product)
        |USING index p:Product(unitsInStock)
        |WHERE p.unitsInStock > 10
        |RETURN p
      """.stripMargin)

    result.toList should equal(List(Map("p" -> large)))
  }

  test("should handle long chains of comparisons") {
    executeScalarWithAllPlanners[Boolean]("RETURN 1 < 2 < 3 < 4 as val") shouldBe true
    executeScalarWithAllPlanners[Boolean]("RETURN 1 < 3 < 2 < 4 as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 1 < 2 < 2 < 4 as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 1 < 2 <= 2 < 4 as val") shouldBe true

    executeScalarWithAllPlanners[Boolean]("RETURN 1.0 < 2.1 < 3.2 < 4.2 as val") shouldBe true
    executeScalarWithAllPlanners[Boolean]("RETURN 1.0 < 3.2 < 2.1 < 4.2 as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 1.0 < 2.1 < 2.1 < 4.2 as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 1.0 < 2.1 <= 2.1 < 4.2 as val") shouldBe true

    executeScalarWithAllPlanners[Boolean]("RETURN 'a' < 'b' < 'c' < 'd' as val") shouldBe true
    executeScalarWithAllPlanners[Boolean]("RETURN 'a' < 'c' < 'b' < 'd' as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 'a' < 'b' < 'b' < 'd' as val") shouldBe false
    executeScalarWithAllPlanners[Boolean]("RETURN 'a' < 'b' <= 'b' < 'd' as val") shouldBe true
  }

}
