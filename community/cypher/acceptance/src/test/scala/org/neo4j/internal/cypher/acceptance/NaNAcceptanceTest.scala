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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_0.executionplan.InternalExecutionResult
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, SyntaxException}

class NaNAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("square root of negative number should not produce nan") {
    val query = "WITH sqrt(-1) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("log of negative number should not produce nan") {
    val query = "WITH log(-1) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("log10 of negative number should not produce nan") {
    val query = "WITH log10(-1) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("asin() outside of [-1, 1] should not produce nan") {
    val query = "WITH asin(2) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("acos() outside of [-1, 1] should not produce nan") {
    val query = "WITH acos(2) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("0 over 0 should not produce nan") {
    val query = s"WITH 0.0 / 0.0 AS shouldBeNull RETURN shouldBeNull"

    a [org.neo4j.cypher.ArithmeticException] should be thrownBy executeWithAllPlanners(query)
  }

  test("any other number over 0 should produce a too large number") {
    val query = s"WITH 10.0 / 0.0 AS inf RETURN inf"

    a [SyntaxException] should be thrownBy executeWithAllPlanners(query)
  }

  test("square root of negative number should not produce nan with parameter") {
    val query = "WITH sqrt({param}) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "param" -> -1)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("log of negative number should not produce nan with parameter") {
    val query = "WITH log({param}) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "param" -> -1)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("log10 of negative number should not produce nan with parameter") {
    val query = "WITH log10({param}) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "param" -> -1)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("asin() outside of [-1, 1] should not produce nan with parameter") {
    val query = "WITH asin({param}) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "param" -> 2)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("acos() outside of [-1, 1] should not produce nan with parameter") {
    val query = "WITH acos({param}) AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "param" -> 2)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("0 over 0 should not produce nan with parameters") {
    val query = s"WITH {lhs} / {rhs} AS shouldBeNull RETURN shouldBeNull"

    a [org.neo4j.cypher.ArithmeticException] should be thrownBy executeWithAllPlanners(query, "lhs" -> 0.0, "rhs" -> 0.0)
  }

  test("any other number over 0 should produce a too large number with parameters") {
    val query = s"WITH {lhs} / {rhs} AS inf RETURN inf"

    a [org.neo4j.cypher.ArithmeticException] should be thrownBy executeWithAllPlanners(query, "lhs" -> 10.0, "rhs" -> 0.0)
  }

  test("inf / inf should not produce nan") {
    val query = "WITH {lhs} / {rhs} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "lhs" -> Double.PositiveInfinity, "rhs" -> Double.PositiveInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("inf / -inf should not produce nan") {
    val query = "WITH {lhs} / {rhs} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "lhs" -> Double.PositiveInfinity, "rhs" -> Double.NegativeInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("-inf / inf should not produce nan") {
    val query = "WITH {lhs} / {rhs} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "lhs" -> Double.NegativeInfinity, "rhs" -> Double.PositiveInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("-inf / -inf should not produce nan") {
    val query = "WITH {lhs} / {rhs} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "lhs" -> Double.NegativeInfinity, "rhs" -> Double.NegativeInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("inf + -inf should not produce nan") {
    val query = "WITH {lhs} + {rhs} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "lhs" -> Double.PositiveInfinity, "rhs" -> Double.NegativeInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("-inf + inf should not produce nan") {
    val query = "WITH {lhs} + {rhs} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "lhs" -> Double.NegativeInfinity, "rhs" -> Double.PositiveInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("-inf + inf should not produce nan for float infinities") {
    val query = "WITH {lhs} + {rhs} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "lhs" -> Float.NegativeInfinity, "rhs" -> Float.PositiveInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("inf - inf should not produce nan") {
    val query = "WITH {lhs} - {rhs} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "lhs" -> Double.PositiveInfinity, "rhs" -> Double.PositiveInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("-inf - -inf should not produce nan") {
    val query = "WITH {lhs} - {rhs} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "lhs" -> Double.NegativeInfinity, "rhs" -> Double.NegativeInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("0 * inf should not produce nan") {
    val query = "WITH {lhs} * {rhs} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "lhs" -> 0, "rhs" -> Double.PositiveInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("0 * -inf should not produce nan") {
    val query = "WITH {lhs} * {rhs} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "lhs" -> 0, "rhs" -> Double.NegativeInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("1^inf should not produce nan") {
    val query = "WITH 1^{inf} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "inf" -> Double.PositiveInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("1^-inf should not produce nan") {
    val query = "WITH 1^{inf} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "inf" -> Float.NegativeInfinity)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("a parameter holding double NaN should be filtered just as if it were null") {
    val query = "WITH {nan} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "nan" -> Double.NaN)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("a parameter holding float NaN should be filtered just as if it were null") {
    val query = "WITH {nan} AS shouldBeNull RETURN shouldBeNull"

    val result = executeWithAllPlanners(query, "nan" -> Float.NaN)

    shouldContainOnlyNullFor(result, "shouldBeNull")
  }

  test("a parameter holding double NaN should be filtered just as if it were null for write query") {
    createNode()
    val query = "WITH {nan} AS shouldBeNull MATCH (n) SET n.prop = shouldBeNull RETURN n.prop"

    val result = executeWithAllPlanners(query, "nan" -> Double.NaN)

    shouldContainOnlyNullFor(result, "n.prop")
  }

  test("a parameter holding float NaN should be filtered just as if it were null for write query") {
    createNode()
    val query = "WITH {nan} AS shouldBeNull MATCH (n) SET n.prop = shouldBeNull RETURN n.prop"

    val result = executeWithAllPlanners(query, "nan" -> Float.NaN)

    shouldContainOnlyNullFor(result, "n.prop")
  }

  test("exists should return false for double nans") {
    createLabeledNode(Map("prop" -> Double.NaN, "name" -> "Mats"), "Label")
    val query = "MATCH (n:Label) WHERE exists(n.prop) RETURN n.name"

    val result = executeWithAllPlanners(query)

    result.toList shouldBe empty
  }

  test("exists should return false for double nans for relationships") {
    relate(createNode(), createNode(), "REL", Map("prop" -> Double.NaN, "name" -> "Mats"))
    val query = "MATCH ()-[r]->() WHERE exists(r.prop) RETURN r.name"

    val result = executeWithAllPlanners(query)

    result.toList shouldBe empty
  }

  test("exists should return false for float nans") {
    createLabeledNode(Map("prop" -> Float.NaN, "name" -> "Mats"), "Label")
    val query = "MATCH (n:Label) WHERE exists(n.prop) RETURN n.name"

    val result = executeWithAllPlanners(query)

    result.toList shouldBe empty
  }

  test("NOT exists should return true for float nans") {
    createLabeledNode(Map("prop" -> Float.NaN, "name" -> "Mats"), "Label")
    val query = "MATCH (n:Label) WHERE NOT exists(n.prop) RETURN n.name"

    val result = executeWithAllPlanners(query)

    result.toList shouldBe List(Map("n.name" -> "Mats"))
  }

  test("IS NULL should return true for nans") {
    createLabeledNode(Map("prop" -> Float.NaN, "name" -> "Mats"), "Label")
    val query = "MATCH (n:Label) WHERE n.prop IS NULL RETURN n.name"

    val result = executeWithAllPlanners(query)

    result.toList shouldBe List(Map("n.name" -> "Mats"))
  }

  test("IS NULL should return true for nans on relationships") {
    relate(createNode(), createNode(), "REL", Map("prop" -> Float.NaN, "name" -> "Mats"))
    val query = "MATCH ()-[r]->() WHERE r.prop IS NULL RETURN r.name"

    val result = executeWithAllPlanners(query)

    result.toList shouldBe List(Map("r.name" -> "Mats"))
  }

  test("NOT IS NULL should return false for nans") {
    createLabeledNode(Map("prop" -> Float.NaN, "name" -> "Mats"), "Label")
    val query = "MATCH (n:Label) WHERE NOT n.prop IS NULL RETURN n.name"

    val result = executeWithAllPlanners(query)

    result.toList shouldBe empty
  }

  test("IS NOT NULL should return false for nans") {
    createLabeledNode(Map("prop" -> Double.NaN, "name" -> "Mats"), "Label")
    val query = "MATCH (n:Label) WHERE n.prop IS NOT NULL RETURN n.name"

    val result = executeWithAllPlanners(query)

    result.toList shouldBe empty
  }

  ignore("index scans should skip float nans") {
    graph.createIndex("Label", "prop")
    createLabeledNode(Map("prop" -> Float.NaN, "name" -> "Mats"), "Label")
    val query = "MATCH (n:Label) USING INDEX n:Label(prop) WHERE exists(n.prop) RETURN n.name"

    val result = executeWithAllPlanners(query)

    println(result.executionPlanDescription())

    result.toList shouldBe empty
  }

  ignore("index scans should skip double nans") {
    graph.createIndex("Label", "prop")
    createLabeledNode(Map("prop" -> Float.NaN, "name" -> "Mats"), "Label")
    val query = "MATCH (n:Label) USING INDEX n:Label(prop) WHERE exists(n.prop) RETURN n.name"

    val result = executeWithAllPlanners(query)

    println(result.executionPlanDescription())

    result.toList shouldBe empty
  }

  private def shouldContainOnlyNullFor(result: InternalExecutionResult, column: String) = {
    result.toList should equal(List(Map(column -> null)))
  }

}
