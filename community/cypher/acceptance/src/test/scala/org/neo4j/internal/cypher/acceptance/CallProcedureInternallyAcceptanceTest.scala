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

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.IgnoreAllTests
import org.neo4j.cypher._
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.proc.{CallableProcedure, Neo4jTypes}
import CallableProcedure.Context
import org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature
import org.neo4j.kernel.api.proc.Neo4jTypes
import CallableProcedure.BasicProcedure
import org.scalatest.Tag

// TODO: Fix return argument handling
class CallProcedureInternallyAcceptanceTest extends ExecutionEngineFunSuite with IgnoreAllTests {

  test("should be able to find labels from built-in-procedure") {
    // Given
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    val result = execute("CALL db.labels AS label RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A"),
        Map("label" -> "B"),
        Map("label" -> "C")))
  }

  test("should be able to find labels from built-in-procedure from within a query") {
    // Given
    createLabeledNode(Map("name" -> "Tic"), "A")
    createLabeledNode(Map("name" -> "Tac"), "B")
    createLabeledNode(Map("name" -> "Toc"), "C")

    //When
    val result = execute("MATCH (n {name: 'Toc'}) WITH n.name AS name CALL db.labels AS label RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("name" -> "Toc", "label" -> "A"),
        Map("name" -> "Toc", "label" -> "B"),
        Map("name" -> "Toc", "label" -> "C")))
  }

  test("sys.db.labels work on an empty database") {
    // Given an empty database
    //When
    val result = execute("CALL db.labels AS label RETURN *")

    // Then
    result.toList shouldBe empty
  }

  test("should be able to call procedure with explicit arguments") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // When
    val result = execute("CALL my.first.proc('42', 42) AS x, y RETURN *")

    // Then
    result.toList should equal(List(Map("x" -> "42", "y" -> 42)))
  }

  test("should be able to call procedure with implicit arguments") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // When
    val result = execute("CALL my.first.proc AS x, y RETURN *", "in0" -> "42", "in1" -> 42)

    // Then
    result.toList should equal(List(Map("x" -> "42", "y" -> 42)))
  }

  // TODO: Should throw InvalidArgumentException but this requires moving resolution to ast rewriter which currently has no plan context
  test("should fail if input type is wrong") {
    // Given
    register(Neo4jTypes.NTNumber)

    // Then
    a [SyntaxException] shouldBe thrownBy(execute("CALL my.first.proc('ten') AS x RETURN x"))
  }

  test("if signature declares number all number types are valid") {
    // Given
    register(Neo4jTypes.NTNumber)

    // Then
    execute("CALL my.first.proc(42) AS x RETURN *").toList should equal(List(Map("x" -> 42)))
    execute("CALL my.first.proc(42.3) AS x RETURN *").toList should equal(List(Map("x" -> 42.3)))
  }

  test("arguments are nullable") {
    // Given
    register(Neo4jTypes.NTNumber)

    // Then
    execute("CALL my.first.proc(NULL) AS x RETURN *").toList should equal(List(Map("x" -> null)))
  }

  // TODO: Should throw CypherTypeException but this requires moving resolution to ast rewriter which currently has no plan context
  test("should fail a procedure declares an integer but gets a float ") {
    // Given
    register(Neo4jTypes.NTInteger)

    // Then
    a [SyntaxException] shouldBe thrownBy(execute("CALL my.first.proc(42.0) AS x RETURN *"))
  }

  // TODO: Should throw InvalidArgumentException but this requires moving resolution to ast rewriter which currently has no plan context
  test("should fail if explicit argument is missing") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    an [SyntaxException] shouldBe thrownBy(execute("CALL my.first.proc('ten') AS x, y RETURN *"))
  }

  // TODO: Should throw InvalidArgumentException but this requires moving resolution to ast rewriter which currently has no plan context
  test("should fail if too many arguments") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    an [SyntaxException] shouldBe thrownBy(execute("CALL my.first.proc('ten', 10, 42) AS x, y, z RETURN *"))
  }

  test("should fail if implicit argument is missing") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    an [ParameterNotFoundException] shouldBe thrownBy(execute("CALL my.first.proc AS x, y RETURN *", "x" -> "42", "y" -> 42))
  }

  test("should be able to call a procedure with explain") {
    // Given
    register(Neo4jTypes.NTNumber)

    // When
    val result = execute("EXPLAIN CALL my.first.proc(42) AS x RETURN *")

    // Then
    result shouldBe empty
  }

  // TODO: Should throw CypherExecutionException but this requires moving resolution to ast rewriter which currently has no plan context
  test("should fail if calling non-existent procedure") {
    a [CypherExecutionException] shouldBe thrownBy(execute("CALL no.such.thing.exists(42) AS x RETURN *"))
  }

  private def register(types: Neo4jTypes.AnyType*) = {

    val builder = procedureSignature(Array("my", "first"), "proc")

    for (i <- types.indices) {

      builder
        .in(s"in$i", types(i))
        .out(s"out$i", types(i))
    }

    val proc = new BasicProcedure(builder.build) {
      override def apply(ctx: Context, input: Array[AnyRef]): RawIterator[Array[AnyRef], ProcedureException] =
        RawIterator.of[Array[AnyRef], ProcedureException](input)
    }
    kernel.registerProcedure(proc)
  }

  override protected def initTest() {
    super.initTest()
    kernel = graph.getDependencyResolver.resolveDependency(classOf[KernelAPI])
  }

  private var kernel: KernelAPI = null

}
