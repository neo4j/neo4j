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
import org.neo4j.cypher.{CypherExecutionException, CypherTypeException, ExecutionEngineFunSuite, InvalidArgumentException}
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.proc.{CallableProcedure, Neo4jTypes}
import CallableProcedure.Context
import org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature
import org.neo4j.kernel.api.proc.Neo4jTypes
import CallableProcedure.BasicProcedure
import org.scalatest.Tag

class CallProcedureInternallyAcceptanceTest extends ExecutionEngineFunSuite with IgnoreAllTests {

  test("should be able to find labels from built-in-procedure") {
    // Given
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    val result = execute("CALL db.labels RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A"),
        Map("label" -> "B"),
        Map("label" -> "C")))
  }

  test("sys.db.labels work on an empty database") {
    // Given an empty database
    //When
    val result = execute("CALL db.labels RETURN *")

    // Then
    result.toList shouldBe empty
  }

  test("should be able to call procedure with explicit arguments") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // When
    val result = execute("CALL my.first.proc('42', 42) RETURN *")

    // Then
    result.toList should equal(List(Map("out0" -> "42", "out1" -> 42)))
  }

  test("should be able to call procedure with implicit arguments") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // When
    val result = execute("CALL my.first.proc RETURN *", "in0" -> "42", "in1" -> 42)

    // Then
    result.toList should equal(List(Map("out0" -> "42", "out1" -> 42)))
  }

  test("should fail if input type is wrong") {
    // Given
    register(Neo4jTypes.NTNumber)

    // Then
    a [CypherTypeException] shouldBe thrownBy(execute("CALL my.first.proc('ten') RETURN *"))
  }

  test("if signature declares number all number types are valid") {
    // Given
    register(Neo4jTypes.NTNumber)

    // Then
    execute("CALL my.first.proc(42) RETURN *").toList should equal(List(Map("out0" -> 42)))
    execute("CALL my.first.proc(42.3) RETURN *").toList should equal(List(Map("out0" -> 42.3)))
  }

  test("arguments are nullable") {
    // Given
    register(Neo4jTypes.NTNumber)

    // Then
    execute("CALL my.first.proc(NULL) RETURN *").toList should equal(List(Map("out0" -> null)))
  }

  test("should fail a procedure declares an integer but gets a float ") {
    // Given
    register(Neo4jTypes.NTInteger)

    // Then
    a [CypherTypeException] shouldBe thrownBy(execute("CALL my.first.proc(42.0) RETURN *"))
  }

  test("should fail a procedure declares a float but gets an integer") {
    // Given
    register(Neo4jTypes.NTFloat)

    // Then
    a [CypherTypeException] shouldBe thrownBy(execute("CALL my.first.proc(42) RETURN *"))
  }

  test("should fail if explicit argument is missing") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    an [InvalidArgumentException] shouldBe thrownBy(execute("CALL my.first.proc('ten') RETURN *"))
  }

  test("should fail if too many arguments") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    an [InvalidArgumentException] shouldBe thrownBy(execute("CALL my.first.proc('ten', 10, 42) RETURN *"))
  }

  test("should fail if implicit argument is missing") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    an [InvalidArgumentException] shouldBe thrownBy(execute("CALL my.first.proc RETURN *", "in0" -> "42", "in42" -> 42))
  }

  test("should be able to call a procedure with explain") {
    // Given
    register(Neo4jTypes.NTNumber)

    // When
    val result = execute("EXPLAIN CALL my.first.proc(42) RETURN *")

    // Then
    result shouldBe empty
  }

  test("should fail if calling non-existent procedure") {
    a [CypherExecutionException] shouldBe thrownBy(execute("CALL no.such.thing.exists(42) RETURN *"))
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
