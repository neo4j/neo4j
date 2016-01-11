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

import java.util.stream.Stream

import org.neo4j.cypher.{CypherTypeException, ExecutionEngineFunSuite, InvalidArgumentException}
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.proc.Procedure.{BasicProcedure, Context}
import org.neo4j.proc.ProcedureSignature.procedureSignature
import org.neo4j.proc.{Neo4jTypes, ProcedureSignature => KernelSignature}

class CallProcedureAcceptanceTest extends ExecutionEngineFunSuite {

  test("should be able to find labels from built-in-procedure") {
    // Given
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    val result = execute("CALL sys.db.labels")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A"),
        Map("label" -> "B"),
        Map("label" -> "C")))
  }

  test("should be able to find types from built-in-procedure") {
    // Given
    relate(createNode(), createNode(), "A")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "C")

    // When
    val result = execute("CALL sys.db.relationshipTypes")

    // Then
    result.toList should equal(
      List(
        Map("relationshipTypes" -> "A"),
        Map("relationshipTypes" -> "B"),
        Map("relationshipTypes" -> "C")))
  }

  test("should be able to find propertyKeys from built-in-procedure") {
    // Given
    createNode("A" -> 1, "B" -> 2, "C" -> 3)

    // When
    val result = execute("CALL sys.db.propertyKeys")

    // Then
    result.toList should equal(
      List(
        Map("propertyKeys" -> "A"),
        Map("propertyKeys" -> "B"),
        Map("propertyKeys" -> "C")))
  }

  test("should be able to call procedure with explicit arguments") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // When
    val result = execute("CALL my.first.proc('42', 42)")

    // Then
    result.toList should equal(List(Map("out0" -> "42", "out1" -> 42)))
  }

  test("should be able to call procedure with implicit arguments") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // When
    val result = execute("CALL my.first.proc", "in0" -> "42", "in1" -> 42)

    // Then
    result.toList should equal(List(Map("out0" -> "42", "out1" -> 42)))
  }

  test("should fail if input type is wrong") {
    // Given
    register(Neo4jTypes.NTNumber)

    // Then
    a [CypherTypeException] shouldBe thrownBy(execute("CALL my.first.proc('ten')"))
  }

  test("if signature declares number all number types are valid") {
    // Given
    register(Neo4jTypes.NTNumber)

    // Then
    execute("CALL my.first.proc(42)").toList should equal(List(Map("out0" -> 42)))
    execute("CALL my.first.proc(42.3)").toList should equal(List(Map("out0" -> 42.3)))
  }

  test("should fail a procedure declares an integer but gets a float ") {
    // Given
    register(Neo4jTypes.NTInteger)

    // Then
    a [CypherTypeException] shouldBe thrownBy(execute("CALL my.first.proc(42.0)"))
  }

  test("should fail a procedure declares a float but gets an integer") {
    // Given
    register(Neo4jTypes.NTFloat)

    // Then
    a [CypherTypeException] shouldBe thrownBy(execute("CALL my.first.proc(42)"))
  }

  test("should fail if explicit argument is missing") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    an [InvalidArgumentException] shouldBe thrownBy(execute("CALL my.first.proc('ten')"))
  }

  test("should fail if implicit argument is missing") {
    // Given
    register(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    an [InvalidArgumentException] shouldBe thrownBy(execute("CALL my.first.proc", "in0" -> "42", "in42" -> 42))
  }

  private def register(types: Neo4jTypes.AnyType*) = {

    val builder = procedureSignature(Array("my", "first"), "proc")

    for (i <- types.indices) {

      builder
        .in(s"in$i", types(i))
        .out(s"out$i", types(i))
    }

    val proc = new BasicProcedure(builder.build) {
      override def apply(ctx: Context, input: Array[AnyRef]): Stream[Array[AnyRef]] =
        java.util.stream.Stream.of[Array[AnyRef]](input)
    }
    kernel.registerProcedure(proc)
  }

  override protected def initTest() {
    super.initTest()
    kernel = graph.getDependencyResolver.resolveDependency(classOf[KernelAPI])
  }

  private var kernel: KernelAPI = null

}
