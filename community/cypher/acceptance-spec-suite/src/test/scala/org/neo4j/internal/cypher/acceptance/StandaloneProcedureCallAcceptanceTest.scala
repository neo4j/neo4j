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

import org.neo4j.cypher._
import org.neo4j.kernel.api.proc.Neo4jTypes

class StandaloneProcedureCallAcceptanceTest extends ProcedureCallAcceptanceTest {

  test("should be able to call procedure with explicit arguments") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // When
    val result = execute("CALL my.first.proc('42', 42)")

    // Then
    result.toList should equal(List(Map("out0" -> "42", "out1" -> 42)))
  }

  test("should be able to call procedure with implicit arguments") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // When
    val result = execute("CALL my.first.proc", "in0" -> "42", "in1" -> 42)

    // Then
    result.toList should equal(List(Map("out0" -> "42", "out1" -> 42)))
  }

  test("should fail if input type is wrong") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTNumber)

    // Then
    a [SyntaxException] shouldBe thrownBy(execute("CALL my.first.proc('ten')"))
  }

  test("if signature declares number all number types are valid") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTNumber)

    // Then
    execute("CALL my.first.proc(42)").toList should equal(List(Map("out0" -> 42)))
    execute("CALL my.first.proc(42.3)").toList should equal(List(Map("out0" -> 42.3)))
  }

  test("arguments are nullable") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTNumber)

    // Then
    execute("CALL my.first.proc(NULL)").toList should equal(List(Map("out0" -> null)))
  }

  test("should not fail if a procedure declares a float but gets an integer") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTFloat)

    // Then
    a [CypherTypeException] shouldNot be(thrownBy(execute("CALL my.first.proc(42)")))
  }

  test("should not fail if a procedure declares a float but gets called with an integer") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTFloat)

    // Then
    a [CypherTypeException] shouldNot be(thrownBy(execute("CALL my.first.proc({param})", "param" -> 42)))
  }

  test("should fail if explicit argument is missing") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    an [SyntaxException] shouldBe thrownBy(execute("CALL my.first.proc('ten')"))
  }

  test("should fail if too many arguments") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    an [SyntaxException] shouldBe thrownBy(execute("CALL my.first.proc('ten', 10, 42)"))
  }

  test("should fail if implicit argument is missing") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    a [ParameterNotFoundException] shouldBe thrownBy(execute("CALL my.first.proc", "in0" -> "42", "in42" -> 42))
  }

  test("should fail if calling non-existent procedure") {
    a [CypherExecutionException] shouldBe thrownBy(execute("CALL no.such.thing.exists(42)"))
  }

  test("should be able to call a procedure with explain") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTNumber)

    // When
    val result = execute("EXPLAIN CALL my.first.proc(42)")

    // Then
    result shouldBe empty
  }
}
