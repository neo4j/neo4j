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

import org.neo4j.cypher._
import org.neo4j.kernel.api.proc.Neo4jTypes

class StandaloneProcedureCallAcceptanceTest extends ProcedureCallAcceptanceTest {

  test("should be able to find labels from built-in-procedure") {
    // Given
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    val result = execute("CALL db.labels")

    // Then
    result.toList should equal(
      List(
        Map("label" -> "A"),
        Map("label" -> "B"),
        Map("label" -> "C")))
  }

  test("should be able to call void procedure") {
    //Given
    registerVoidProcedure()

    //When
    val result = execute("CALL dbms.do_nothing()")

    // Then
    result.toList shouldBe empty
  }

  test("should be able to call void procedure without arguments") {
    //Given
    registerVoidProcedure()

    //When
    val result = execute("CALL dbms.do_nothing")

    // Then
    result.toList shouldBe empty
  }

  test("should be able to call empty procedure") {
    //Given
    registerProcedureReturningNoRowsOrColumns()

    //When
    val result = execute("CALL dbms.return_nothing()")

    // Then
    result.toList shouldBe empty
  }

  test("should be able to call empty procedure without arguments") {
    //Given
    registerProcedureReturningNoRowsOrColumns()

    //When
    val result = execute("CALL dbms.return_nothing")

    // Then
    result.toList shouldBe empty
  }

  test("db.labels work on an empty database") {
    // Given an empty database
    //When
    val result = execute("CALL db.labels")

    // Then
    result.toList shouldBe empty
  }

  test("db.labels should be empty when all labels are removed") {
    // Given
    createLabeledNode("A")
    execute("MATCH (a:A) REMOVE a:A")

    //When
    val result = execute("CALL db.labels")

    // Then
    result shouldBe empty
  }

  test("db.labels should be empty when all nodes are removed") {
    // Given
    createLabeledNode("A")
    execute("MATCH (a) DETACH DELETE a")

    //When
    val result = execute("CALL db.labels")

    // Then
    result shouldBe empty
  }

  test("should be able to find types from built-in-procedure") {
    // Given
    relate(createNode(), createNode(), "A")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "C")

    // When
    val result = execute("CALL db.relationshipTypes")

    // Then
    result.toList should equal(
      List(
        Map("relationshipType" -> "A"),
        Map("relationshipType" -> "B"),
        Map("relationshipType" -> "C")))
  }

  test("db.relationshipType work on an empty database") {
    // Given an empty database
    //When
    val result = execute("CALL db.relationshipTypes")

    // Then
    result shouldBe empty
  }

  test("db.relationshipTypes should be empty when all relationships are removed") {
    // Given
    // Given
    relate(createNode(), createNode(), "A")
    relate(createNode(), createNode(), "B")
    relate(createNode(), createNode(), "C")
    execute("MATCH (a) DETACH DELETE a")

    //When
    val result = execute("CALL db.relationshipTypes")

    // Then
    result shouldBe empty
  }

  test("should be able to find propertyKeys from built-in-procedure") {
    // Given
    createNode("A" -> 1, "B" -> 2, "C" -> 3)

    // When
    val result = execute("CALL db.propertyKeys")

    // Then
    result.toList should equal(
      List(
        Map("propertyKey" -> "A"),
        Map("propertyKey" -> "B"),
        Map("propertyKey" -> "C")))
  }

  test("db.propertyKeys works on an empty database") {
    // Given an empty database

    // When
    val result = execute("CALL db.propertyKeys")

    // Then
    result shouldBe empty
  }

  test("removing properties from nodes and relationships does not remove them from the store") {
    // Given
    relate(createNode("A" -> 1), createNode("B" -> 1), "R" ->1)
    execute("MATCH (a)-[r]-(b) REMOVE a.A, r.R, b.B")

    // When
    val result = execute("CALL db.propertyKeys")

    // Then
    result.toList should equal(
      List(
        Map("propertyKey" -> "A"),
        Map("propertyKey" -> "B"),
        Map("propertyKey" -> "R")))
  }

  test("removing all nodes and relationship does not remove properties from the store") {
    // Given
    relate(createNode("A" -> 1), createNode("B" -> 1), "R" ->1)
    execute("MATCH (a) DETACH DELETE a")

    // When
    val result = execute("CALL db.propertyKeys")

    // Then
    result.toList should equal(
      List(
        Map("propertyKey" -> "A"),
        Map("propertyKey" -> "B"),
        Map("propertyKey" -> "R")))
  }

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
    a [SyntaxException] shouldBe thrownBy(execute("CALL my.first.proc('ten')"))
  }

  test("should fail if too many arguments") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    a [SyntaxException] shouldBe thrownBy(execute("CALL my.first.proc('ten', 10, 42)"))
  }

  test("should fail if implicit argument is missing") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    a [ParameterNotFoundException] shouldBe thrownBy(execute("CALL my.first.proc", "in0" -> "42", "in42" -> 42))
  }

  test("should be able to call a procedure with explain") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTNumber)

    // When
    val result = execute("EXPLAIN CALL my.first.proc(42)")

    // Then
    result shouldBe empty
  }

  test("should fail if calling non-existent procedure") {
    a [CypherExecutionException] shouldBe thrownBy(execute("CALL no.such.thing.exists(42)"))
  }

  test("should be able to find indexes from built-in-procedure") {
    // Given
    graph.createIndex("A", "prop")

    //When
    val result = execute("CALL db.indexes")

    // Then
    result.toList should equal(
      List(Map("description" -> "INDEX ON :A(prop)", "state" -> "online", "type" -> "node_label_property")))
  }
}
