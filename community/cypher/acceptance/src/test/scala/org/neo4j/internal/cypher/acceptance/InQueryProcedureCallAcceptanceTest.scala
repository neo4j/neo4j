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

import java.util

import org.neo4j.cypher._
import org.neo4j.kernel.api.proc.Neo4jTypes

class InQueryProcedureCallAcceptanceTest extends ProcedureCallAcceptanceTest {

  test("should be able to find labels from built-in-procedure") {
    // Given
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    val result = execute("CALL db.labels() YIELD label RETURN *")

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
    val result = execute("MATCH (n {name: 'Toc'}) WITH n.name AS name CALL db.labels() YIELD label RETURN *")

    // Then
    result.toList should equal(
      List(
        Map("name" -> "Toc", "label" -> "A"),
        Map("name" -> "Toc", "label" -> "B"),
        Map("name" -> "Toc", "label" -> "C")))
  }

  test("db.labels works on an empty database") {
    // Given an empty database
    //When
    val result = execute("CALL db.labels() YIELD label RETURN *")

    // Then
    result.toList shouldBe empty
  }

  test("should be able to call procedure with explicit arguments") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // When
    val result = execute("CALL my.first.proc('42', 42) YIELD out0, out1 RETURN *")

    // Then
    result.toList should equal(List(Map("out0" -> "42", "out1" -> 42)))
  }

  test("should be able to call the same procedure twice even using the same outputs") {
    // Given
    createLabeledNode("A", "B", "C")

    // When
    val result = execute(
      """call db.labels() yield label
        |with count(*) as c
        |call db.labels() yield label
        |return *""".stripMargin)

    // Then
    result.toSet should equal(Set(
      Map("label" -> "A", "c" -> 3),
      Map("label" -> "B", "c" -> 3),
      Map("label" -> "C", "c" -> 3)
    ))
  }

  test("should be able to call empty procedure") {
    //Given
    registerProcedureReturningNoRowsOrColumns()

    //When
    val result = execute("CALL dbms.return_nothing() RETURN 1")

    // Then
    result.toList shouldBe empty
  }

  test("should be able to call void procedure") {
    //Given
    registerVoidProcedure()

    //When
    val result = execute("MATCH (n) CALL dbms.do_nothing() RETURN n")

    // Then
    result.toList shouldBe empty
  }

  test("should be able to call void procedure without swallowing rows") {
    //Given
    registerVoidProcedure()

    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    val result = execute("MATCH (n) CALL dbms.do_nothing() RETURN n")

    // Then
    result.toList.size shouldBe 3
  }

  test("should fail to shadow already bound identifier from a built-in-procedure") {
    // Given
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    //When
    a [SyntaxException] shouldBe thrownBy(
      execute("WITH 'Hi' AS label CALL db.labels YIELD label RETURN *")
    )
  }

  test("should fail if input type is wrong") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTNumber)

    // Then
    a [SyntaxException] shouldBe thrownBy(execute("CALL my.first.proc('ten') YIELD x RETURN x"))
  }

  test("if signature declares number all number types are valid") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTNumber)

    // Then
    execute("CALL my.first.proc(42) YIELD out0 AS x RETURN *").toList should equal(List(Map("x" -> 42)))
    execute("CALL my.first.proc(42.3) YIELD out0 AS x RETURN *").toList should equal(List(Map("x" -> 42.3)))
  }

  test("arguments are nullable") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTNumber)

    // Then
    execute("CALL my.first.proc(NULL) YIELD out0 AS x RETURN *").toList should equal(List(Map("x" -> null)))
  }

  test("should not fail if a procedure declares a float but gets an integer") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTFloat)

    // Then
    a [CypherTypeException] shouldNot be(thrownBy(execute("CALL my.first.proc(42) YIELD out0 RETURN *")))
  }

  test("should not fail if a procedure declares a float but gets called with an integer") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTFloat)

    // Then
    a [CypherTypeException] shouldNot be(thrownBy(execute("CALL my.first.proc({param}) YIELD out0 RETURN *", "param" -> 42)))
  }

  test("should fail if too many arguments") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTString, Neo4jTypes.NTNumber)

    // Then
    an [SyntaxException] shouldBe thrownBy(execute("CALL my.first.proc('ten', 10, 42) YIELD x, y, z RETURN *"))
  }

  test("should be able to call a procedure with explain") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTNumber)

    // When
    val result = execute("EXPLAIN CALL my.first.proc(42) YIELD out0 RETURN *")

    // Then
    result shouldBe empty
  }

  test("should fail when using aggregating function as argument") {
    // Given
    registerDummyInOutProcedure(Neo4jTypes.NTNumber)

    // Then
    a [SyntaxException] shouldBe thrownBy(execute("MATCH (n) CALL my.first.proc(count(n)) YIELD out0 RETURN out0"))
  }

  test("should fail if calling non-existent procedure") {
    a [CypherExecutionException] shouldBe thrownBy(execute("CALL no.such.thing.exists(42) YIELD x RETURN *"))
  }


  test("should fail if arguments are missing when calling procedure in a query") {
    a [SyntaxException] shouldBe thrownBy(execute("CALL db.labels YIELD label RETURN *"))
  }

  test("should fail if outputs are missing when calling procedure in a query") {
    a [SyntaxException] shouldBe thrownBy(execute("CALL db.labels() RETURN *"))
  }

  test("should fail if outputs and arguments are missing when calling procedure in a query") {
    a [SyntaxException] shouldBe thrownBy(execute("CALL db.labels RETURN *"))
  }

  test("should fail if calling procedure via rule planner") {
    an [InternalException] shouldBe thrownBy(execute(
      "CYPHER planner=rule CALL db.labels() YIELD label RETURN *"
    ))
  }

  test("should return correctly typed map result (even if converting to and from scala representation internally)") {
    val value = new util.HashMap[String, Any]()
    value.put("name", "Cypher")
    value.put("level", 9001)

    registerProcedureReturningSingleValue(value)

    // Using graph execute to get a Java value
    graph.execute("CALL my.first.value() YIELD out RETURN * LIMIT 1").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("out", value)
    ))
  }

  test("should return correctly typed list result (even if converting to and from scala representation internally)") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")

    registerProcedureReturningSingleValue(value)

    // Using graph execute to get a Java value
    graph.execute("CALL my.first.value() YIELD out RETURN * LIMIT 1").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("out", value)
    ))
  }

  test("should return correctly typed stream result (even if converting to and from scala representation internally)") {
    val value = new util.ArrayList[Any]()
    value.add("Norris")
    value.add("Strange")
    val stream = value.stream()

    registerProcedureReturningSingleValue(stream)

    // Using graph execute to get a Java value
    graph.execute("CALL my.first.value() YIELD out RETURN * LIMIT 1").stream().toArray.toList should equal(List(
      java.util.Collections.singletonMap("out", stream)
    ))
  }
}
