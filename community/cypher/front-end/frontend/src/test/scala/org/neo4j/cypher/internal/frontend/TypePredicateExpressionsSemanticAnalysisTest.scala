/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class TypePredicateExpressionsSemanticAnalysisTest extends CypherFunSuite with NameBasedSemanticAnalysisTestSuite {

  test("Simple Closed Dynamic Union with different nullabilities should error") {
    val query =
      """RETURN 1 IS :: INTEGER | FLOAT NOT NULL
        |""".stripMargin

    val result = runSemanticAnalysis(query)

    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`", 1, 16)
    ))
  }

  test("Simple Closed Dynamic Union using ANY syntax with different nullabilities should error") {
    val query =
      """RETURN 1 IS :: ANY<INTEGER | FLOAT NOT NULL>
        |""".stripMargin

    val result = runSemanticAnalysis(query)

    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`", 1, 20)
    ))
  }

  test("Dynamic union within a list should fail with different nullabilities") {
    val query =
      """RETURN 1 IS :: LIST<ANY<INTEGER | FLOAT NOT NULL>>
        |""".stripMargin

    val result = runSemanticAnalysis(query)

    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`", 1, 25)
    ))
  }

  test("Deeper nested unions should be found and fail semantic check") {
    val query =
      """RETURN 1 IS :: LIST<ANY<INTEGER NOT NULL | FLOAT NOT NULL | LIST<ANY<INTEGER NOT NULL | FLOAT>> NOT NULL>>
        |""".stripMargin

    val result = runSemanticAnalysis(query)

    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`", 1, 70)
    ))
  }

  test("Nested unions should be uncovered during normalization and fail semantic check") {
    val query =
      """RETURN 1 IS :: ANY<ANY<BOOL | BOOLEAN | INT NOT NULL> | ANY<ANY<BOOL NOT NULL>>>
        |""".stripMargin

    val result = runSemanticAnalysis(query)

    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`", 1, 20)
    ))
  }

  test("Nested unions which are okay by themselves should be uncovered during normalization and fail semantic check") {
    val query = "RETURN 1 IS :: ANY<ANY<BOOL | BOOLEAN | INT> | ANY<ANY<BOOL NOT NULL>>>"

    val result = runSemanticAnalysis(query)

    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`", 1, 20)
    ))
  }

  test("ANY<BOOLEAN NOT NULL | BOOLEAN> is not allowed") {
    val query = "RETURN 1 IS :: ANY<BOOLEAN NOT NULL | BOOLEAN>"

    val result = runSemanticAnalysis(query)

    result.errors.map(e => (e.msg, e.position.line, e.position.column)) should equal(List(
      ("All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`", 1, 20)
    ))
  }

  test("ANY<INTEGER | BOOLEAN> should be allowed") {
    val query = "RETURN 1 IS :: ANY<INTEGER | BOOLEAN>"

    val result = runSemanticAnalysis(query)

    result.errors shouldBe empty
  }

  test("ANY<INTEGER NOT NULL | BOOLEAN NOT NULL> should be allowed") {
    val query = "RETURN 1 IS :: ANY<INTEGER NOT NULL | BOOLEAN NOT NULL>"

    val result = runSemanticAnalysis(query)

    result.errors shouldBe empty
  }

  test("ANY<NOTHING | BOOLEAN> should be allowed") {
    val query = "RETURN 1 IS :: ANY<NOTHING | BOOLEAN>"

    val result = runSemanticAnalysis(query)

    result.errors shouldBe empty
  }

  test("ANY<NOTHING NOT NULL | BOOLEAN> should be allowed as it is the same as ANY<NOTHING | BOOLEAN>") {
    val query = "RETURN 1 IS :: ANY<NOTHING NOT NULL | BOOLEAN>"

    val result = runSemanticAnalysis(query)

    result.errors shouldBe empty
  }

  test("ANY<NULL NOT NULL | BOOLEAN> should be allowed as it is the same as ANY<NOTHING | BOOLEAN>") {
    val query = "RETURN 1 IS :: ANY<NULL NOT NULL | BOOLEAN>"

    val result = runSemanticAnalysis(query)

    result.errors shouldBe empty
  }

}
