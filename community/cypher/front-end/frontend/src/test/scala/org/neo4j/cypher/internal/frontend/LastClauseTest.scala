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

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LastClauseTest
    extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  def errorCanOnlyBeUsedAtTheEnd(clause: String, offset: Int, line: Int, column: Int): SemanticError =
    SemanticError(
      s"$clause can only be used at the end of the query.",
      InputPosition(offset, line, column)
    )

  def errorCannotConcludeWith(clause: String, offset: Int, line: Int, column: Int): SemanticError =
    SemanticError(
      s"Query cannot conclude with $clause (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      InputPosition(offset, line, column)
    )

  test("FINISH") {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""FINISH
         |RETURN 1""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1)
    )
  }

  test("""FINISH
         |MATCH (a)""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1),
      errorCannotConcludeWith("MATCH", 7, 2, 1)
    )
  }

  test("""FINISH
         |MATCH (a)
         |FINISH""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1)
    )
  }

  test("""FINISH
         |MATCH (a)
         |RETURN a""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1)
    )
  }

  test("""FINISH
         |CREATE (a)""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1)
    )
  }

  test("""FINISH
         |CREATE (a)
         |FINISH""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1)
    )
  }

  test("""FINISH
         |CREATE (a)
         |RETURN a""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1)
    )
  }

  test("RETURN 1") {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""RETURN 1
         |FINISH""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1)
    )
  }

  test("""RETURN 1
         |MATCH (a)""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1),
      errorCannotConcludeWith("MATCH", 9, 2, 1)
    )
  }

  test("""RETURN 1
         |MATCH (a)
         |FINISH""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1)
    )
  }

  test("""RETURN 1
         |MATCH (a)
         |RETURN a""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1)
    )
  }

  test("""RETURN 1
         |CREATE (a)""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1)
    )
  }

  test("""RETURN 1
         |CREATE (a)
         |FINISH""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1)
    )
  }

  test("""RETURN 1
         |CREATE (a)
         |RETURN a""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1)
    )
  }

  test("MATCH (a)") {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCannotConcludeWith("MATCH", 0, 1, 1)
    )
  }

  test("""MATCH (a)
         |FINISH
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (a)
         |RETURN a
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("WITH 1 AS a".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCannotConcludeWith("WITH", 0, 1, 1)
    )
  }

  test("""WITH 1 AS a
         |FINISH
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""WITH 1 AS a
         |RETURN a
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("UNWIND [1,2] AS a") {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCannotConcludeWith("UNWIND", 0, 1, 1)
    )
  }

  test("""UNWIND [1,2] AS a
         |FINISH
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""UNWIND [1,2] AS a
         |RETURN a
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("CREATE (a)") {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""CREATE (a)
         |FINISH
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""CREATE (a)
         |RETURN a
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (a)
         |SET a.p = 5
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (a)
         |SET a.p = 5
         |FINISH
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (a)
         |SET a.p = 5
         |RETURN a
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (a)
         |REMOVE a.p
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (a)
         |REMOVE a.p
         |FINISH
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (a)
         |REMOVE a.p
         |RETURN a
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (a)
         |CALL {
         |  MATCH (b)
         |}
         |RETURN a""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      errorCannotConcludeWith("MATCH", 19, 3, 3)
    )
  }

  test("""MATCH (a)
         |CALL {
         |  MATCH (b)
         |  FINISH
         |}
         |RETURN a
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""MATCH (a)
         |CALL {
         |  MATCH (b)
         |  RETURN b
         |}
         |RETURN a, b
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""  MATCH (a)
         |UNION
         |  MATCH (b)
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set(
      errorCannotConcludeWith("MATCH", 0, 1, 1),
      errorCannotConcludeWith("MATCH", 18, 3, 3)
    )
  }

  test("""  MATCH (a)
         |  FINISH
         |UNION
         |  MATCH (b)
         |  FINISH
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""  MATCH (a)
         |  RETURN 1
         |UNION
         |  MATCH (b)
         |  RETURN 1
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""  CREATE (a)
         |UNION
         |  CREATE (b)
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""  CREATE (a)
         |  FINISH
         |UNION
         |  CREATE (b)
         |  FINISH
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

  test("""  CREATE (a)
         |  RETURN 1
         |UNION
         |  CREATE (b)
         |  RETURN 1
         |""".stripMargin) {
    runSemanticAnalysis().errors.toSet shouldBe Set.empty
  }

}
