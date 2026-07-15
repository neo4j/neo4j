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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N71
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.util.Failure
import scala.util.Success

class LastClauseTest
    extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  def errorCanOnlyBeUsedAtTheEnd(clause: String, offset: Int, line: Int, column: Int): SemanticError = {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
      .atPosition(offset, line, column)
      .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I38)
        .withParam(GqlParams.StringParam.clause, clause)
        .atPosition(offset, line, column)
        .build())
      .build()

    SemanticError(
      gql,
      s"$clause can only be used at the end of the query.",
      InputPosition(offset, line, column)
    )
  }

  def errorCannotConcludeWith(clause: String, offset: Int, line: Int, column: Int): SemanticError =
    SemanticError(
      getGql42001_42N71(offset, line, column),
      s"Query cannot conclude with $clause (must be a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD).",
      InputPosition(offset, line, column)
    )

  test("FINISH") {
    run().hasNoErrors
  }

  test("""FINISH
         |RETURN 1""".stripMargin) {
    run().hasErrors(errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1))
  }

  test("""FINISH
         |MATCH (a)""".stripMargin) {
    run().hasErrors(
      errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1),
      errorCannotConcludeWith("MATCH", 7, 2, 1)
    )
  }

  test("""FINISH
         |MATCH (a)
         |FINISH""".stripMargin) {
    run().hasErrors(
      errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1)
    )
  }

  test("""FINISH
         |MATCH (a)
         |RETURN a""".stripMargin) {
    run().hasErrors(errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1))
  }

  test("""FINISH
         |CREATE (a)""".stripMargin) {
    run().hasErrors(errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1))
  }

  test("""FINISH
         |CREATE (a)
         |FINISH""".stripMargin) {
    run().hasErrors(errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1))
  }

  test("""FINISH
         |CREATE (a)
         |RETURN a""".stripMargin) {
    run().hasErrors(errorCanOnlyBeUsedAtTheEnd("FINISH", 0, 1, 1))
  }

  test("RETURN 1") {
    run().hasNoErrors
  }

  test("""RETURN 1
         |FINISH""".stripMargin) {
    run().hasErrors(errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1))
  }

  test("""RETURN 1
         |MATCH (a)""".stripMargin) {
    run().hasErrors(
      errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1),
      errorCannotConcludeWith("MATCH", 9, 2, 1)
    )
  }

  test("""RETURN 1
         |MATCH (a)
         |FINISH""".stripMargin) {
    run().hasErrors(errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1))
  }

  test("""RETURN 1
         |MATCH (a)
         |RETURN a""".stripMargin) {
    run().hasErrors(errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1))
  }

  test("""RETURN 1
         |CREATE (a)""".stripMargin) {
    run().hasErrors(errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1))
  }

  test("""RETURN 1
         |CREATE (a)
         |FINISH""".stripMargin) {
    run().hasErrors(errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1))
  }

  test("""RETURN 1
         |CREATE (a)
         |RETURN a""".stripMargin) {
    run().hasErrors(errorCanOnlyBeUsedAtTheEnd("RETURN", 0, 1, 1))
  }

  test("MATCH (a)") {
    run().hasErrors(errorCannotConcludeWith("MATCH", 0, 1, 1))
  }

  test("""MATCH (a)
         |FINISH
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |RETURN a
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("WITH 1 AS a".stripMargin) {
    run().hasErrors(errorCannotConcludeWith("WITH", 0, 1, 1))
  }

  test("""WITH 1 AS a
         |FINISH
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""WITH 1 AS a
         |RETURN a
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("UNWIND [1,2] AS a") {
    run().hasErrors(errorCannotConcludeWith("UNWIND", 0, 1, 1))
  }

  test("""UNWIND [1,2] AS a
         |FINISH
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""UNWIND [1,2] AS a
         |RETURN a
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |FILTER true
         |""".stripMargin) {
    run().assertTryIn {
      case CypherVersion.Cypher5 => {
        case Success(_) => fail(new Exception("FILTER is not part of Cypher 5 syntax"))
        case Failure(_) => ()
      }
      case _ => {
        case Success(result) =>
          result.errors should contain theSameElementsAs Seq(errorCannotConcludeWith("FILTER", 10, 2, 1))
        case Failure(t) => fail(t)
      }
    }
  }

  test("""MATCH (a)
         |FILTER true
         |FINISH
         |""".stripMargin) {
    run().assertTryIn {
      case CypherVersion.Cypher5 => {
        case Success(_) => fail(new Exception("FILTER is not part of Cypher 5 syntax"))
        case Failure(_) => ()
      }
      case _ => {
        case Success(result) => result.errors shouldBe empty
        case Failure(t)      => fail(t)
      }
    }
  }

  test("""MATCH (a)
         |FILTER true
         |RETURN a
         |""".stripMargin) {
    run().assertTryIn {
      case CypherVersion.Cypher5 => {
        case Success(_) => fail(new Exception("FILTER is not part of Cypher 5 syntax"))
        case Failure(_) => ()
      }
      case _ => {
        case Success(result) => result.errors shouldBe empty
        case Failure(t)      => fail(t)
      }
    }
  }

  test("LET a = 123".stripMargin) {
    run().assertTryIn {
      case CypherVersion.Cypher5 => {
        case Success(_) => fail(new Exception("LET is not part of Cypher 5 syntax"))
        case Failure(_) => ()
      }
      case _ => {
        case Success(result) =>
          result.errors should contain theSameElementsAs Seq(errorCannotConcludeWith("LET", 0, 1, 1))
        case Failure(t) => fail(t)
      }
    }
  }

  test("""LET a = 123
         |FINISH
         |""".stripMargin) {
    run().assertTryIn {
      case CypherVersion.Cypher5 => {
        case Success(_) => fail(new Exception("LET is not part of Cypher 5 syntax"))
        case Failure(_) => ()
      }
      case _ => {
        case Success(result) => result.errors shouldBe empty
        case Failure(t)      => fail(t)
      }
    }
  }

  test("""LET a = 123
         |RETURN a
         |""".stripMargin) {
    run().assertTryIn {
      case CypherVersion.Cypher5 => {
        case Success(_) => fail(new Exception("LET is not part of Cypher 5 syntax"))
        case Failure(_) => ()
      }
      case _ => {
        case Success(result) => result.errors shouldBe empty
        case Failure(t)      => fail(t)
      }
    }
  }

  test("CREATE (a)") {
    run().hasNoErrors
  }

  test("""CREATE (a)
         |FINISH
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""CREATE (a)
         |RETURN a
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |SET a.p = 5
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |SET a.p = 5
         |FINISH
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |SET a.p = 5
         |RETURN a
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |REMOVE a.p
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |REMOVE a.p
         |FINISH
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |REMOVE a.p
         |RETURN a
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |CALL {
         |  MATCH (b)
         |}
         |RETURN a""".stripMargin) {
    run().hasErrors(errorCannotConcludeWith("MATCH", 19, 3, 3))
  }

  test("""MATCH (a)
         |CALL {
         |  MATCH (b)
         |  FINISH
         |}
         |RETURN a
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""MATCH (a)
         |CALL {
         |  MATCH (b)
         |  RETURN b
         |}
         |RETURN a, b
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""  MATCH (a)
         |UNION
         |  MATCH (b)
         |""".stripMargin) {
    run().hasErrors(
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
    run().hasNoErrors
  }

  test("""  MATCH (a)
         |  RETURN 1
         |UNION
         |  MATCH (b)
         |  RETURN 1
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""  CREATE (a)
         |UNION
         |  CREATE (b)
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""  CREATE (a)
         |  FINISH
         |UNION
         |  CREATE (b)
         |  FINISH
         |""".stripMargin) {
    run().hasNoErrors
  }

  test("""  CREATE (a)
         |  RETURN 1
         |UNION
         |  CREATE (b)
         |  RETURN 1
         |""".stripMargin) {
    run().hasNoErrors
  }
}
