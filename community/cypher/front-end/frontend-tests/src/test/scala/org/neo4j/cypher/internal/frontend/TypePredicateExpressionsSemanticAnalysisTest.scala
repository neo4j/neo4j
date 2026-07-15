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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.p
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.GqlHelper

class TypePredicateExpressionsSemanticAnalysisTest extends CypherFunSuite with NameBasedSemanticAnalysisTestSuite {

  test("Simple Closed Dynamic Union with different nullabilities should error") {
    val query =
      """RETURN 1 IS :: INTEGER | FLOAT NOT NULL
        |""".stripMargin

    run(query).hasError(
      GqlHelper.getGql42001_42N63(15, 1, 16),
      "All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`",
      p(15, 1, 16)
    )
  }

  test("Simple Closed Dynamic Union using ANY syntax with different nullabilities should error") {
    val query =
      """RETURN 1 IS :: ANY<INTEGER | FLOAT NOT NULL>
        |""".stripMargin

    run(query).hasError(
      GqlHelper.getGql42001_42N63(19, 1, 20),
      "All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`",
      p(19, 1, 20)
    )
  }

  test("Dynamic union within a list should fail with different nullabilities") {
    val query =
      """RETURN 1 IS :: LIST<ANY<INTEGER | FLOAT NOT NULL>>
        |""".stripMargin

    run(query).hasError(
      GqlHelper.getGql42001_42N63(24, 1, 25),
      "All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`",
      p(24, 1, 25)
    )
  }

  test("Deeper nested unions should be found and fail semantic check") {
    val query =
      """RETURN 1 IS :: LIST<ANY<INTEGER NOT NULL | FLOAT NOT NULL | LIST<ANY<INTEGER NOT NULL | FLOAT>> NOT NULL>>
        |""".stripMargin

    run(query).hasError(
      GqlHelper.getGql42001_42N63(69, 1, 70),
      "All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`",
      p(69, 1, 70)
    )
  }

  test("Nested unions should be uncovered during normalization and fail semantic check") {
    val query =
      """RETURN 1 IS :: ANY<ANY<BOOL | BOOLEAN | INT NOT NULL> | ANY<ANY<BOOL NOT NULL>>>
        |""".stripMargin

    run(query).hasError(
      GqlHelper.getGql42001_42N63(19, 1, 20),
      "All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`",
      p(19, 1, 20)
    )
  }

  test("Nested unions which are okay by themselves should be uncovered during normalization and fail semantic check") {
    val query = "RETURN 1 IS :: ANY<ANY<BOOL | BOOLEAN | INT> | ANY<ANY<BOOL NOT NULL>>>"

    run(query).hasError(
      GqlHelper.getGql42001_42N63(19, 1, 20),
      "All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`",
      p(19, 1, 20)
    )
  }

  test("ANY<BOOLEAN NOT NULL | BOOLEAN> is not allowed") {
    val query = "RETURN 1 IS :: ANY<BOOLEAN NOT NULL | BOOLEAN>"

    run(query).hasError(
      GqlHelper.getGql42001_42N63(19, 1, 20),
      "All types in a Closed Dynamic Union must be nullable, or be appended with `NOT NULL`",
      p(19, 1, 20)
    )
  }

  test("ANY<INTEGER | BOOLEAN> should be allowed") {
    run("RETURN 1 IS :: ANY<INTEGER | BOOLEAN>").hasNoErrors
  }

  test("ANY<INTEGER NOT NULL | BOOLEAN NOT NULL> should be allowed") {
    run("RETURN 1 IS :: ANY<INTEGER NOT NULL | BOOLEAN NOT NULL>").hasNoErrors
  }

  test("ANY<NOTHING | BOOLEAN> should be allowed") {
    run("RETURN 1 IS :: ANY<NOTHING | BOOLEAN>").hasNoErrors
  }

  test("ANY<NOTHING NOT NULL | BOOLEAN> should be allowed as it is the same as ANY<NOTHING | BOOLEAN>") {
    run("RETURN 1 IS :: ANY<NOTHING NOT NULL | BOOLEAN>").hasNoErrors
  }

  test("ANY<NULL NOT NULL | BOOLEAN> should be allowed as it is the same as ANY<NOTHING | BOOLEAN>") {
    run("RETURN 1 IS :: ANY<NULL NOT NULL | BOOLEAN>").hasNoErrors
  }
}
