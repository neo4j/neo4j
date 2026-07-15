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
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.GqlHelper

class WithClauseSemanticAnalysisTest
    extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  /*
   * positive tests
   */
  val validLets = Seq(
    "MATCH (n) WITH n RETURN *",
    "MATCH (n) WITH n.prop AS p RETURN *",
    "CREATE (n) WITH n RETURN *",
    "CREATE (n) WITH n.prop AS p RETURN *",
    "MERGE (n) WITH n RETURN *",
    "MERGE (n) WITH n.prop AS p RETURN *",
    "CALL { MATCH (n) RETURN n } WITH n RETURN *",
    "CALL { MATCH (n) RETURN n } WITH n.prop AS p RETURN *",
    "UNWIND [1, 2, 3] AS i WITH i, i AS j RETURN *"
  )

  for {
    validLet <- validLets
  } {
    test(validLet) {
      run().hasNoErrors
    }
  }

  /*
   * negative tests
   */
  private def hasErrors(expected: SemanticError*): Unit = {
    run().hasErrors(expected: _*)
  }

  /*
   * negative tests: Variable not defined
   */
  for {
    (proj, offset) <- Seq(
      ("a AS x", 0),
      ("1 AS x, a AS y", 8),
      ("1 AS a, a AS x", 8),
      ("a AS x, 1 AS a", 0),
      ("1 AS x, a AS a", 8),
      ("1 AS x, ceil(sqrt(a)) % 2 = 0 AS y, 'abc' AS z", 18)
    )
  } {
    test(s"WITH $proj RETURN *") {
      val position = p(5 + offset, 1, 6 + offset)
      hasErrors(SemanticError(
        GqlHelper.getGql42001_42N62("a", position.offset, position.line, position.column),
        "Variable `a` not defined",
        position
      ))
    }
  }

  test("Importing WITH with subquery expression") {
    for {
      reference <- Seq("()", "(n)")
      expression <- Seq(s"EXISTS { MATCH $reference }", s"[p = $reference--() | p]")
    } {
      val query =
        s"""MATCH (n)
           |CALL {
           |  WITH *, $expression AS x
           |  RETURN x
           |} RETURN x
           |""".stripMargin
      withClue(query) {
        run(query).hasErrors(
          SemanticError.invalidImportingWithAliasOrExpression(s"$expression AS x", p(19, 3, 3))
        )
      }
    }
  }
}
