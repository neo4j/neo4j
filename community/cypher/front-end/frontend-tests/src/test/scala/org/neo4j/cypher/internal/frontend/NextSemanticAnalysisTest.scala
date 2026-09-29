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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.p
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class NextSemanticAnalysisTest
    extends CypherFunSuite with NameBasedSemanticAnalysisTestSuite {

  def run(query: String): AnalysisAssertions = super.run(query, disabledVersions = Set(CypherVersion.Cypher5))

  test("Returning unaliased in NEXT") {
    val query = """RETURN 2
                  |
                  |NEXT
                  |
                  |RETURN 1
                  |""".stripMargin
    run(query).hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq()
      case CypherVersion.Cypher25 =>
        Seq(
          SemanticError.unaliasedReturnItem("RETURN followed by NEXT", p(7, 1, 8))
        )
    }
  }

  test("A NEXT part cannot be a procedure call with YIELD") {
    val query = """RETURN 1 AS a
                  |
                  |NEXT
                  |
                  |CALL proc() YIELD x
                  |
                  |NEXT
                  |
                  |RETURN 1 AS b
                  |""".stripMargin
    run(query).hasErrors(SemanticError.queryCannotConcludeWithCall("CALL", p(21, 5, 1)))
  }

  test("The last NEXT part cannot be a procedure call with YIELD") {
    val query = """RETURN 1 AS a
                  |
                  |NEXT
                  |
                  |CALL proc() YIELD x
                  |""".stripMargin
    run(query).hasErrors(SemanticError.queryCannotConcludeWithCall("CALL", p(21, 5, 1)))
  }

  test("A NEXT part can be a procedure call without YIELD") {
    val query = """RETURN 1 AS a
                  |
                  |NEXT
                  |
                  |CALL proc()
                  |
                  |NEXT
                  |
                  |RETURN 1 AS b
                  |""".stripMargin
    run(query).hasNoErrors
  }

  test("A NEXT part can contain a procedure call with YIELD followed by other clauses") {
    val query = """RETURN 1 AS a
                  |
                  |NEXT
                  |
                  |CALL proc() YIELD x
                  |RETURN x
                  |
                  |NEXT
                  |
                  |RETURN x AS b
                  |""".stripMargin
    run(query).hasNoErrors
  }
}
