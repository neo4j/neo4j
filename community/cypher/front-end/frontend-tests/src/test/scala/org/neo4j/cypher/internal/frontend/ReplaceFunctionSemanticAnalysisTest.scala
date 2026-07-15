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
import org.neo4j.gqlstatus.GqlHelper

class ReplaceFunctionSemanticAnalysisTest extends CypherFunSuite with NameBasedSemanticAnalysisTestSuite {

  // Limit is only available in Cypher 25+
  test("RETURN replace(\"hello\", \"l\", \"w\", 2)") {
    run().hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq(SemanticError.invalidNumberOfProcedureOrFunctionArguments(
          3,
          4,
          "replace",
          "replace(original :: STRING, search :: STRING, replace :: STRING) :: STRING",
          s"Too many parameters for function 'replace'",
          p(7, 1, 8)
        ))
      case _ => Seq()
    }
  }

  test("RETURN replace(\"hello\", \"l\", \"w\")") {
    run().hasNoErrors
  }

  test("RETURN replace(\"hello\", \"l\", \"w\", 1)") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test("RETURN replace(\"hello\", \"l\", \"w\", 0)") {
    run(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  // Limit must be 0 or more
  test("RETURN replace(\"hello\", \"l\", \"w\", -1)") {
    run().hasSemanticErrorsIn {
      case CypherVersion.Cypher5 => Seq(SemanticError.invalidNumberOfProcedureOrFunctionArguments(
          3,
          4,
          "replace",
          "replace(original :: STRING, search :: STRING, replace :: STRING) :: STRING",
          s"Too many parameters for function 'replace'",
          p(7, 1, 8)
        ))
      case _ => Seq(
          SemanticError.specifiedNumberOutOfRange(
            "limit",
            "INTEGER",
            0,
            Long.MaxValue,
            "-1",
            "The limit needs to be greater than or equal to 0.",
            p(34, 1, 35).withInputLength(2)
          )
        )
    }
  }

  // Limit must be an integer
  test("RETURN replace(\"hello\", \"l\", \"w\", 1.0)") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasError(
      GqlHelper.getGql22G03_22N27(
        "FLOAT",
        "argument at index 3 of function replace()",
        java.util.List.of("INTEGER"),
        34,
        1,
        35
      ),
      "Type mismatch: expected Integer but was Float",
      p(34, 1, 35).withInputLength(3)
    )
  }
}
