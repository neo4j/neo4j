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

class VectorValueSemanticAnalysisTest extends CypherFunSuite with NameBasedSemanticAnalysisTestSuite {

  test("RETURN VECTOR([1, 2, 3, 4], 4, INTEGER64)") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  // Failing cases for invalid dimensions
  test("RETURN VECTOR([1, 2, 3], -1, INTEGER64)") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "dimension",
        "NUMBER",
        1,
        4096,
        "-1",
        s"Invalid input. '-1' is not a valid value. Must be a number in the range 1 to 4096.",
        p(25, 1, 26).withInputLength(2)
      )
    )
  }

  test("RETURN VECTOR([1, 2, 3], 5000, INTEGER64)") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError.specifiedNumberOutOfRange(
        "dimension",
        "NUMBER",
        1,
        4096,
        "5000",
        s"Invalid input. '5000' is not a valid value. Must be a number in the range 1 to 4096.",
        p(25, 1, 26).withInputLength(4)
      )
    )
  }

  // Failing cases for invalid type (note the 3rd argument is checked in parser tests)
  test("RETURN VECTOR(1, 3, INTEGER64)") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasError(
      GqlHelper.getGql22G03_22N27(
        "INTEGER",
        "argument at index 0 of function vector()",
        java.util.List.of("STRING", "LIST<INTEGER | FLOAT>"),
        14,
        1,
        15
      ),
      "Type mismatch: expected String, List<Float>, List<Integer> or List<Number> but was Integer",
      p(14, 1, 15).withInputLength(1)
    )
  }

  test("RETURN VECTOR([\"1\"], 1, INTEGER64)") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql22G03_22N27(
          "LIST<STRING>",
          "argument at index 0 of function vector()",
          java.util.List.of("STRING", "LIST<INTEGER | FLOAT>"),
          14,
          1,
          15
        ),
        "Type mismatch: expected String, List<Float>, List<Integer> or List<Number> but was List<String>",
        p(14, 1, 15)
      )
    )
  }

  test("RETURN VECTOR([1], \"1\", INTEGER64)") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql22G03_22N27(
          "STRING",
          "argument at index 1 of function vector()",
          java.util.List.of("INTEGER"),
          19,
          1,
          20
        ),
        "Type mismatch: expected Integer but was String",
        p(19, 1, 20).withInputLength(3)
      )
    )
  }

  // Vector types should not work with operators
  test("RETURN VECTOR([1, 2, 3], 3, INTEGER)[0]") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("LIST"),
          "VECTOR",
          7,
          1,
          8
        ),
        "Type mismatch: expected List<T> but was Vector",
        p(7, 1, 8)
      )
    )
  }

  test("RETURN VECTOR([1, 2, 3], 3, INTEGER)[0..2]") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("LIST"),
          "VECTOR",
          7,
          1,
          8
        ),
        "Type mismatch: expected List<T> but was Vector",
        p(7, 1, 8)
      )
    )
  }

  test("RETURN last(VECTOR([1, 2, 3], 3, INTEGER))") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("LIST"),
          "VECTOR",
          12,
          1,
          13
        ),
        "Type mismatch: expected List<T> but was Vector",
        p(12, 1, 13)
      )
    )
  }

  test("RETURN head(VECTOR([1, 2, 3], 3, INTEGER))") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("LIST"),
          "VECTOR",
          12,
          1,
          13
        ),
        "Type mismatch: expected List<T> but was Vector",
        p(12, 1, 13)
      )
    )
  }

  test("RETURN 4 + VECTOR([1, 2, 3], 3, INTEGER)") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("FLOAT", "INTEGER", "STRING", "LIST"),
          "VECTOR",
          11,
          1,
          12
        ),
        "Type mismatch: expected Float, Integer, String or List<T> but was Vector",
        p(11, 1, 12)
      )
    )
  }

  test("RETURN VECTOR([1, 2, 3], 3, INTEGER) + 4") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("STRING", "LIST"),
          "INTEGER",
          39,
          1,
          40
        ),
        "Type mismatch: expected String or List<T> but was Integer",
        p(39, 1, 40).withInputLength(1)
      )
    )
  }

  test("RETURN VECTOR([1, 2, 3], 3, INTEGER) || [4]") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("STRING", "LIST"),
          "VECTOR",
          7,
          1,
          8
        ),
        "Type mismatch: expected String or List<T> but was Vector",
        p(7, 1, 8)
      )
    )
  }

  test("RETURN 5 IN VECTOR([1, 2, 3], 3, INTEGER)") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("LIST"),
          "VECTOR",
          12,
          1,
          13
        ),
        "Type mismatch: expected List<T> but was Vector",
        p(12, 1, 13)
      )
    )
  }

  test("RETURN [ c IN VECTOR([1, 2, 3], 3, INTEGER) | c * -1 ]") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("LIST"),
          "VECTOR",
          14,
          1,
          15
        ),
        "Type mismatch: expected List<T> but was Vector",
        p(14, 1, 15)
      ),
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("FLOAT", "INTEGER", "DURATION"),
          "ANY",
          46,
          1,
          47
        ),
        "Type mismatch: expected Float, Integer or Duration but was Any",
        p(46, 1, 47)
      )
    )
  }

  test("RETURN sqrt(reduce( norm = 0.0, c IN VECTOR([1, 2, 3], 3, INTEGER) | norm + (c^2) ))") {
    runWith(disabledCypherVersions = Set(CypherVersion.Cypher5)).hasErrors(
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("LIST"),
          "VECTOR",
          37,
          1,
          38
        ),
        "Type mismatch: expected List<T> but was Vector",
        p(37, 1, 38)
      ),
      SemanticError(
        GqlHelper.getGql42001_22NB1(
          java.util.List.of("FLOAT"),
          "ANY",
          77,
          1,
          78
        ),
        "Type mismatch: expected Float but was Any",
        p(77, 1, 78)
      )
    )
  }

}
