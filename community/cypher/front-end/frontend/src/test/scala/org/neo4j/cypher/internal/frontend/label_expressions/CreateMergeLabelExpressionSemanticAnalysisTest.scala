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
package org.neo4j.cypher.internal.frontend.label_expressions

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.frontend.SemanticAnalysisTestSuiteWithDefaultQuery
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.gqlstatus.GqlHelper

abstract class LabelExpressionSemanticAnalysisTestSuiteWithUpdateStatement(statement: UpdateStatement)
    extends SemanticAnalysisTestSuiteWithDefaultQuery
    with TestName {

  override def defaultQuery: String = s"$statement $testName"

  // Length of the query before the test name
  protected val offset = statement.asPrettyString.length + 1

  private val labelExprErrorMessage =
    s"Label expressions in patterns are not allowed in a $statement clause, but only in a MATCH clause and in expressions"

  test("(n:A:B)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n:A&B)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n:A|B)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      labelExprErrorMessage
    )
  }

  test("(n:A|:B)") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      labelExprErrorMessage,
      "Label expressions are not allowed to contain '|:'."
    )
  }

  test("(IS A)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n IS A&B)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n IS !(A&B))") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      labelExprErrorMessage
    )
  }

  test("(n IS A&!B)") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      labelExprErrorMessage
    )
  }

  test("(n IS A|B)") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      labelExprErrorMessage
    )
  }

  test("(n IS %)") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      labelExprErrorMessage
    )
  }

  test("(n IS A|:B)") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      labelExprErrorMessage,
      "Label expressions are not allowed to contain '|:'."
    )
  }

  test("(IS:IS)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n:A&B:C)") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B&C."
    )
  }

  test("(n IS A:B)") {
    // should not allow mixing colon as label conjunction symbol with IS keyword in label expression
    // Just checking the first error, since MERGE (being ReadWrite) reports the error twice, but CREATE only once.
    runSemanticAnalysis().errors.headOption shouldEqual Some(
      SemanticError(
        GqlHelper.getGql42001_42I29("IS A:B", "IS A&B", 1, offset + 8, offset + 7),
        "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&B.",
        InputPosition(offset + 7, 1, offset + 8)
      )
    )
  }

  test("(n IS A&B:C)") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression
    runSemanticAnalysis().errors.toSet shouldEqual Set(
      SemanticError(
        GqlHelper.getGql42001_42I29("IS (A&B):C", "IS A&B&C", 1, offset + 10, offset + 9),
        "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&B&C.",
        InputPosition(offset + 9, 1, offset + 10)
      )
    )
  }

  test("()-[:Rel1]->()") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("()-[:Rel1|Rel2]->()") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      s"A single relationship type must be specified for $statement"
    )
  }

  test("()-[:Rel1&Rel2]->()") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      s"Relationship type expressions in patterns are not allowed in a $statement clause, but only in a MATCH clause",
      s"A single relationship type must be specified for $statement"
    )
  }

  test("()-[:Rel1&!Rel2]->()") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      s"Relationship type expressions in patterns are not allowed in a $statement clause, but only in a MATCH clause",
      s"A single relationship type must be specified for $statement"
    )
  }

  test("()-[:!Rel1]->()") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      s"A single plain relationship type like `:Rel1` must be specified for $statement",
      s"Relationship type expressions in patterns are not allowed in a $statement clause, but only in a MATCH clause"
    )
  }

  test("()-[r]->()") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      s"Exactly one relationship type must be specified for $statement. Did you forget to prefix your relationship type with a ':'?"
    )
  }

  test("()-[r IS Rel1]->()") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n IS A)-[:REL]->()") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("()-[:REL]->(IS B)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("()-[IS Rel1|Rel2]->()") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      s"A single relationship type must be specified for $statement"
    )
  }

  test("()-[IS Rel1|:Rel2]->()") {
    // Just checking contains, since MERGE (being ReadWrite) reports the error twice, but CREATE only once.
    runSemanticAnalysis().errors.toSet should contain(SemanticError(
      GqlHelper.getGql42001_42I29("IS Rel1|:Rel2", "IS Rel1|Rel2", 1, offset + 12, offset + 11),
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS Rel1|Rel2.",
      InputPosition(offset + 11, 1, offset + 12)
    ))
  }

  test("()-[IS !Rel1]->()") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      s"A single plain relationship type like `:Rel1` must be specified for $statement",
      s"Relationship type expressions in patterns are not allowed in a $statement clause, but only in a MATCH clause"
    )
  }

  test("()-[IS:IS]->()") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n IS A)-[r IS R]->(m:B) RETURN *") {
    // Mixing colon (not as conjunction) and IS keyword should be allowed as they are both part of GQL
    runSemanticAnalysis().errors shouldBe empty
  }

  // Dynamic labels and types
  test("(n:$(\"label\"))") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n:A&B&$(\"label\"))") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n)-[:$(\"label\")]->()") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n:$(1))") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      "Type mismatch: expected String or List<String> but was Integer"
    )
  }

  test("(n)-[:$(1 + 3.0)]->()") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      "Type mismatch: expected String or List<String> but was Float"
    )
  }

  test("(n:$([1]))") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      "Type mismatch: expected String or List<String> but was List<Integer>"
    )
  }

  test("(n:$(['']))") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      "'' is not a valid token name. Token names cannot be empty or contain any null-bytes."
    )
  }

  test("(n:$([null]))") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      "Null is not a valid token name. Token names cannot be empty or contain any null-bytes."
    )
  }

  test("(n:$all(['Foo', 'Bar']))") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n:$any(['Foo', 'Bar']))") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      "Dynamic labels using `$any()` are not allowed in CREATE or MERGE."
    )
  }

  test("(n:$(['Foo', 'Bar']))") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n)-[:$any('Foo')]->()") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      "Dynamic types using `$any()` are not allowed in CREATE or MERGE."
    )
  }

  test("(n)-[:$(['Foo', 'Bar'])]->()") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      s"A single relationship type must be specified for $statement"
    )
  }

  test("(n)-[:$([])]->()") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      s"Exactly one relationship type must be specified for $statement. Did you forget to prefix your relationship type with a ':'?"
    )
  }
}

sealed trait UpdateStatement

object UpdateStatement {
  case object CREATE extends UpdateStatement
  case object MERGE extends UpdateStatement
}

class LabelExpressionInCreateSemanticAnalysisTest
    extends LabelExpressionSemanticAnalysisTestSuiteWithUpdateStatement(UpdateStatement.CREATE) {

  // These queries do not parse for MERGE

  test("(n:A:B), (m:A&B)") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("(n:A), (m IS B) RETURN *") {
    // Mixing colon (not as conjunction) and IS keyword should be allowed as they are both part of GQL
    runSemanticAnalysis().errors shouldBe empty
  }
}

class LabelExpressionInMergeSemanticAnalysisTest
    extends LabelExpressionSemanticAnalysisTestSuiteWithUpdateStatement(UpdateStatement.MERGE) {}
