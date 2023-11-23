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

import org.neo4j.cypher.internal.frontend.SemanticAnalysisTestSuiteWithDefaultQuery
import org.neo4j.cypher.internal.util.test_helpers.TestName

abstract class LabelExpressionSemanticAnalysisTestSuiteWithUpdateStatement(statement: UpdateStatement)
    extends SemanticAnalysisTestSuiteWithDefaultQuery
    with TestName {

  override def defaultQuery: String = s"$statement $testName"

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
    runSemanticAnalysis().errorMessages.headOption shouldEqual Some(
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&B."
    )
  }

  test("(n IS A&B:C)") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&B&C."
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
    runSemanticAnalysis().errorMessages should contain
    s"A single relationship type must be specified for $statement"
  }

  test("()-[:Rel1&!Rel2]->()") {
    runSemanticAnalysis().errorMessages should contain
    s"A single relationship type must be specified for $statement"
  }

  test("()-[:!Rel1]->()") {
    runSemanticAnalysis().errorMessages should contain
    s"A single plain relationship type like `:Rel1` must be specified for $statement"
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
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      s"A single relationship type must be specified for $statement",
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS Rel1|Rel2."
    )
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
