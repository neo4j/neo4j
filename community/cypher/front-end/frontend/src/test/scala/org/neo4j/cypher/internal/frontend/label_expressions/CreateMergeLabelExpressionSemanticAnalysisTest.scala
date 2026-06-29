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
import org.scalatest.LoneElement

abstract class LabelExpressionSemanticAnalysisTestSuiteWithUpdateStatement(statement: UpdateStatement)
    extends SemanticAnalysisTestSuiteWithDefaultQuery
    with LoneElement
    with TestName {

  override def defaultQuery: String = s"$statement $testName"

  // Length of the query before the test name
  protected val offset = statement.toString.length + 1

  private val labelExprErrorMessage =
    s"Label expressions in patterns are not allowed in a $statement clause, but only in a MATCH clause and in expressions"

  test("(n:A:B)") {
    run().hasNoErrors
  }

  test("(n:A&B)") {
    run().hasNoErrors
  }

  test("(n:A|B)") {
    run().hasError(
      GqlHelper.getGql42001_42I30(s"a $statement clause", offset + 4, 1, offset + 5),
      labelExprErrorMessage,
      InputPosition(offset + 4, 1, offset + 5)
    )
  }

  test("(n:A|:B)") {
    run().hasErrors(
      GqlHelper.getGql42001_42I30(s"a $statement clause", offset + 4, 1, offset + 5),
      labelExprErrorMessage,
      InputPosition(offset + 4, 1, offset + 5),
      GqlHelper.getGql42001_42I20("|:", "|", offset + 4, 1, offset + 5),
      "Label expressions are not allowed to contain '|:'.",
      InputPosition(offset + 4, 1, offset + 5)
    )
  }

  test("(IS A)") {
    run().hasNoErrors
  }

  test("(n IS A&B)") {
    run().hasNoErrors
  }

  test("(n IS !(A&B))") {
    run().hasError(
      GqlHelper.getGql42001_42I30(s"a $statement clause", offset + 6, 1, offset + 7),
      labelExprErrorMessage,
      InputPosition(offset + 6, 1, offset + 7)
    )
  }

  test("(n IS A&!B)") {
    run().hasError(
      GqlHelper.getGql42001_42I30(s"a $statement clause", offset + 7, 1, offset + 8),
      labelExprErrorMessage,
      InputPosition(offset + 7, 1, offset + 8)
    )
  }

  test("(n IS A|B)") {
    run().hasError(
      GqlHelper.getGql42001_42I30(s"a $statement clause", offset + 7, 1, offset + 8),
      labelExprErrorMessage,
      InputPosition(offset + 7, 1, offset + 8)
    )
  }

  test("(n IS %)") {
    run().hasError(
      GqlHelper.getGql42001_42I30(s"a $statement clause", offset + 6, 1, offset + 7),
      labelExprErrorMessage,
      InputPosition(offset + 6, 1, offset + 7)
    )
  }

  test("(n IS A|:B)") {
    run().hasErrors(
      GqlHelper.getGql42001_42I30(s"a $statement clause", offset + 7, 1, offset + 8),
      labelExprErrorMessage,
      InputPosition(offset + 7, 1, offset + 8),
      GqlHelper.getGql42001_42I20("|:", "|", offset + 7, 1, offset + 8),
      "Label expressions are not allowed to contain '|:'.",
      InputPosition(offset + 7, 1, offset + 8)
    )
  }

  test("(IS:IS)") {
    run().hasNoErrors
  }

  test("(n:A&B:C)") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B&C", offset + 6, 1, offset + 7),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B&C.",
      InputPosition(offset + 6, 1, offset + 7)
    )
  }

  test("(n IS A:B)") {
    // should not allow mixing colon as label conjunction symbol with IS keyword in label expression
    // Just checking the first error, since MERGE (being ReadWrite) reports the error twice, but CREATE only once.
    run().assert(
      _.errors.headOption.get shouldBe
        SemanticError(
          GqlHelper.getGql42001_42I29("IS A:B", "IS A&B", offset + 7, 1, offset + 8),
          "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&B.",
          InputPosition(offset + 7, 1, offset + 8)
        )
    )
  }

  test("(n IS A&B:C)") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression
    run().hasError(
      GqlHelper.getGql42001_42I29("IS (A&B):C", "IS A&B&C", offset + 9, 1, offset + 10),
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&B&C.",
      InputPosition(offset + 9, 1, offset + 10)
    )
  }

  test("()-[:Rel1]->()") {
    run().hasNoErrors
  }

  test("()-[:Rel1|Rel2]->()") {
    run().hasErrorMessages(s"A single relationship type must be specified for $statement")
  }

  test("()-[:Rel1&Rel2]->()") {
    run().hasErrors(
      GqlHelper.getGql42001_42I35(offset + 9, 1, offset + 10),
      s"Relationship type expressions in patterns are not allowed in a $statement clause, but only in a MATCH clause",
      InputPosition(offset + 9, 1, offset + 10), // Position of the rel type expression
      GqlHelper.getGql42001_42I14(statement.toString, offset + 2, 1, offset + 3),
      s"A single relationship type must be specified for $statement",
      InputPosition(offset + 2, 1, offset + 3) // Position of the relationship
    )
  }

  test("()-[:Rel1&!Rel2]->()") {
    run().hasErrors(
      GqlHelper.getGql42001_42I35(offset + 9, 1, offset + 10),
      s"Relationship type expressions in patterns are not allowed in a $statement clause, but only in a MATCH clause",
      InputPosition(offset + 9, 1, offset + 10), // Position of the rel type expression
      GqlHelper.getGql42001_42I14(statement.toString, offset + 2, 1, offset + 3),
      s"A single relationship type must be specified for $statement",
      InputPosition(offset + 2, 1, offset + 3) // Position of the relationship
    )
  }

  test("()-[:!Rel1]->()") {
    run().hasErrors(
      GqlHelper.getGql42001_42I14(statement.toString, offset + 2, 1, offset + 3),
      s"A single plain relationship type like `:Rel1` must be specified for $statement",
      InputPosition(offset + 2, 1, offset + 3), // Position of the relationship,
      GqlHelper.getGql42001_42I35(offset + 5, 1, offset + 6),
      s"Relationship type expressions in patterns are not allowed in a $statement clause, but only in a MATCH clause",
      InputPosition(offset + 5, 1, offset + 6) // Position of the rel type expression
    )
  }

  test("()-[r]->()") {
    run().hasErrorMessages(
      s"Exactly one relationship type must be specified for $statement. Did you forget to prefix your relationship type with a ':'?"
    )
  }

  test("()-[r IS Rel1]->()") {
    run().hasNoErrors
  }

  test("(n IS A)-[:REL]->()") {
    run().hasNoErrors
  }

  test("()-[:REL]->(IS B)") {
    run().hasNoErrors
  }

  test("()-[IS Rel1|Rel2]->()") {
    run().hasErrorMessages(
      s"A single relationship type must be specified for $statement"
    )
  }

  test("()-[IS Rel1|:Rel2]->()") {
    // Just checking contains, since MERGE (being ReadWrite) reports the error twice, but CREATE only once.
    run().assert(
      _.errors should contain(SemanticError(
        GqlHelper.getGql42001_42I29("IS Rel1|:Rel2", "IS Rel1|Rel2", offset + 11, 1, offset + 12),
        "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS Rel1|Rel2.",
        InputPosition(offset + 11, 1, offset + 12)
      ))
    )
  }

  test("()-[IS !Rel1]->()") {
    run().hasErrors(
      GqlHelper.getGql42001_42I14(statement.toString, offset + 2, 1, offset + 3),
      s"A single plain relationship type like `:Rel1` must be specified for $statement",
      InputPosition(offset + 2, 1, offset + 3), // Position of the relationship
      GqlHelper.getGql42001_42I35(offset + 7, 1, offset + 8),
      s"Relationship type expressions in patterns are not allowed in a $statement clause, but only in a MATCH clause",
      InputPosition(offset + 7, 1, offset + 8) // Position of the rel type expression
    )
  }

  test("()-[IS:IS]->()") {
    run().hasNoErrors
  }

  test("(n IS A)-[r IS R]->(m:B) RETURN *") {
    // Mixing colon (not as conjunction) and IS keyword should be allowed as they are both part of GQL
    run().hasNoErrors
  }

  // Dynamic labels and types
  test("(n:$(\"label\"))") {
    run().hasNoErrors
  }

  test("(n:A&B&$(\"label\"))") {
    run().hasNoErrors
  }

  test("(n)-[:$(\"label\")]->()") {
    run().hasNoErrors
  }

  test("(n:$(1))") {
    run().hasErrorMessages("Type mismatch: expected String or List<String> but was Integer")
  }

  test("(n)-[:$(1 + 3.0)]->()") {
    run()
      .hasErrorMessages("Type mismatch: expected String or List<String> but was Float")
  }

  test("(n:$([1]))") {
    run().hasErrorMessages(
      "Type mismatch: expected String or List<String> but was List<Integer>"
    )
  }

  test("(n:$(['']))") {
    run()
      .hasError(
        GqlHelper.getGql42001_42I11("label", "", offset + 3, 1, offset + 4),
        "'' is not a valid token name. Token names cannot be empty or contain any null-bytes.",
        InputPosition(offset + 3, 1, offset + 4)
      )
  }

  test("(n:$([null]))") {
    run()
      .hasError(
        GqlHelper.getGql42001_42I11("label", "Null", offset + 3, 1, offset + 4),
        "Null is not a valid token name. Token names cannot be empty or contain any null-bytes.",
        InputPosition(offset + 3, 1, offset + 4)
      )
  }

  test("(n:$all(['Foo', 'Bar']))") {
    run().hasNoErrors
  }

  test("(n:$any(['Foo', 'Bar']))") {
    run().hasErrorMessages(
      "Dynamic labels using `$any()` are not allowed in CREATE or MERGE."
    )
  }

  test("(n:$(['Foo', 'Bar']))") {
    run().hasNoErrors
  }

  test("(n)-[:$any('Foo')]->()") {
    run().hasErrorMessages(
      "Dynamic types using `$any()` are not allowed in CREATE or MERGE."
    )
  }

  test("(n)-[:$(['Foo', 'Bar'])]->()") {
    run()
      .hasErrorMessages(s"A single relationship type must be specified for $statement")
  }

  test("(n)-[:$([])]->()") {
    run().hasErrorMessages(
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
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", offset + 4, 1, offset + 5),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(offset + 4, 1, offset + 5)
    )
  }

  test("(n:A), (m IS B) RETURN *") {
    // Mixing colon (not as conjunction) and IS keyword should be allowed as they are both part of GQL
    run().hasNoErrors
  }
}

class LabelExpressionInMergeSemanticAnalysisTest
    extends LabelExpressionSemanticAnalysisTestSuiteWithUpdateStatement(UpdateStatement.MERGE) {}
