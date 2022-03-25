/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

abstract class LabelExpressionSemanticAnalysisTestSuiteWithStatement(statement: Statement)
    extends CypherFunSuite
    with SemanticAnalysisTestSuiteWithDefaultQuery
    with TestName {

  override def defaultQuery: String = s"$statement $testName"

  test("(n:A:B)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("(n:A&B)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      s"Label expressions in patterns are not allowed in $statement, but only in MATCH clause"
    )
  }

  test("(n:A|B)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      s"Label expressions in patterns are not allowed in $statement, but only in MATCH clause"
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

}

sealed trait Statement

object Statement {
  case object CREATE extends Statement
  case object MERGE extends Statement
}

class LabelExpressionInCreateSemanticAnalysisTestSuite
    extends LabelExpressionSemanticAnalysisTestSuiteWithStatement(Statement.CREATE)

class LabelExpressionInMergeSemanticAnalysisTestSuite
    extends LabelExpressionSemanticAnalysisTestSuiteWithStatement(Statement.MERGE)

class OtherLabelExpressionSemanticAnalysisTest
    extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  // Node Pattern
  test("MATCH (n) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (:A) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A:B) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (:A:B) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A|:B) RETURN n") {
    // should not allow colon disjunctions on nodes
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Label expressions are not allowed to contain '|:'."
    )
  }

  test("MATCH (:(A|B)&!C) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A&B:C) RETURN n") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B&C."
    )
  }

  // Relationship Pattern
  test("MATCH ()-[r]->() RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[r:A]->() RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[:A]->() RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[r:A|B]->() RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[:A|B]->() RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[:A|B*]->() RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[r:%]->() RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n)-[:A|:B]->() RETURN n") {
    // should allow old style relationship types without names, predicates, properties, quantifiers (for now)
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[:A|B|(!C&!D)]->() RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[r:A:B]->() RETURN r") {
    // should not allow colon conjunctions on relationships
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Relationship types in a relationship type expressions may not be combined using ':'"
    )
  }

  test("MATCH (n)-[:A|:B&!C]->() RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """The semantics of using colon in the separation of alternative relationship types in conjunction with
        |the use of variable binding, inlined property predicates, or variable length is no longer supported.
        |Please separate the relationships types using `:A|(B&!C)` instead.""".stripMargin
    )
  }

  test("MATCH (n)-[:(A&!B)|:C]->() RETURN n") {
    // should not allow mixing colon disjunction symbol with GPM label expression symbols in relationship type expression
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """The semantics of using colon in the separation of alternative relationship types in conjunction with
        |the use of variable binding, inlined property predicates, or variable length is no longer supported.
        |Please separate the relationships types using `:(A&!B)|C` instead.""".stripMargin
    )
  }

  test("MATCH ()-[:!A*]->() RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Variable length relationships must not use relationship type expressions."
    )
  }

  // LabelExpressionPredicate

  // Node
  test("MATCH (n) WHERE n:A RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n) WHERE n:A:B RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n) WHERE n:A|:B RETURN n") {
    // should not allow colon disjunctions on node label predicate
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """Label expressions are not allowed to contain '|:'.
        |If you want to express a disjunction of labels, please use `:A|B` instead""".stripMargin
    )
  }

  test("MATCH (n) WHERE n:(A|B)&!C RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n) WHERE n:A&B:C RETURN n") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression predicate
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B&C."
    )
  }

  test("MATCH (n) WHERE n:% RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Wildcards ('%') in label/relationship type expression predicates are not supported yet"
    )
  }

  test("MATCH (n) WHERE n:!A&% RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Wildcards ('%') in label/relationship type expression predicates are not supported yet"
    )
  }

  // Relationship
  test("MATCH ()-[r]->() WHERE r:A RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[r]->() WHERE r:A|B RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[r]->() WHERE r:A:B RETURN count(*)") {
    // this was allowed before, so we must continue to accept it
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test("MATCH (n)-[r]->() WHERE r:(A&!B)|:C RETURN n") {
    // should not allow mixing colon disjunction symbol with GPM label expression symbols in relationship type expression – separate error
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """The semantics of using colon in the separation of alternative relationship types in conjunction with
        |the use of variable binding, inlined property predicates, or variable length is no longer supported.
        |Please separate the relationships types using `:(A&!B)|C` instead.""".stripMargin
    )
  }

  test("MATCH (n)-[r]->() WHERE r:B|:C RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """The semantics of using colon in the separation of alternative relationship types in conjunction with
        |the use of variable binding, inlined property predicates, or variable length is no longer supported.
        |Please separate the relationships types using `:B|C` instead.""".stripMargin
    )
  }

  test("MATCH ()-[r]->() WHERE r:% RETURN r") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Wildcards ('%') in label/relationship type expression predicates are not supported yet"
    )
  }

  test("MATCH ()-[r]->() WHERE r:!A&% RETURN r") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Wildcards ('%') in label/relationship type expression predicates are not supported yet"
    )
  }

  // Unknown

  test("RETURN $param:A:B") {
    // should allow colon conjunction on unknown type
    runSemanticAnalysis().errors shouldBe empty
  }

  test("RETURN $param:A|:B") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """The semantics of using colon in the separation of alternative relationship types in conjunction with
        |the use of variable binding, inlined property predicates, or variable length is no longer supported.
        |Please separate the relationships types using `:A|B` instead.""".stripMargin
    )
  }

  test("RETURN $param:A:B&C") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B&C."
    )
  }

  test("RETURN $param:A|B:C") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A|(B&C)."
    )
  }

  // Both Node and predicate

  test("MATCH (n:A:B) WHERE n:C&D|E RETURN n") {
    // should allow mixing colon as label conjunction symbols on node pattern with GPM label expression predicate
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A:B WHERE n:C&D|E) RETURN n") {
    // should allow mixing colon as label conjunction symbols on node pattern with GPM label expression predicate
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:C&D|E) WHERE n:A:B RETURN n") {
    // should allow mixing GPM label expression predicate with colon as label conjunction symbols on node pattern
    runSemanticAnalysis().errors shouldBe empty
  }
}
