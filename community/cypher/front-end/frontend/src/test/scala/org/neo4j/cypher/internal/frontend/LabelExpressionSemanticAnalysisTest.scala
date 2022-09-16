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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory.SyntaxException
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
      s"Label expressions in patterns are not allowed in $statement, but only in MATCH clause and in expressions"
    )
  }

  test("(n:A|B)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      s"Label expressions in patterns are not allowed in $statement, but only in MATCH clause and in expressions"
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

  test("MATCH (n:A), (m:A&B) RETURN *") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A), (m:A:B) RETURN *") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A)-[r:R|T]-(m:B) RETURN *") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A:B)-[r:R|T]-(m:B) RETURN *") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A&B)-[r:R|T]-(m:B) RETURN *") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A)-[r:!R&!T]-(m:B) RETURN *") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A&B)-[r]-(m:B:C) RETURN *") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :B&C."
    )
  }

  test("MATCH (n:A:B)-[r:!R&!T]-(m:B) RETURN *") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:A:B), (m:A&B) RETURN *") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:A:B)-[]-(m) WHERE m:(A&B)|C RETURN *") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:A:B)-[]-(m) WHERE (m:(A&B)|C)--() RETURN *") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:A)-[]-(m) WHERE (m:(A&B)|C)--() RETURN *") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (a), (b) WITH shortestPath((a:A|B)-[:REL*]->(b:B|C)) AS p RETURN length(p) AS result") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Label expressions in shortestPath are not allowed in expression"
    )
  }

  test("MATCH (a), (b) WITH shortestPath((a:A)-[:A*]->(b:B)) AS p RETURN length(p) AS result") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (a), (b) RETURN [(a:A|B)-[:REL*]->(b:B|C) | 1] AS p") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A&B)-[]-(m) WHERE (m:A:B)--() RETURN *") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:A:B) MATCH (m:(A&B)|C) RETURN *") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A:B) WITH n WHERE n:(A&B)|C RETURN *") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A:B WHERE true)-[]-(m) RETURN *") {
    runSemanticAnalysis().errors shouldBe empty
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
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test("MATCH (n) WHERE n:!A&% RETURN n") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  // Relationship
  test("MATCH ()-[r]->() WHERE r:A RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ()-[r]->() WHERE r:A|B RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n)-[r]->() WHERE n:A|B&C RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A:B)-[r]->() WITH [r, n] AS list UNWIND list as x RETURN x:A|B") {
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
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test("MATCH ()-[r]->() WHERE r:!A&% RETURN r") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  // Unknown

  test("RETURN $param:A:B") {
    // should allow colon conjunction on unknown type
    runSemanticAnalysis().errors shouldBe empty
  }

  test("RETURN $param:A|B") {
    // should allow disjunction on unknown type
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

  test("MATCH (n:A:B WHERE $param:C|D) RETURN count(*)") {
    // should allow disjunction on unknown type
    runSemanticAnalysis().errors shouldBe empty
  }

  // Both Node and predicate

  test("MATCH (n:A:B) WHERE n:C&D|E RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:A:B) WHERE n:C|D|E RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:A:B WHERE n:C&D|E) RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:A:B)-[:R|(T&S)]-(m) RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:A&B)-[:R|T|:S]-(m) RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :R|T|S."
    )
  }

  test("MATCH (n:A:B WHERE n:C|D|E) RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:C&D|E) WHERE n:A:B RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:C&D|E)-[]-(m:A:F) WHERE n:A:B RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. These expressions could be expressed as :A&F, :A&B."
    )
  }

  // CIP-40 test cases
  // all non-GPM
  test("MATCH (n:A:B:C)-[*]->() RETURN n") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n:A:B)-[r:S|T|U]-() RETURN n, r") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH p = shortestPath(()-[*1..5]-()) RETURN p") {
    runSemanticAnalysis().errors shouldBe empty
  }

  // All GPM
  test("MATCH ()-[r:A&B]->*() RETURN r") {
    // quantified relationships are not implemented yet. Once this is the case, please change to the test below
    the[SyntaxException].thrownBy(
      runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns)
    ).getMessage should include("Invalid input '*': expected \"(\"")
    // runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH (n:(A&B)|C)-[]->+() RETURN n") {
    // quantified relationships are not implemented yet. Once this is the case, please change to the test below
    the[SyntaxException].thrownBy(
      runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns)
    ).getMessage should include("Invalid input '+': expected \"(\"")
    // runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH p = SHORTEST 2 PATHS ()-[]-{1,5}() RETURN p") {
    // Shortest paths by keywords are not implemented yet. Once this is the case, please change to the test below
    the[SyntaxException].thrownBy(runSemanticAnalysis()).getMessage should include(
      "Invalid input 'SHORTEST': expected \"allShortestPaths\" or \"shortestPath\""
    )
    // runSemanticAnalysis().errors shouldBe empty
  }

  // GPM and non-GPM in separate statements
  test("MATCH (m:A:B:C)-[]->() MATCH (n:(A&B)|C)-[]->(m) RETURN m,n") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n)-[r*]-(m) MATCH (n)-[]->+() RETURN m,n,r") {
    // quantified relationships are not implemented yet. Once this is the case, please change to the test below
    the[SyntaxException].thrownBy(
      runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns)
    ).getMessage should include("Invalid input '+': expected \"(\"")
    // runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH p = shortestPath(()-[*1..5]-()) MATCH q = SHORTEST 2 PATHS ()-[]-{1,5}() RETURN nodes(p), nodes(q)") {
    // Shortest paths by keywords are not implemented yet. Once this is the case, please change to the test below
    the[SyntaxException].thrownBy(runSemanticAnalysis()).getMessage should include(
      "Invalid input 'SHORTEST': expected \"allShortestPaths\" or \"shortestPath\""
    )
    // runSemanticAnalysis().errors shouldBe empty
  }

  test("""MATCH (m:A:B:C)-[]->()
         |RETURN
         |  CASE
         |    WHEN m:D|E THEN m.p
         |    ELSE null
         |  END
         |""".stripMargin) {
    runSemanticAnalysis().errors shouldBe empty
  }

  // GPM and non-GPM in unrelated features
  test("MATCH p = shortestPath((n)-[]->+({s: 1})) RETURN p") {
    // quantified relationships are not implemented yet. Once this is the case, please change to the test below
    the[SyntaxException].thrownBy(
      runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns)
    ).getMessage should include("Invalid input '+': expected \"(\"")
    // runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH (m)-[]->+(n:R) RETURN m, n") {
    // quantified relationships are not implemented yet. Once this is the case, please change to the test below
    the[SyntaxException].thrownBy(
      runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns)
    ).getMessage should include("Invalid input '+': expected \"(\"")
    // runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH ((a:A:B)-[]->(b) WHERE a.p < b.p)+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH p = SHORTEST 2 PATHS (m)-[*0..5]-(n) RETURN p") {
    // Shortest paths by keywords are not implemented yet. Once this is the case, please change to the test below
    the[SyntaxException].thrownBy(runSemanticAnalysis()).getMessage should include(
      "Invalid input 'SHORTEST': expected \"allShortestPaths\" or \"shortestPath\""
    )
    // runSemanticAnalysis().errors shouldBe empty
  }

  // Mixed label expression in same statement
  test("MATCH (n:A:B)-[]-(m) WHERE m:(A&B)|C RETURN m, n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH ((n:A:B:C)-[]->()) RETURN n:A&B, n:A:B") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  // ... graph pattern
  test("MATCH (n:A:B)--(:C), (n)-->(m:(A&B)|C) RETURN m, n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  // ... path pattern
  test("MATCH (n:A:B)-[]-(m:(A&B)|C) RETURN m, n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  // ... node pattern
  test("MATCH (n:A|B:C) RETURN n") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A|(B&C)."
    )
  }

  // Mixing pre-GPM label expression with QPP does not raise SyntaxError
  test("MATCH ({p: 1})-->() ((:R:T)--()){1,2} ()-->(m) RETURN m.p as mp") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }
}
