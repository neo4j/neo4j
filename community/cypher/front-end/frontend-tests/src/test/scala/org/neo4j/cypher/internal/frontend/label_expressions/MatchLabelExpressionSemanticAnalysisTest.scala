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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.p
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.frontend.NameBasedSemanticAnalysisTestSuite
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.gqlstatus.GqlHelper
import org.scalatest.LoneElement

class MatchLabelExpressionSemanticAnalysisTest extends NameBasedSemanticAnalysisTestSuite with LoneElement {

  // Node Pattern
  test("MATCH (n) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n:A) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n IS A) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (:A) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (IS A) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n:A:B) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (:A:B) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n:A|:B) RETURN n") {
    // should not allow colon disjunctions on nodes
    run()
      .hasErrorMessages("Label expressions are not allowed to contain '|:'.")
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, "|:"))
  }

  test("MATCH (n IS A|:B) RETURN n") {
    // should not allow colon disjunctions on nodes
    run()
      .hasErrorMessages("Label expressions are not allowed to contain '|:'.")
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, "|:"))
  }

  test("MATCH (:(A|B)&!C) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (IS !(A|B&C)) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (IS %) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n:A&B:C) RETURN n") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B&C", 12, 1, 13),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B&C.",
      InputPosition(12, 1, 13)
    )
  }

  test("MATCH (n IS A:B) RETURN n") {
    // should not allow mixing colon as label conjunction symbol with IS keyword in label expression
    run().hasError(
      GqlHelper.getGql42001_42I29("IS A:B", "IS A&B", 13, 1, 14),
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&B.",
      InputPosition(13, 1, 14)
    )
  }

  test("MATCH (n IS A&B:C) RETURN n") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression
    run().hasError(
      GqlHelper.getGql42001_42I29("IS (A&B):C", "IS A&B&C", 15, 1, 16),
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&B&C.",
      InputPosition(15, 1, 16)
    )
  }

  test("MATCH (n:A), (m:A&B) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A), (m:A:B) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A)-[r:R|T]-(m:B) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A:B)-[r:R|T]-(m:B) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A&B)-[r:R|T]-(m:B) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A)-[r:!R&!T]-(m:B) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A&B)-[r]-(m:B:C) RETURN *") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":B&C", 22, 1, 23),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :B&C.",
      InputPosition(22, 1, 23)
    )
  }

  test("MATCH (n:A:B)-[r:!R&!T]-(m:B) RETURN *") {
    run().hasErrorMessages(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  test("MATCH (n:A:B), (m:A&B) RETURN *") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 10, 1, 11),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n:A:B)-[]-(m) WHERE m:(A&B)|C RETURN *") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 10, 1, 11),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n:A:B)-[]-(m) WHERE (m:(A&B)|C)--() RETURN *") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 10, 1, 11),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n:A:B)-[]-(m) WHERE m IS C RETURN *") {
    run().hasError(
      GqlHelper.getGql42001_42I29(":A:B", ":A&B", 10, 1, 11),
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n:A:B)-[]-(m) WHERE n IS C AND m:D:E RETURN *") {
    run().hasError(
      GqlHelper.getGql42001_42I29(":A:B, :D:E", ":A&B, :D&E", 10, 1, 11),
      "Mixing the IS keyword with colon (':') between labels is not allowed. These expressions could be expressed as :A&B, :D&E.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n:A:B)-[r IS A|B]->(m) RETURN *") {
    run().hasError(
      GqlHelper.getGql42001_42I29(":A:B", ":A&B", 10, 1, 11),
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n:A)-[]-(m) WHERE (m:(A&B)|C)--() RETURN *") {
    run().hasNoErrors
  }

  test("MATCH p = shortestPath((a:A|B)-[:REL*]->(b:B|C)) RETURN length(p) AS result") {
    run().hasNoErrors
  }

  test("MATCH p = shortestPath((a IS A)-[:REL*]->(b:B)) RETURN length(p) AS result") {
    run().hasNoErrors
  }

  test("MATCH (n), (m) WHERE length(shortestPath((n)-[:A|B|C*]->(m))) > 1 RETURN n, m AS result") {
    run().hasNoErrors
  }

  test("MATCH p = shortestPath((n)-[:A|B|C*]->(m)) RETURN length(p) AS result") {
    run().hasNoErrors
  }

  test("MATCH (n), (m) WHERE length(shortestPath((n)-[:!A&!B*]->(m))) > 0 RETURN n, m AS result") {
    run().hasErrorMessages("Variable length relationships must not use relationship type expressions.")
  }

  test("MATCH (n), (m) WITH COLLECT { MATCH p = (n)-[IS A*]->(m) RETURN p } AS p RETURN p AS result") {
    run().hasNoErrors
  }

  test("MATCH p = shortestPath((n)-[:!A&!B*]->(m)) RETURN length(p) AS result") {
    run().hasErrorMessages("Variable length relationships must not use relationship type expressions.")
  }

  test("MATCH p = shortestPath((n)-[:!A&!B]->(m)) RETURN length(p) AS result") {
    run().hasErrorMessages(
      "Variable length relationships must not use relationship type expressions."
    )
  }

  test("MATCH p = shortestPath((n)-[IS A]->(m)) RETURN length(p) AS result") {
    run().hasNoErrors
  }

  test("MATCH (n:A&B)-[]-(m) WHERE (m:A:B)--() RETURN *") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 31, 1, 32),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(31, 1, 32)
    )
  }

  test("MATCH (n:A:B) MATCH (m:(A&B)|C) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A:B) MATCH (m IS C) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A:B) WITH n WHERE n:(A&B)|C RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A:B) WITH n WHERE n IS C RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A:B WHERE true)-[]-(m) RETURN *") {
    run().hasNoErrors
  }

  // Relationship Pattern
  test("MATCH ()-[r]->() RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[r:A]->() RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[r IS A]->() RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[:A]->() RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[IS A]->() RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[r:A|B]->() RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[:A|B]->() RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[:A|B*]->() RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[r:%]->() RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n)-[:A|:B]->() RETURN n") {
    // should allow old style relationship types without names, predicates, properties, quantifiers (for now)
    run().hasNoErrors
  }

  test("MATCH ()-[:A|B|(!C&!D)]->() RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[r:A:B]->() RETURN r") {
    // should not allow colon conjunctions on relationships
    run()
      .hasErrorMessages("Relationship types in a relationship type expressions may not be combined using ':'")
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, ":"))
  }

  test("MATCH ()-[r IS A:B]->() RETURN r") {
    // should not allow colon conjunctions on relationships
    run()
      .hasErrorMessages("Relationship types in a relationship type expressions may not be combined using ':'")
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, ":"))
  }

  test("MATCH (n)-[:A|:B&!C]->() RETURN n") {
    run()
      .hasErrorMessages(
        """The semantics of using colon in the separation of alternative relationship types in conjunction with
          |the use of variable binding, inlined property predicates, or variable length is no longer supported.
          |Please separate the relationships types using `:A|(B&!C)` instead.""".stripMargin
      )
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, "|:"))
  }

  test("MATCH (n)-[IS A|:B&!C]->() RETURN n") {
    run()
      .hasErrorMessages(
        """The semantics of using colon in the separation of alternative relationship types in conjunction with
          |the use of variable binding, inlined property predicates, or variable length is no longer supported.
          |Please separate the relationships types using `IS A|(B&!C)` instead.""".stripMargin
      )
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, "|:"))
  }

  test("MATCH (n)-[:(A&!B)|:C]->() RETURN n") {
    // should not allow mixing colon disjunction symbol with GPM label expression symbols in relationship type expression
    run()
      .hasErrorMessages(
        """The semantics of using colon in the separation of alternative relationship types in conjunction with
          |the use of variable binding, inlined property predicates, or variable length is no longer supported.
          |Please separate the relationships types using `:(A&!B)|C` instead.""".stripMargin
      )
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, "|:"))
  }

  test("MATCH ()-[:!A*]->() RETURN count(*)") {
    run().hasErrorMessages(
      "Variable length relationships must not use relationship type expressions."
    )
  }

  // LabelExpressionPredicate

  // Node
  test("MATCH (n) WHERE n:A RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n) WHERE n IS A RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n) WHERE n:A:B RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n) WHERE n:A|:B RETURN n") {
    // should not allow colon disjunctions on node label predicate
    run()
      .hasErrorMessages(
        """Label expressions are not allowed to contain '|:'.
          |If you want to express a disjunction of labels, please use `:A|B` instead""".stripMargin
      )
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, "|:"))
  }

  test("MATCH (n) WHERE n IS A|:B RETURN n") {
    // should not allow colon disjunctions on node label predicate
    run()
      .hasErrorMessages(
        """Label expressions are not allowed to contain '|:'.
          |If you want to express a disjunction of labels, please use `IS A|B` instead""".stripMargin
      )
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, "|:"))
  }

  test("MATCH (n) WHERE n:(A|B)&!C RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n) WHERE n:A&B:C RETURN n") {
    // should not allow mixing colon as label conjunction symbol with GPM label expression symbols in label expression predicate
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B&C", 21, 1, 22),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B&C.",
      InputPosition(21, 1, 22)
    )
  }

  test("MATCH (n) WHERE n IS A:C RETURN n") {
    // should not allow mixing colon as label conjunction symbol with IS keyword in label expression predicate
    run().hasError(
      GqlHelper.getGql42001_42I29("IS A:C", "IS A&C", 22, 1, 23),
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as IS A&C.",
      InputPosition(22, 1, 23)
    )
  }

  test("MATCH (n) WHERE n:% RETURN n") {
    run().hasNoErrors
  }

  test("MATCH (n) WHERE n:!A&% RETURN n") {
    run().hasNoErrors
  }

  // Relationship
  test("MATCH ()-[r]->() WHERE r:A RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[r]->() WHERE r IS A RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[r]->() WHERE r:A|B RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n)-[r]->() WHERE n:A|B&C RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (n)-[r]->() WHERE n IS A|B&C RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ()-[r]->() WHERE r:A:B RETURN count(*)") {
    // this was allowed before, so we must continue to accept it
    run().hasNoErrors
  }

  test("MATCH (n)-[r]->() WHERE r:(A&!B)|:C RETURN n") {
    // should not allow mixing colon disjunction symbol with GPM label expression symbols in relationship type expression – separate error
    run()
      .hasErrorMessages(
        """The semantics of using colon in the separation of alternative relationship types in conjunction with
          |the use of variable binding, inlined property predicates, or variable length is no longer supported.
          |Please separate the relationships types using `:(A&!B)|C` instead.""".stripMargin
      )
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, "|:"))
  }

  test("MATCH (n)-[r]->() WHERE r IS (A&!B)|:C RETURN n") {
    // should not allow mixing colon disjunction symbol with IS keyword in relationship type expression – separate error
    run()
      .hasErrorMessages(
        """The semantics of using colon in the separation of alternative relationship types in conjunction with
          |the use of variable binding, inlined property predicates, or variable length is no longer supported.
          |Please separate the relationships types using `IS (A&!B)|C` instead.""".stripMargin
      )
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, "|:"))
  }

  test("MATCH (n)-[r]->() WHERE r:B|:C RETURN n") {
    run()
      .hasErrorMessages(
        """The semantics of using colon in the separation of alternative relationship types in conjunction with
          |the use of variable binding, inlined property predicates, or variable length is no longer supported.
          |Please separate the relationships types using `:B|C` instead.""".stripMargin
      )
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, "|:"))
  }

  test("MATCH ()-[r]->() WHERE r:% RETURN r") {
    run().hasNoErrors
  }

  test("MATCH ()-[r]->() WHERE r:!A&% RETURN r") {
    run().hasNoErrors
  }

  test("MATCH (n:A:B WHERE $param:C|D) RETURN count(*)") {
    // should allow disjunction on unknown type
    run().hasNoErrors
  }

  // Both Node and predicate

  test("MATCH (n:A:B) WHERE n:C&D|E RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 10, 1, 11),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n:A:B) WHERE n IS C RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I29(":A:B", ":A&B", 10, 1, 11),
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n IS A) WHERE n :B:C RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I29(":B:C", ":B&C", 25, 1, 26),
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as :B&C.",
      InputPosition(25, 1, 26)
    )
  }

  test("MATCH (n:A:B) WHERE n:C|D|E RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 10, 1, 11),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n:A:B WHERE n:C&D|E) RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 10, 1, 11),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n:A:B)-[:R|(T&S)]-(m) RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 10, 1, 11),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n:A&B)-[:R|T|:S]-(m) RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":R|T|S", 19, 1, 20),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :R|T|S.",
      InputPosition(19, 1, 20)
    )
  }

  test("MATCH (n:A&B)-[IS R|T|:S]-(m) RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I10("IS R|T|S", 21, 1, 22),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as IS R|T|S.",
      InputPosition(21, 1, 22)
    )
  }

  test("MATCH (n:A:B WHERE n:C|D|E) RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 10, 1, 11),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH (n:C&D|E) WHERE n:A:B RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 25, 1, 26),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(25, 1, 26)
    )
  }

  test("MATCH (n:C&D|E)-[]-(m:A:F) WHERE n:A:B RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&F, :A&B", 23, 1, 24),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. These expressions could be expressed as :A&F, :A&B.",
      InputPosition(23, 1, 24)
    )
  }

  test("MATCH (n:C&D|E)-[]-(m IS A:F) WHERE n:A:B RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I10("IS A&F, :A&B", 26, 1, 27),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. These expressions could be expressed as IS A&F, :A&B.",
      InputPosition(26, 1, 27)
    )
  }

  // CIP-40 test cases
  // all non-GPM
  test("MATCH (n:A:B:C)-[*]->() RETURN n") {
    run().hasNoErrors
  }

  test("MATCH (n:A:B)-[r:S|T|U]-() RETURN n, r") {
    run().hasNoErrors
  }

  test("MATCH p = shortestPath(()-[*1..5]-()) RETURN p") {
    run().hasNoErrors
  }

  // All GPM
  test("MATCH ()-[r:A&B]->*() RETURN r") {
    run().hasNoErrors
  }

  test("MATCH (n:(A&B)|C)-[]->+() RETURN n") {
    run().hasNoErrors
  }

  test("MATCH p = SHORTEST 2 PATHS ()-[]-{1,5}() RETURN p") {
    run().hasNoErrors
  }

  // GPM and non-GPM in separate statements
  test("MATCH (m:A:B:C)-[]->() MATCH (n:(A&B)|C)-[]->(m) RETURN m,n") {
    run().hasNoErrors
  }

  test("MATCH (n)-[r*]-(m) MATCH (n)-[]->+() RETURN m,n,r") {
    run().hasNoErrors
  }

  test("MATCH p = shortestPath(()-[*1..5]-()) MATCH q = SHORTEST 2 PATHS ()-[]-{1,5}() RETURN nodes(p), nodes(q)") {
    run().hasNoErrors
  }

  // GPM and non-GPM in unrelated features
  test("MATCH (m)-[]->+(n:R) RETURN m, n") {
    run().hasNoErrors
  }

  test("MATCH ((a:A:B)-[]->(b) WHERE a.p < b.p)+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH p = SHORTEST 2 PATHS (m)-[*0..5]-(n) RETURN p") {
    run().hasNoErrors
  }

  // Mixed label expression in same statement
  test("MATCH (n:A:B)-[]-(m) WHERE m:(A&B)|C RETURN m, n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 10, 1, 11),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  // ... graph pattern
  test("MATCH (n:A:B)--(:C), (n)-->(m:(A&B)|C) RETURN m, n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 10, 1, 11),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  // ... path pattern
  test("MATCH (n:A:B)-[]-(m:(A&B)|C) RETURN m, n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 10, 1, 11),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(10, 1, 11)
    )
  }

  // ... node pattern
  test("MATCH (n:A|B:C) RETURN n") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A|(B&C)", 12, 1, 13),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A|(B&C).",
      InputPosition(12, 1, 13)
    )
  }

  // Mixing pre-GPM label expression with QPP does not raise SyntaxError
  test("MATCH ({p: 1})-->() ((:R:T)--()){1,2} ()-->(m) RETURN m.p as mp") {
    run().hasNoErrors
  }

  // Mixing colon (not as conjunction) and IS keyword should be allowed as they are both part of GQL

  test("MATCH (n:A)-[r]-(m IS B) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A WHERE n IS B) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n IS A WHERE n:B) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n)-[r1:$([\"Z\"])]->(m:!Z) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n)-[r1 IS $([\"Z\"])]->(m IS !Z) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n)-[r1 IS $([\"Z\"])]->(m:!Z) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n)-[r IS $([\"A\"])|B]->(m) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A:$([\"B\"]))-[r IS $([\"A\"])|B]->(m) RETURN *") {
    run().hasError(
      GqlHelper.getGql42001_42I29(":A:$all([\"B\"])", ":A&$all([\"B\"])", 10, 1, 11),
      "Mixing the IS keyword with colon (':') between labels is not allowed. This expression could be expressed as :A&$all([\"B\"]).",
      InputPosition(10, 1, 11)
    )
  }

  test(
    """
      |MATCH (n)-[r]->()
      |WITH [n, r] AS x
      |UNWIND x AS y
      |RETURN
      |  CASE
      |    WHEN y:A|B THEN 1
      |    WHEN y:A:B THEN 0
      |  END AS z
      |""".stripMargin
  ) {
    // We can only at runtime detect whether y:A|B is gpm-only (if applied to a node),
    // or both gpm and legacy (if applied to a relationship).
    run().hasNoErrors
  }

  // Dynamic labels and types
  test("MATCH (n:$(\"label\")) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:A&B&$(\"label\")) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n)-[:$(\"label\")]->() RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:$(1)) RETURN *") {
    run().hasErrors(SemanticError.invalidEntityType(
      "INTEGER",
      "dynamic label",
      List("STRING", "LIST<STRING>"),
      "Type mismatch: expected String or List<String> but was Integer",
      p(11, 1, 12).withInputLength(1)
    ))
  }

  test("MATCH (n:$([''])) RETURN *") {
    run().hasError(
      GqlHelper.getGql42001_42I11("label", "", 9, 1, 10),
      "'' is not a valid token name. Token names cannot be empty or contain any null-bytes.",
      p(9, 1, 10) // Position of the node
    )
  }

  test("MATCH (n:$(null)) RETURN *") {
    run().hasError(
      GqlHelper.getGql42001_42I11("label", "Null", 9, 1, 10),
      "Null is not a valid token name. Token names cannot be empty or contain any null-bytes.",
      p(9, 1, 10) // Position of the node
    )
  }

  test("MATCH (n:$([\"A\", \"\"])) RETURN *") {
    run().hasError(
      GqlHelper.getGql42001_42I11("label", "", 9, 1, 10),
      "'' is not a valid token name. Token names cannot be empty or contain any null-bytes.",
      p(9, 1, 10) // Position of the node
    )
  }

  test("MATCH (n)-[:$(point({x:22, y:44}))]-() RETURN *") {
    run().hasErrors(SemanticError.invalidEntityType(
      "POINT",
      "dynamic type",
      List("STRING", "LIST<STRING>"),
      "Type mismatch: expected String or List<String> but was Point",
      p(14, 1, 15)
    ))
  }

  test("MATCH (n:$([1])) RETURN *") {
    run().hasErrors(SemanticError.invalidEntityType(
      "LIST<INTEGER>",
      "dynamic label",
      List("STRING", "LIST<STRING>"),
      "Type mismatch: expected String or List<String> but was List<Integer>",
      p(11, 1, 12)
    ))
  }

  test("MATCH (n:$all(['Foo', 'Bar'])) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:$any(['Foo', 'Bar'])) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n:$(['Foo', 'Bar'])) RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n)-[:$all(['Foo', 'Bar'])]-() RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n)-[:$any(['Foo', 'Bar'])]-() RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n)-[:$(['Foo', 'Bar'])]-() RETURN *") {
    run().hasNoErrors
  }

  test("MATCH (n)-[:!$('R')]-() RETURN *") {
    run().hasNoNotifications
  }

  test("MATCH (n)-[:A&$('R')]-() RETURN *") {
    run().hasNoNotifications
  }

  test("MATCH (n)-[:$('R2')&$('R')]-() RETURN *") {
    run().hasNoNotifications
  }

  test("MATCH (n)-[:A&!%]-() RETURN *") {
    run()
      .assert(_.notifications.map(_.notificationName) shouldBe Set("UnsatisfiableRelationshipTypeExpression"))
  }
}
