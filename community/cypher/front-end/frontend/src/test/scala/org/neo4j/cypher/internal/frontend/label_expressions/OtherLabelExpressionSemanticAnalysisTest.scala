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

import org.neo4j.cypher.internal.frontend.NameBasedSemanticAnalysisTestSuite

class OtherLabelExpressionSemanticAnalysisTest extends NameBasedSemanticAnalysisTestSuite {

  test("MATCH (a), (b) WITH shortestPath((a:A|B)-[:REL*]->(b:B|C)) AS p RETURN length(p) AS result") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Label expressions in shortestPath are not allowed in an expression"
    )
  }

  test("MATCH (a), (b) WITH shortestPath((a IS A)-[:REL*]->(b:B)) AS p RETURN length(p) AS result") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (a), (b) WITH shortestPath((a:A)-[:A*]->(b:B)) AS p RETURN length(p) AS result") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n), (m) WITH shortestPath((n)-[:A|B|C*]->(m)) AS p RETURN length(p) AS result") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n), (m) WITH shortestPath((n)-[:!A&!B*]->(m)) AS p RETURN length(p) AS result") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      "Variable length relationships must not use relationship type expressions."
    )
  }

  test("MATCH (n), (m) WITH shortestPath((n)-[IS A*]->(m)) AS p RETURN length(p) AS result") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (n), (m) WITH (n)-[:!A&!B*]->(m) AS p RETURN p AS result") {
    runSemanticAnalysis().errorMessages.toSet shouldEqual Set(
      "Variable length relationships must not use relationship type expressions."
    )
  }

  test("MATCH (a), (b) RETURN [(a:A|B)-[:REL*]->(b IS B) | 1] AS p") {
    runSemanticAnalysis().errors shouldBe empty
  }

  // LabelExpressionPredicate

  // Node

  test("MATCH (n:A:B)-[r]->() WITH [r, n] AS list UNWIND list as x RETURN x:A|B") {
    runSemanticAnalysis().errors shouldBe empty
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

  // Mixed label expression in same statement
  test("""MATCH (m:A:B:C)-[]->()
         |RETURN
         |  CASE
         |    WHEN m:D|E THEN m.p
         |    ELSE null
         |  END
         |""".stripMargin) {
    runSemanticAnalysis().errors shouldBe empty
  }

  // Mixed label expression in same statement
  test("MATCH ((n:A:B:C)-[]->()) RETURN n:A&B, n:A:B") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B."
    )
  }

  // Ignored since changing this would break backwards compatibility.
  // See the "GPM Sync Rolling Agenda" notes for Nov 23, 2023
  // Mixed label specification in same statements
  ignore(
    """
      |CALL {
      |  CREATE (n:A&B)
      |  SET n:C:D
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as multiple comma separated items which one Label each."
    )
  }

  // Ignored since changing this would break backwards compatibility.
  // See the "GPM Sync Rolling Agenda" notes for Nov 23, 2023
  // Mixed label specification in same statements
  ignore(
    """
      |CALL {
      |  CREATE (n:A&B)
      |  REMOVE n:C:D
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') is not allowed. Please only use one set of symbols. This expression could be expressed as multiple comma separated items which one Label each."
    )
  }

  // Ignored since changing this would break backwards compatibility.
  // See the "GPM Sync Rolling Agenda" notes for Nov 23, 2023
  // Mixed quantifiers
  ignore(
    """
      |RETURN COUNT {
      |  MATCH (n:A)--{,5}(:B)
      |  MATCH (n)-[*0..5]-(:C)  
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  test(
    """
      |MATCH (n)
      |CALL {
      | WITH n
      |  SET n:A
      |  REMOVE n:C:D
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test(
    """
      |MATCH (n)
      |CALL {
      |  WITH n
      |  SET n:A:B
      |  REMOVE n:C:D
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test(
    """
      |CALL {
      |  CREATE (n:A)
      |  REMOVE n:C:D
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test(
    """
      |CALL {
      |  MATCH (n:A&B)
      |  REMOVE n:C:D
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test(
    """
      |CALL {
      |  MATCH (n:A&B)
      |  SET n:C:D
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test(
    """
      |CALL {
      |  MATCH (n:A&B)
      |  CREATE (m:A:B)
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test(
    """
      |CALL {
      |  MATCH (n:A:B)
      |  CREATE (m:A&B)
      |}
      |""".stripMargin
  ) {
    runSemanticAnalysis().errorMessages shouldBe empty
  }
}
