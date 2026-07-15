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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.frontend.NameBasedSemanticAnalysisTestSuite
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.scalatest.LoneElement

class OtherLabelExpressionSemanticAnalysisTest extends NameBasedSemanticAnalysisTestSuite with LoneElement {

  test("MATCH (a), (b) WITH shortestPath((a:A|B)-[:REL*]->(b:B|C)) AS p RETURN length(p) AS result") {
    run().hasError(
      GqlHelper.getGql42001_42I30("shortestPath", 20, 1, 21),
      "Label expressions in shortestPath are not allowed in an expression",
      InputPosition(20, 1, 21)
    )
  }

  test("MATCH (a), (b) WITH shortestPath((a IS A)-[:REL*]->(b:B)) AS p RETURN length(p) AS result") {
    run().hasNoErrors
  }

  test("MATCH (a), (b) WITH shortestPath((a:A)-[:A*]->(b:B)) AS p RETURN length(p) AS result") {
    run().hasNoErrors
  }

  test("MATCH (n), (m) WITH shortestPath((n)-[:A|B|C*]->(m)) AS p RETURN length(p) AS result") {
    run().hasNoErrors
  }

  test("MATCH (n), (m) WITH shortestPath((n)-[:!A&!B*]->(m)) AS p RETURN length(p) AS result") {
    run().hasError(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(41, 1, 42)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I41)
          .atPosition(41, 1, 42)
          .withParam(GqlParams.StringParam.value, "combination with relationship type expressions")
          .build())
        .build(),
      "Variable length relationships must not use relationship type expressions.",
      InputPosition(41, 1, 42)
    )
  }

  test("MATCH (n), (m) WITH shortestPath((n)-[IS A*]->(m)) AS p RETURN length(p) AS result") {
    run().hasNoErrors
  }

  test("MATCH (n), (m) WITH COLLECT { MATCH p = (n)-[:!A&!B*]->(m) RETURN p } AS p RETURN p AS result") {
    run().hasErrorMessages("Variable length relationships must not use relationship type expressions.")
  }

  test("MATCH (a), (b) RETURN [(a:A|B)-[:REL*]->(b IS B) | 1] AS p") {
    run().hasNoErrors
  }

  // LabelExpressionPredicate

  // Node

  test("MATCH (n:A:B)-[r]->() WITH [r, n] AS list UNWIND list as x RETURN x:A|B") {
    run().hasNoErrors
  }

  // Unknown

  test("RETURN $param:A:B") {
    // should allow colon conjunction on unknown type
    run().hasNoErrors
  }

  test("RETURN $param:A|B") {
    // should allow disjunction on unknown type
    run().hasNoErrors
  }

  test("RETURN $param:A|:B") {
    run()
      .hasErrorMessages(
        """The semantics of using colon in the separation of alternative relationship types in conjunction with
          |the use of variable binding, inlined property predicates, or variable length is no longer supported.
          |Please separate the relationships types using `:A|B` instead.""".stripMargin
      )
      .assert(r => checkGqlDisjunctionError(r.errors.loneElement, "|:"))
  }

  test("RETURN $param:A:B&C") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B&C", 15, 1, 16),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B&C.",
      InputPosition(15, 1, 16)
    )
  }

  test("RETURN $param:A|B:C") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A|(B&C)", 17, 1, 18),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A|(B&C).",
      InputPosition(17, 1, 18)
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
    run().hasNoErrors
  }

  // Mixed label expression in same statement
  test("MATCH ((n:A:B:C)-[]->()) RETURN n:A&B, n:A:B") {
    run().hasError(
      GqlHelper.getGql42001_42I10(":A&B", 42, 1, 43),
      "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as :A&B.",
      InputPosition(42, 1, 43)
    )
  }

  ignore(
    """
      |CALL {
      |  CREATE (n:A&B)
      |  SET n:C:D
      |}
      |""".stripMargin
  ) {
    run()
      .hasErrorMessages(
        "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as multiple comma separated items which one Label each."
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
    run()
      .hasErrorMessages(
        "Mixing label expression symbols ('|', '&', '!', and '%') with colon (':') between labels is not allowed. Please only use one set of symbols. This expression could be expressed as multiple comma separated items which one Label each."
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
    run()
      .hasErrorMessages(
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
    run().hasNoErrors
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
    run().hasNoErrors
  }

  test(
    """
      |CALL {
      |  CREATE (n:A)
      |  REMOVE n:C:D
      |}
      |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    """
      |CALL {
      |  MATCH (n:A&B)
      |  REMOVE n:C:D
      |}
      |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    """
      |CALL {
      |  MATCH (n:A&B)
      |  SET n:C:D
      |}
      |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    """
      |CALL {
      |  MATCH (n:A&B)
      |  CREATE (m:A:B)
      |}
      |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    """
      |CALL {
      |  MATCH (n:A:B)
      |  CREATE (m:A&B)
      |}
      |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    """MATCH (n)
      |WITH [x IN [n] WHERE x:$("A")] AS labelCheck
      |RETURN labelCheck""".stripMargin
  ) {
    runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher25)).hasErrorMessages(
      "Dynamic Label and Types are only allowed in MATCH, CREATE, MERGE, SET and REMOVE clauses."
    )
    runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    """
      |MATCH (n)
      |WHERE n:$("A")
      |RETURN n
      |""".stripMargin
  ) {
    runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher25)).hasErrorMessages(
      "Dynamic Label and Types are only allowed in MATCH, CREATE, MERGE, SET and REMOVE clauses."
    )
    runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    """
      |MATCH (n)
      |RETURN n:$("A")
      |""".stripMargin
  ) {
    runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher25)).hasErrorMessages(
      "Dynamic Label and Types are only allowed in MATCH, CREATE, MERGE, SET and REMOVE clauses."
    )
    runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    """
      |MATCH (n WHERE n:$("A"))
      |RETURN n
      |""".stripMargin
  ) {
    runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher25)).hasErrorMessages(
      "Dynamic Label and Types are only allowed in MATCH, CREATE, MERGE, SET and REMOVE clauses."
    )
    runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }

  test(
    """
      |MATCH ()-[r WHERE r:$("A")]->()
      |RETURN r
      |""".stripMargin
  ) {
    runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher25)).hasErrorMessages(
      "Dynamic Label and Types are only allowed in MATCH, CREATE, MERGE, SET and REMOVE clauses."
    )
    runWith(testName, disabledCypherVersions = Set(CypherVersion.Cypher5)).hasNoErrors
  }
}
