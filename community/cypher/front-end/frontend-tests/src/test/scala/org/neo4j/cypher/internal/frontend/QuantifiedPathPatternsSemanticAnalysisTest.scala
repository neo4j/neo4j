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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.p
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.varFor
import org.neo4j.cypher.internal.frontend.label_expressions.UpdateStatement
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

import scala.jdk.CollectionConverters.SeqHasAsJava

abstract class QuantifiedPathPatternsInDifferentClausesSemanticAnalysisTest(statement: UpdateStatement)
    extends CypherFunSuite
    with SemanticAnalysisTestSuiteWithDefaultQuery
    with TestName {

  override def defaultQuery: String = s"$statement $testName"

  test("((a)-[:Rel]->(b)){2}") {
    val statementOffset = s"$statement ".length
    run().hasError(
      GqlHelper.getGql42001_42I04(testName, statement.toString, statementOffset, 1, statementOffset + 1),
      s"Quantified path patterns cannot be used in a $statement clause, but only in a MATCH clause.",
      InputPosition(statementOffset, 1, statementOffset + 1)
    )
  }
}

class QuantifiedPathPatternsInCreateClausesSemanticAnalysisTest
    extends QuantifiedPathPatternsInDifferentClausesSemanticAnalysisTest(UpdateStatement.CREATE)

class QuantifiedPathPatternsInMergeClausesSemanticAnalysisTest
    extends QuantifiedPathPatternsInDifferentClausesSemanticAnalysisTest(UpdateStatement.MERGE)

class QuantifiedPathPatternsSemanticAnalysisTest extends NameBasedSemanticAnalysisTestSuite {

  test("MATCH ((a)-[]->(b))+ RETURN a") {
    run()
      .hasNoErrors
      .assert(_.semanticTable.types(varFor("a", p(8, 1, 9))).specified shouldBe CTNode.invariant)
      .assert(_.semanticTable.types(varFor("a", p(28, 1, 29))).specified shouldBe CTList(CTNode).invariant)
  }

  test("MATCH (p = (a)-[]->(b))+ RETURN p") {
    run().hasError(
      GqlHelper.getGql42001_42N34(7, 1, 8),
      "Assigning a path in a quantified path pattern is not yet supported.",
      InputPosition(7, 1, 8)
    )
  }

  test("MATCH (a) (()--(x {prop: a.prop}))+ (b) (()--())+ (c) RETURN *") {
    run().hasNoErrors
  }

  test("MERGE (var0 WHERE COUNT { ((var1)--())+ } > 1 ) RETURN *") {
    // This test asserts that we give semantic errors instead of throwing "java.util.NoSuchElementException: key not found"
    run().hasErrors(
      GqlHelper.getGql42001_42I32("a MERGE clause", 42, 1, 43),
      "Node pattern predicates are not allowed in a MERGE clause, but only in a MATCH clause or inside a pattern comprehension",
      InputPosition(42, 1, 43),
      GqlHelper.getGql42001_42I48(18, 1, 19),
      "Subquery expressions are not allowed in a MERGE clause.",
      InputPosition(18, 1, 19)
    )
  }

  test("MATCH (p = (a)--(b))+ (p = (c)--(d))+ RETURN p") {
    run().hasAtLeastOneGqlErrorIn(_ =>
      Seq(
        (
          GqlHelper.getGql42001_42N59("p", 7, 1, 8),
          "The variable `p` occurs in multiple quantified path patterns and needs to be renamed.",
          InputPosition(7, 1, 8)
        ),
        (
          GqlHelper.getGql42001_42N34(7, 1, 8),
          "Assigning a path in a quantified path pattern is not yet supported.",
          InputPosition(7, 1, 8)
        ),
        (
          GqlHelper.getGql42001_42N34(23, 1, 24),
          "Assigning a path in a quantified path pattern is not yet supported.",
          InputPosition(23, 1, 24)
        ),
        (
          GqlHelper.getGql42001_42N59("p", 22, 1, 23),
          "Variable `p` already declared",
          InputPosition(22, 1, 23)
        )
      )
    )
  }

  test("MATCH (p = (a)--(b))+ (p = (c)--(d)) RETURN p") {
    run().hasErrors(
      GqlHelper.getGql42001_42N34(7, 1, 8),
      "Assigning a path in a quantified path pattern is not yet supported.",
      InputPosition(7, 1, 8),
      GqlHelper.getGql42001_42N59("p", 23, 1, 24),
      "Variable `p` already declared",
      InputPosition(23, 1, 24),
      GqlHelper.getGql42001_42N42(23, 1, 24),
      "Sub-path assignment is currently not supported.",
      InputPosition(23, 1, 24)
    )
  }

  test("MATCH (p = (a)--(b))+ MATCH (p = (c)--(d))+ RETURN p") {
    run().hasAtLeastOneGqlErrorIn(_ =>
      Seq(
        (
          GqlHelper.getGql42001_42N34(7, 1, 8),
          "Assigning a path in a quantified path pattern is not yet supported.",
          InputPosition(7, 1, 8)
        ),
        (
          GqlHelper.getGql42001_42N34(29, 1, 30),
          "Assigning a path in a quantified path pattern is not yet supported.",
          InputPosition(29, 1, 30)
        ),
        (
          GqlHelper.getGql42001_42N59("p", 29, 1, 30),
          "The variable `p` is already defined in a previous clause, it cannot be referenced as a node or as a relationship variable inside of a quantified path pattern.",
          InputPosition(29, 1, 30)
        ),
        (
          GqlHelper.getGql42001_42N59("p", 28, 1, 29),
          "Variable `p` already declared",
          InputPosition(28, 1, 29)
        )
      )
    )
  }

  test("MATCH p = (p = (a)--(b))+ (c)--(d) RETURN p") {
    run().hasErrors(
      GqlHelper.getGql42001_42N34(11, 1, 12),
      "Assigning a path in a quantified path pattern is not yet supported.",
      InputPosition(11, 1, 12),
      GqlHelper.getGql42001_42N59("p", 6, 1, 7),
      "Variable `p` already declared",
      InputPosition(6, 1, 7)
    )
  }

  // nested shortest path
  test("MATCH (p = shortestPath((a)-[]->(b)))+ RETURN p") {
    run().hasErrors(
      GqlHelper.getGql42001_42N34(7, 1, 8),
      "Assigning a path in a quantified path pattern is not yet supported.",
      InputPosition(7, 1, 8),
      GqlHelper.getGql42001_42N69("shortestPath", "quantified path pattern", 11, 1, 12),
      "shortestPath(...) is only allowed as a top-level element and not inside a quantified path pattern",
      InputPosition(11, 1, 12),
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(27, 1, 28)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I41)
          .atPosition(27, 1, 28)
          .withParam(
            GqlParams.StringParam.value,
            "combination with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*')"
          )
          .build())
        .build(),
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed.",
      InputPosition(27, 1, 28)
    )
  }

  test("MATCH shortestPath( ((a)-[]->(b))+ ) RETURN count(*)") {
    run().hasErrors(
      GqlHelper.getGql42001_42I23("shortestPath", 20, 1, 21),
      "shortestPath(...) contains quantified pattern. This is currently not supported.",
      InputPosition(20, 1, 21),
      GqlHelper.getGql42001_42N40("shortestPath", 6, 1, 7),
      "shortestPath(...) requires a pattern containing a single relationship",
      InputPosition(6, 1, 7)
    )
  }

  test("MATCH (shortestPath((a)-[]->(b))) RETURN count(*)") {
    run().hasError(
      GqlHelper.getGql42001_42N69("shortestPath", "parenthesized path pattern", 7, 1, 8),
      // this is the error message that we ultimately expect
      "shortestPath(...) is only allowed as a top-level element and not inside a parenthesized path pattern",
      InputPosition(7, 1, 8)
    )
  }

  test("MATCH shortestPath((n)-[]->+({s: 1})) RETURN count(*)") {
    run().hasErrors(
      GqlHelper.getGql42001_42I23("shortestPath", 22, 1, 23),
      "shortestPath(...) contains quantified pattern. This is currently not supported.",
      InputPosition(22, 1, 23),
      GqlHelper.getGql42001_42N40("shortestPath", 6, 1, 7),
      "shortestPath(...) requires a pattern containing a single relationship",
      InputPosition(6, 1, 7)
    )
  }

  // minimum node count
  test("MATCH ((a)-[]->(b)){0,} RETURN count(*)") {
    run().hasError(
      GqlHelper.getGql42001_42N64(6, 1, 7),
      """A top-level path pattern in a `MATCH` clause must be written such that it always evaluates to at least one node pattern.
        |In this case, `((a)-->(b)){0}` would result in an empty pattern.""".stripMargin,
      InputPosition(6, 1, 7)
    )
  }

  test("MATCH ((a)-[]->(b))* RETURN count(*)") {
    run().hasError(
      GqlHelper.getGql42001_42N64(6, 1, 7),
      """A top-level path pattern in a `MATCH` clause must be written such that it always evaluates to at least one node pattern.
        |In this case, `((a)-->(b)){0}` would result in an empty pattern.""".stripMargin,
      InputPosition(6, 1, 7)
    )
  }

  test("MATCH ((a)-[]->(b)){0,}((c)-[]->(d)){0,} RETURN count(*)") {
    run().hasError(
      GqlHelper.getGql42001_42N64(6, 1, 7),
      """A top-level path pattern in a `MATCH` clause must be written such that it always evaluates to at least one node pattern.
        |In this case, `((a)-->(b)){0} ((c)-->(d)){0}` would result in an empty pattern.""".stripMargin,
      InputPosition(6, 1, 7)
    )
  }

  test("MATCH ((a)-[]->(b)){0,}((c)-[]->(d)){1,} RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ((a)-[]->(b)){1,}((c)-[]->(d)){0,} RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (x)((a)-[]->(b)){0, } RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ((a)-[]->(b)){1,} RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ((a)-[]->(b)){0, 5}(y) RETURN count(*)") {
    run().hasNoErrors
  }

  // quantifier
  test("MATCH (x)((a)-[]->(b)){0} RETURN count(*)") {
    run().hasErrorMessages(
      "A quantifier for a path pattern must not be limited by 0."
    )
  }

  test("MATCH (x)((a)-[]->(b)){,0} RETURN count(*)") {
    run().hasErrorMessages(
      "A quantifier for a path pattern must not be limited by 0."
    )
  }

  test("MATCH (x)((a)-[]->(b)){2,1} RETURN count(*)") {
    run().hasError(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(22, 1, 23)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I17)
          .atPosition(22, 1, 23)
          .build()).build(),
      """A quantifier for a path pattern must not have a lower bound which exceeds its upper bound.
        |In this case, the lower bound 2 is greater than the upper bound 1.""".stripMargin,
      InputPosition(22, 1, 23)
    )
  }

  test("MATCH (a)-[]->{9223372036854775808}(b) RETURN count(*)") {
    run().hasErrorMessages(
      "integer is too large"
    )
  }

  test("MATCH (a)-[]->{1, 9223372036854775808}(b) RETURN count(*)") {
    run().hasErrorMessages(
      "integer is too large"
    )
  }

  test("MATCH (a)-[]->{9223372036854775808,}(b) RETURN count(*)") {
    run().hasErrorMessages(
      "integer is too large"
    )
  }

  test("MATCH (x) ((a)-[]->(b)){0, 1_000_000} RETURN count(*)") {
    run().hasNoErrors
  }

  // single node pattern
  test("MATCH ((n)){1, 5} RETURN count(*)") {
    run().hasError(
      GqlHelper.getGql42001_42N64(6, 1, 7),
      """A quantified path pattern needs to have at least one relationship.
        |In this case, the quantified path pattern ((n)){1, 5} consists of only one node.""".stripMargin,
      InputPosition(6, 1, 7)
    )
  }

  test("MATCH ((n) (m)){1, 5} RETURN count(*)") {
    run().hasErrors(
      GqlHelper.getGql42001_42N64(6, 1, 7),
      // this is the error message that we ultimately expect
      """A quantified path pattern needs to have at least one relationship.
        |In this case, the quantified path pattern ((n) (m)){1, 5} consists of only nodes.""".stripMargin,
      InputPosition(6, 1, 7),
      GqlHelper.getGql42001_42I46(11, 1, 12),
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, both (n) and (m) are single nodes.
        |That is, neither of these is a quantified path pattern.""".stripMargin,
      InputPosition(11, 1, 12)
    )
  }

  test("MATCH (x) (((a)-[b]->(c))*)+ RETURN count(*)") {
    run().hasError(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(11, 1, 12)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I12)
          .atPosition(11, 1, 12)
          .build())
        .build(),
      "Quantified path patterns are not allowed to be nested.",
      InputPosition(11, 1, 12)
    )
  }

  test("MATCH ((a)-->(b)-[r]->*(c))+ RETURN count(*)") {
    run().hasError(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(16, 1, 17)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I12)
          .atPosition(16, 1, 17)
          .build())
        .build(),
      "Quantified path patterns are not allowed to be nested.",
      InputPosition(16, 1, 17)
    )
  }

  test("MATCH ((a)-[*]->(b))+ RETURN count(*)") {
    run().hasErrors(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(10, 1, 11)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I41)
          .atPosition(10, 1, 11)
          .withParam(GqlParams.StringParam.value, "a quantified path pattern")
          .build())
        .build(),
      "Variable length relationships cannot be part of a quantified path pattern.",
      InputPosition(10, 1, 11),
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(10, 1, 11)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I41)
          .atPosition(10, 1, 11)
          .withParam(
            GqlParams.StringParam.value,
            "combination with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*')"
          )
          .build())
        .build(),
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed.",
      InputPosition(10, 1, 11)
    )
  }

  // relationship quantification
  test("MATCH (a)-[*]->+(b) RETURN count(*)") {
    run().hasErrors(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(9, 1, 10)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I41)
          .atPosition(9, 1, 10)
          .withParam(GqlParams.StringParam.value, "a quantified path pattern")
          .build())
        .build(),
      "Variable length relationships cannot be part of a quantified path pattern.",
      InputPosition(9, 1, 10),
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(9, 1, 10)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I41)
          .atPosition(9, 1, 10)
          .withParam(
            GqlParams.StringParam.value,
            "combination with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*')"
          )
          .build())
        .build(),
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed.",
      InputPosition(9, 1, 10)
    )
  }

  test("MATCH (a)-[r]->*(b) RETURN count(*)") {
    run().hasNoErrors
  }

  // variable overlap
  test("MATCH ((a)-->(b)-->(a)-->(c))+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (c) ((a)-->(b))+ (d)-->(c) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ((a)-->(b))+ ((b)-->(c))+ RETURN count(*)") {
    run().hasAtLeastOneErrorMessageIn(
      "The variable `b` occurs in multiple quantified path patterns and needs to be renamed.",
      "Type mismatch: b defined with conflicting type List<Node> (expected Node)",
      "Variable `b` already declared"
    )
  }

  test("MATCH (()-[r]->())+ (()-[r]->())+ RETURN count(*)") {
    run().hasAtLeastOneErrorMessageIn(
      "The variable `r` occurs in multiple quantified path patterns and needs to be renamed.",
      "Type mismatch: r defined with conflicting type List<Relationship> (expected Relationship)",
      "Variable `r` already declared"
    )
  }

  test("MATCH ((a)-[b]->(c))* (d)-[e]->(a) RETURN count(*)") {
    run().hasAtLeastOneErrorMessageIn(
      "The variable `a` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      "Type mismatch: a defined with conflicting type List<Node> (expected Node)"
    )
  }

  test("MATCH (a)-[e]->(d) ((a)-[b]->(c))*  RETURN count(*)") {
    run().hasAtLeastOneErrorMessageIn(
      "The variable `a` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      "Variable `a` already declared"
    )
  }

  test("MATCH (()-[r]->())* ()-[r]->() RETURN count(*)") {
    run().hasAtLeastOneErrorMessageIn(
      "The variable `r` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      "Type mismatch: r defined with conflicting type List<Relationship> (expected Relationship)"
    )
  }

  test("MATCH ()-[r]->() (()-[r]->())*  RETURN count(*)") {
    run().hasAtLeastOneErrorMessageIn(
      "The variable `r` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      "Variable `r` already declared"
    )
  }

  test("MATCH ((a)-[b]->(c))* (d)-[e]->()((a)-[f]->(g)){2,} RETURN count(*)") {
    run().hasAtLeastOneErrorMessageIn(
      "The variable `a` occurs in multiple quantified path patterns and needs to be renamed.",
      "Type mismatch: a defined with conflicting type List<Node> (expected Node)",
      "Variable `a` already declared"
    )
  }

  test("MATCH ((a)-[b]->(c))* (d)-[b]->+(f) RETURN count(*)") {
    run().hasAtLeastOneErrorMessageIn(
      "The variable `b` occurs in multiple quantified path patterns and needs to be renamed.",
      "Type mismatch: b defined with conflicting type List<Relationship> (expected Relationship)",
      "Variable `b` already declared"
    )
  }

  test("MATCH (a)-->(b) MATCH (x)--(y) ((a)-->(t)){1,5} ()-->(z) RETURN count(*)") {
    run().hasAtLeastOneGqlErrorIn(_ =>
      Seq(
        (
          GqlHelper.getGql42001_42N59("a", 33, 1, 34),
          "The variable `a` is already defined in a previous clause, it cannot be referenced as a node or as a relationship variable inside of a quantified path pattern.",
          InputPosition(33, 1, 34)
        ),
        (
          GqlHelper.getGql42001_42N59("a", 33, 1, 34),
          "Variable `a` already declared",
          InputPosition(33, 1, 34)
        ),
        (
          GqlHelper.getGql42001_42N59("a", 31, 1, 32),
          "Variable `a` already declared",
          InputPosition(31, 1, 32)
        )
      )
    )
  }

  test("MATCH ((a)-->(b))+ MATCH (x)--(y) ((a)-->(t)){1,5} ()-->(z) RETURN count(*)") {
    run().hasAtLeastOneErrorMessageIn(
      "The variable `a` is already defined in a previous clause, it cannot be referenced as a node or as a relationship variable inside of a quantified path pattern.",
      "Type mismatch: a defined with conflicting type List<Node> (expected Node)",
      "Variable `a` already declared"
    )
  }

  // parenthesized path patterns
  test("MATCH ((a)-->(b)) (x) RETURN x") {
    run().hasError(
      GqlHelper.getGql42001_42I46(18, 1, 19),
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, ((a)-->(b)) is a (non-quantified) parenthesized path pattern and (x) is a single node.
        |That is, neither of these is a quantified path pattern.""".stripMargin,
      InputPosition(18, 1, 19)
    )
  }

  test("MATCH (x) ((a)-->(b)) RETURN x") {
    run().hasError(
      GqlHelper.getGql42001_42I46(10, 1, 11),
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, (x) is a single node and ((a)-->(b)) is a (non-quantified) parenthesized path pattern.
        |That is, neither of these is a quantified path pattern.""".stripMargin,
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH ((a)-->(b)) (x)-->(y) RETURN x") {
    run().hasError(
      GqlHelper.getGql42001_42I46(18, 1, 19),
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, ((a)-->(b)) is a (non-quantified) parenthesized path pattern and (x)-->(y) is a simple path pattern.
        |That is, neither of these is a quantified path pattern.""".stripMargin,
      InputPosition(18, 1, 19)
    )
  }

  test("MATCH ((a)-->(b)) ((x)-->(y)) RETURN x") {
    run().hasError(
      GqlHelper.getGql42001_42I46(18, 1, 19),
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, both ((a)-->(b)) and ((x)-->(y)) are (non-quantified) parenthesized path patterns.
        |That is, neither of these is a quantified path pattern.""".stripMargin,
      InputPosition(18, 1, 19)
    )
  }

  test("MATCH (x) (y) RETURN x") {
    run().hasError(
      GqlHelper.getGql42001_42I46(10, 1, 11),
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, both (x) and (y) are single nodes.
        |That is, neither of these is a quantified path pattern.""".stripMargin,
      InputPosition(10, 1, 11)
    )
  }

  test("MATCH ((a)-->(b)) ((x)-->(y))* RETURN x") {
    run().hasNoErrors
  }

  test("MATCH (p = (a)-->(b)) ((x)-->(y))* RETURN x") {
    run().hasError(
      GqlHelper.getGql42001_42N42(7, 1, 8),
      "Sub-path assignment is currently not supported.",
      InputPosition(7, 1, 8)
    )
  }

  // Predicates

  test("MATCH ((a)-->(b) WHERE b.prop > 7)+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ((a)-->(b) WHERE a.prop < b.prop)+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ((a)-[:R]->(b) WHERE (a)-[:S]->(b))+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ((a)-[:R]->(b) WHERE EXISTS { MATCH (a)-[:S]->(b) })+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH ((a)-[:R]->(b) WHERE COUNT { MATCH (a)-[:S]->(b) } > 1)+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (x) MATCH ((a)-->(b) WHERE a.prop < x.prop)+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (x) MATCH ((a WHERE a.prop < x.prop)-->(b))+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (x) MATCH ((a)-[r:REL WHERE r.prop < x.prop]->(b))+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (a) MATCH p = ( ()--() WHERE EXISTS { (a) } OR true AND false )+ RETURN 1") {
    run().hasNoErrors
  }

  test("MATCH p = ( ()--() WHERE EXISTS { () } OR true AND false )+ RETURN 1") {
    run().hasNoErrors
  }

  test("MATCH (n), p = ( ()--() WHERE EXISTS { (n) } OR true AND false )+ RETURN 1") {
    run().hasNoErrors
  }

  // accessing non-local variables outside of the quantification
  test("MATCH (x)-->(y)((a)-[e]->(b) WHERE a.h > x.h)* (s)-->(u) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (x)-->(y)((a)-[e]->(b) WHERE a.h > u.h)* (s)-->(u) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (x)-->(y)((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (x)-->(y), ((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (x) ((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (x) ((a)-[e {h: x.h}]->(b))* (s)-->(u) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (x)-->(y) ((a)-[e]->(b {h: u.h}))* (s)-->(u) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH p=(x)-->(y), ((a)-[e]->(b {h: nodes(p)[0].prop}))* (s)-->(u) RETURN count(*)") {
    run().hasAtLeastOneGqlErrorIn(_ =>
      Seq((
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(6, 1, 7)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I21)
            .atPosition(6, 1, 7)
            .withParam(GqlParams.ListParam.variableList, Seq("p").asJava)
            .withParam(GqlParams.StringParam.pat, "((a)-[e]->(b {h: (nodes(p)[0]).prop}))*")
            .build())
          .build(),
        """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
          |In this case, `p` is defined in the same `MATCH` clause as ((a)-[e]->(b {h: (nodes(p)[0]).prop}))*.""".stripMargin,
        InputPosition(6, 1, 7)
      ))
    )
  }

  test("References from within QPP to other path pattern") {
    val query =
      """MATCH
        |  (a) ((b)-[r]-(c) WHERE i.prop = r.prop)+ (d)--(e),
        |  (f)--(d) ((g)-[s]-(h) WHERE s.prop = a.prop)+ (i)
        |RETURN count(*)""".stripMargin

    run(query).hasNoErrors
  }

  test("MATCH (x)-->(y) MATCH (y) ((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (s)-->(u) MATCH (x)-->(y)((a)-[e]->(b {h: u.h}))* (s) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (a), (b) MATCH (a) ((n)-[]->(m) WHERE n.prop > a.prop AND n.prop > b.prop)+ (b) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (a), (b) MATCH (a2) ((n)-[]->(m) WHERE ALL(a IN n.prop WHERE a > 2) )+ (b2) RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH (a) ((n)-[]->(m) WHERE ALL(a IN n.prop WHERE a > 2) )+ (b) RETURN count(*)") {
    run().hasNoErrors
  }

  // access group variables without aggregation
  test("MATCH (x)-->(y)((a)-[e]->(b))+(s)-->(u) WHERE e.weight < 4 RETURN count(*)") {
    run().hasErrorMessages(
      "Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was List<Relationship>"
    )
  }

  // path assignment with quantified path patterns
  test("MATCH p = ((a)-[]->(b))+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("MATCH p = (x)-->(y) ((a)-[]->(b))+ RETURN count(*)") {
    run().hasNoErrors
  }

  test("path assignment with predicate referring to path") {
    val query =
      """MATCH p = (a) ((b)-[r]-(c) WHERE r.prop = length(p))+ (d)
        |RETURN p""".stripMargin
    run(query).hasAllErrorMessagesIn(_ =>
      Seq(
        """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
          |In this case, `p` is defined in the same `MATCH` clause as ((b)-[r]-(c) WHERE r.prop = length(p))+.""".stripMargin
      )
    )
  }

  // Mixing with legacy var-length

  // Different clauses
  test("MATCH (x)-[*]->(y) MATCH ((a)-[]->(b))+ RETURN count(*)") {
    run().hasNoErrors
  }

  // Mixed quantifier in same pattern element
  test("MATCH (x)-[*]->(y) ((a)-[]->(b))+ RETURN count(*)") {
    run().hasErrorMessages(
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // Two legacy var-length
  test("MATCH (x)-[*]->(y) ((a)-[]->(b))+ (n)-[*]->(m) RETURN count(*)") {
    run().hasErrorMessages(
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // Mixed quantifier in same clause
  test("MATCH (x)-[*]->(y), ((a)-[]->(b))+ RETURN count(*)") {
    run().hasErrorMessages(
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // Mixed quantifier (quantified relationship) in same clause
  test("MATCH (n) RETURN [(n)-->+(m) | m], [(n)-[*3]-(m) | m]") {
    // quantified relationships are not implemented yet. Once this is the case, please change to the test below
    run().failsWithMessageContaining("Invalid input '+': expected")
    // run().hasErrorMessages(
    //   "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed. This relationship can be expressed as '--{3}'"
    // )
  }

  // ... on same element pattern
  test("MATCH ()-[r:A*]->*() RETURN r") {
    run().hasErrors(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(8, 1, 9)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I41)
          .atPosition(8, 1, 9)
          .withParam(GqlParams.StringParam.value, "a quantified path pattern")
          .build())
        .build(),
      "Variable length relationships cannot be part of a quantified path pattern.",
      InputPosition(8, 1, 9),
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(8, 1, 9)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I41)
          .atPosition(8, 1, 9)
          .withParam(
            GqlParams.StringParam.value,
            "combination with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*')"
          )
          .build())
        .build(),
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed.",
      InputPosition(8, 1, 9)
    )
  }

  test("MATCH ()-[r:A*1..2]->{1,2}() RETURN r") {
    run().hasErrors(
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(8, 1, 9)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I41)
          .atPosition(8, 1, 9)
          .withParam(GqlParams.StringParam.value, "a quantified path pattern")
          .build())
        .build(),
      "Variable length relationships cannot be part of a quantified path pattern.",
      InputPosition(8, 1, 9),
      ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
        .atPosition(8, 1, 9)
        .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I41)
          .atPosition(8, 1, 9)
          .withParam(
            GqlParams.StringParam.value,
            "combination with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*')"
          )
          .build())
        .build(),
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed.",
      InputPosition(8, 1, 9)
    )
  }

  // ... in different statements
  test("MATCH (s)-[:A*2..2]->(n) MATCH (n)-[:B]->{2}(t) RETURN s.p AS sp, t.p AS tp") {
    run().hasNoErrors
  }

  // should not throw error about mixing if they are in different scopes
  test("MATCH ((a)--(b) WHERE EXISTS { (c)-[r*]-(d) })+ RETURN 1") {
    run().hasNoErrors
  }

  test("MATCH ((a)--(b WHERE EXISTS { (c)-[r*]-(d) }))+ RETURN 1") {
    run().hasNoErrors
  }

  test("MATCH (a)-[r*]-(b WHERE EXISTS { (a)(()-[r1]->())*(b) }) RETURN 1") {
    run().hasNoErrors
  }

  test(
    """MATCH (n)
      |CALL {
      |  MATCH ((a)--(b))+
      |  MATCH (c)-[r*]-(d)
      |  RETURN *
      |}
      |RETURN 1""".stripMargin
  ) {
    run().hasNoErrors
  }

  // pattern comprehension
  test("MATCH (n) WITH [ p = (n)--(m) ((a)-->(b))+  | p ] as paths RETURN count(*)") {
    // this currently fails with a parse error
    run().failsWithMessageContaining("Invalid input")
  }

  // this query may not be super useful but at least it works w/o juxtaposition
  test("MATCH (n) WITH [ p = ((a)-->(b))+  | p ] as paths RETURN count(*)") {
    // this currently fails with a parse error
    run().failsWithMessageContaining("Invalid input")
  }

  // pattern expression
  test("MATCH (n) WHERE (n)--() (()-->())+ RETURN count(*)") {
    // this currently fails with a parse error
    run().failsWithMessageContaining("Invalid input")
  }

  // this query may not be super useful but at least it works w/o juxtaposition
  test("MATCH (n) WHERE (()-->())+ RETURN count(*)") {
    // this currently fails with a parse error
    run().failsWithMessageContaining("Invalid input")
  }
}
