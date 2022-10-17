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
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory.SyntaxException
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

abstract class QuantifiedPathPatternsInDifferentClausesSemanticAnalysisTest(statement: Statement)
    extends CypherFunSuite
    with SemanticAnalysisTestSuiteWithDefaultQuery
    with TestName {

  override def defaultQuery: String = s"$statement $testName"

  test("((a)-[:Rel]->(b)){2}") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      s"Quantified path patterns are not allowed in $statement, but only in MATCH clause."
    )
  }
}

class QuantifiedPathPatternsInCreateClausesSemanticAnalysisTest
    extends QuantifiedPathPatternsInDifferentClausesSemanticAnalysisTest(Statement.CREATE)

class QuantifiedPathPatternsInMergeClausesSemanticAnalysisTest
    extends QuantifiedPathPatternsInDifferentClausesSemanticAnalysisTest(Statement.MERGE)

class QuantifiedPathPatternsSemanticAnalysisTest extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  test("MATCH ((a)-->(b))+ RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Quantified path patterns are not yet supported."
    )
  }

  test("MATCH ((a)-[]->(b))+ RETURN a") {
    val result = runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns)
    result.errors shouldBe empty
    val innerA = Variable("a")(InputPosition(8, 1, 9))
    result.semanticTable.types(innerA).specified shouldBe CTNode.invariant
    val outerA = Variable("a")(InputPosition(28, 1, 29))
    result.semanticTable.types(outerA).specified shouldBe CTList(CTNode).invariant
  }

  test("MATCH (p = (a)-[]->(b))+ RETURN p") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Assigning a path in a quantified path pattern is not yet supported."
    )
  }

  test("MATCH (p = (a)--(b))+ (p = (c)--(d))+ RETURN p") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Assigning a path in a quantified path pattern is not yet supported.",
      "Assigning a path in a quantified path pattern is not yet supported.",
      "Variable `p` already declared"
    )
  }

  test("MATCH (p = (a)--(b))+ (p = (c)--(d)) RETURN p") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Assigning a path in a quantified path pattern is not yet supported.",
      "Type mismatch: p defined with conflicting type List<T> (expected Path)",
      "Sub-path assignment is currently not supported outside quantified path patterns."
    )
  }

  test("MATCH (p = (a)--(b))+ MATCH (p = (c)--(d))+ RETURN p") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Assigning a path in a quantified path pattern is not yet supported.",
      "Assigning a path in a quantified path pattern is not yet supported.",
      "Variable `p` already declared"
    )
  }

  test("MATCH p = (p = (a)--(b))+ (c)--(d) RETURN p") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Assigning a path in a quantified path pattern is not yet supported.",
      "Variable `p` already declared",
      "Assigning a path with a quantified path pattern is not yet supported."
    )
  }

  // nested shortest path
  test("MATCH (p = shortestPath((a)-[]->(b)))+ RETURN p") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Assigning a path in a quantified path pattern is not yet supported.",
      "shortestPath is only allowed as a top-level element and not inside a quantified path pattern",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  test("MATCH shortestPath( ((a)-[]->(b))+ ) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "shortestPath(...) requires a pattern containing a single relationship"
    )
  }

  test("MATCH (shortestPath((a)-[]->(b))) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      // this is the error message that we ultimately expect
      "shortestPath is only allowed as a top-level element and not inside a parenthesized path pattern"
    )
  }

  test("MATCH shortestPath((n)-[]->+({s: 1})) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldBe Seq(
      "shortestPath(...) requires a pattern containing a single relationship"
    )
  }

  // minimum node count
  test("MATCH ((a)-[]->(b)){0,} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """A top-level path pattern in a `MATCH` clause must be written such that it always evaluates to at least one node pattern.
        |In this case, `((a)-->(b)){0}` would result in an empty pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-[]->(b))* RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """A top-level path pattern in a `MATCH` clause must be written such that it always evaluates to at least one node pattern.
        |In this case, `((a)-->(b)){0}` would result in an empty pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-[]->(b)){0,}((c)-[]->(d)){0,} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """A top-level path pattern in a `MATCH` clause must be written such that it always evaluates to at least one node pattern.
        |In this case, `((a)-->(b)){0} ((c)-->(d)){0}` would result in an empty pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-[]->(b)){0,}((c)-[]->(d)){1,} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH ((a)-[]->(b)){1,}((c)-[]->(d)){0,} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH (x)((a)-[]->(b)){0, } RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH ((a)-[]->(b)){1,} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH ((a)-[]->(b)){0, 5}(y) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  // quantifier
  test("MATCH (x)((a)-[]->(b)){0} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "A quantifier for a path pattern must not be limited by 0."
    )
  }

  test("MATCH (x)((a)-[]->(b)){,0} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "A quantifier for a path pattern must not be limited by 0."
    )
  }

  test("MATCH (x)((a)-[]->(b)){2,1} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """A quantifier for a path pattern must not have a lower bound which exceeds its upper bound.
        |In this case, the lower bound 2 is greater than the upper bound 1.""".stripMargin
    )
  }

  test("MATCH (x) ((a)-[]->(b)){0, 1_000_000} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  // single node pattern
  test("MATCH ((n)){1, 5} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """A quantified path pattern needs to have at least one relationship.
        |In this case, the quantified path pattern ((n)){1, 5} consists of only one node.""".stripMargin
    )
  }

  test("MATCH ((n) (m)){1, 5} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      // this is the error message that we ultimately expect
      """A quantified path pattern needs to have at least one relationship.
        |In this case, the quantified path pattern ((n) (m)){1, 5} consists of only nodes.""".stripMargin,
      """Concatenation is currently only supported for quantified path patterns.
        |In this case, both (n) and (m) are single nodes.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH (x) (((a)-[b]->(c))*)+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Quantified path patterns are not allowed to be nested."
    )
  }

  test("MATCH ((a)-->(b)-[r]->*(c))+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Quantified path patterns are not allowed to be nested."
    )
  }

  test("MATCH ((a)-[*]->(b))+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Variable length relationships cannot be part of a quantified path pattern.",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // relationship quantification
  test("MATCH (a)-[*]->+(b) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Variable length relationships cannot be part of a quantified path pattern.",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  test("MATCH (a)-[r]->*(b) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  // variable overlap
  test("MATCH ((a)-->(b)-->(a)-->(c))+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH (c) ((a)-->(b))+ (d)-->(c) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH ((a)-->(b))+ ((b)-->(c))+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "The variable `b` occurs in multiple quantified path patterns and needs to be renamed.",
      "Variable `b` already declared"
    )
  }

  test("MATCH (()-[r]->())+ (()-[r]->())+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "The variable `r` occurs in multiple quantified path patterns and needs to be renamed.",
      "Variable `r` already declared",
      "Cannot use the same relationship variable 'r' for multiple relationships"
    )
  }

  test("MATCH ((a)-[b]->(c))* (d)-[e]->(a) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "The variable `a` occurs both inside and outside a quantified path pattern and needs to be renamed."
      // There is no "already declared" error, because the QPP variable `a` is defined first
    )
  }

  test("MATCH (a)-[e]->(d) ((a)-[b]->(c))*  RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "The variable `a` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      // There is an "already declared" error, because the non-QPP variable `a` is defined first, and group variables are not implicit variables like nodes.
      "Variable `a` already declared"
    )
  }

  test("MATCH (()-[r]->())* ()-[r]->() RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "The variable `r` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      "Cannot use the same relationship variable 'r' for multiple relationships"
    )
  }

  test("MATCH ()-[r]->() (()-[r]->())*  RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "The variable `r` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      "Variable `r` already declared",
      "Cannot use the same relationship variable 'r' for multiple relationships"
    )
  }

  test("MATCH ((a)-[b]->(c))* (d)-[e]->()((a)-[f]->(g)){2,} RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "The variable `a` occurs in multiple quantified path patterns and needs to be renamed.",
      "Variable `a` already declared"
    )
  }

  test("MATCH ((a)-[b]->(c))* (d)-[b]->+(f) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "The variable `b` occurs in multiple quantified path patterns and needs to be renamed.",
      "Variable `b` already declared",
      "Cannot use the same relationship variable 'b' for multiple relationships"
    )
  }

  test("MATCH (a)-->(b) MATCH (x)--(y) ((a)-->(t)){1,5} ()-->(z) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "The variable `a` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      "Variable `a` already declared"
    )
  }

  test("MATCH ((a)-->(b))+ MATCH (x)--(y) ((a)-->(t)){1,5} ()-->(z) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "The variable `a` occurs in multiple quantified path patterns and needs to be renamed.",
      "Variable `a` already declared"
    )
  }

  // parenthesized path patterns
  test("MATCH ((a)-->(b)) (x) RETURN x") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """Concatenation is currently only supported for quantified path patterns.
        |In this case, ((a)-->(b)) is a (non-quantified) parenthesized path pattern and (x) is a single node.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH (x) ((a)-->(b)) RETURN x") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """Concatenation is currently only supported for quantified path patterns.
        |In this case, (x) is a single node and ((a)-->(b)) is a (non-quantified) parenthesized path pattern.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-->(b)) (x)-->(y) RETURN x") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """Concatenation is currently only supported for quantified path patterns.
        |In this case, ((a)-->(b)) is a (non-quantified) parenthesized path pattern and (x)-->(y) is a simple path pattern.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-->(b)) ((x)-->(y)) RETURN x") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """Concatenation is currently only supported for quantified path patterns.
        |In this case, both ((a)-->(b)) and ((x)-->(y)) are (non-quantified) parenthesized path patterns.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH (x) (y) RETURN x") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """Concatenation is currently only supported for quantified path patterns.
        |In this case, both (x) and (y) are single nodes.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-->(b)) ((x)-->(y))* RETURN x") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH (p = (a)-->(b)) ((x)-->(y))* RETURN x") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldBe Seq(
      "Sub-path assignment is currently not supported outside quantified path patterns."
    )
  }

  // Predicates

  test("MATCH ((a)-->(b) WHERE b.prop > 7)+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH ((a)-->(b) WHERE a.prop < b.prop)+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH (x) MATCH ((a)-->(b) WHERE a.prop < x.prop)+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH (x) MATCH ((a WHERE a.prop < x.prop)-->(b))+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH (x) MATCH ((a)-[r:REL WHERE r.prop < x.prop]->(b))+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  // accessing non-local variables outside of the quantification
  test("MATCH (x)-->(y)((a)-[e]->(b) WHERE a.h > x.h)* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, x is defined in the same `MATCH` clause as ((a)-[e]->(b) WHERE a.h > x.h)*.""".stripMargin
    )
  }

  test("MATCH (x)-->(y)((a)-[e]->(b) WHERE a.h > u.h)* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, u is defined in the same `MATCH` clause as ((a)-[e]->(b) WHERE a.h > u.h)*.""".stripMargin
    )
  }

  test("MATCH (x)-->(y)((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, x is defined in the same `MATCH` clause as ((a)-[e]->(b {h: x.h}))*.""".stripMargin
    )
  }

  test("MATCH (x)-->(y), ((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, x is defined in the same `MATCH` clause as ((a)-[e]->(b {h: x.h}))*.""".stripMargin
    )
  }

  test("MATCH (x) ((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, x is defined in the same `MATCH` clause as ((a)-[e]->(b {h: x.h}))*.""".stripMargin
    )
  }

  test("MATCH (x) ((a)-[e {h: x.h}]->(b))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, x is defined in the same `MATCH` clause as ((a)-[e {h: x.h}]->(b))*.""".stripMargin
    )
  }

  test("MATCH (x)-->(y) ((a)-[e]->(b {h: u.h}))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, u is defined in the same `MATCH` clause as ((a)-[e]->(b {h: u.h}))*.""".stripMargin
    )
  }

  test("MATCH (x)-->(y) MATCH (y) ((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  test("MATCH (s)-->(u) MATCH (x)-->(y)((a)-[e]->(b {h: u.h}))* (s) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  // access group variables without aggregation
  test("MATCH (x)-->(y)((a)-[e]->(b))+(s)-->(u) WHERE e.weight < 4 RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      """Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was List<Relationship>
        |A group variable cannot be used in a non-aggregating operation.""".stripMargin
    )
  }

  // path assignment with quantified path patterns
  test("MATCH p = ((a)-[]->(b))+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Assigning a path with a quantified path pattern is not yet supported."
    )
  }

  test("MATCH p = (x)-->(y) ((a)-[]->(b))+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Assigning a path with a quantified path pattern is not yet supported."
    )
  }

  // Mixing with legacy var-length

  // Different clauses
  test("MATCH (x)-[*]->(y) MATCH ((a)-[]->(b))+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  // Mixed quantifier in same pattern element
  test("MATCH (x)-[*]->(y) ((a)-[]->(b))+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // Two legacy var-length
  test("MATCH (x)-[*]->(y) ((a)-[]->(b))+ (n)-[*]->(m) RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed.",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // Mixed quantifier in same clause
  test("MATCH (x)-[*]->(y), ((a)-[]->(b))+ RETURN count(*)") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // Mixed quantifier (quantified relationship) in same clause
  test("MATCH (n) RETURN [(n)-->+(m) | m], [(n)-[*3]-(m) | m]") {
    // quantified relationships are not implemented yet. Once this is the case, please change to the test below
    the[SyntaxException].thrownBy(
      runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns)
    ).getMessage should include("Invalid input '+': expected \"(\"")
    // runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
    //   "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed. This relationship can be expressed as '--{3}'"
    // )
  }

  // ... on same element pattern
  test("MATCH ()-[r:A*]->*() RETURN r") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Variable length relationships cannot be part of a quantified path pattern.",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  test("MATCH ()-[r:A*1..2]->{1,2}() RETURN r") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldEqual Seq(
      "Variable length relationships cannot be part of a quantified path pattern.",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // ... in different statements
  test("MATCH (s)-[:A*2..2]->(n) MATCH (n)-[:B]->{2}(t) RETURN s.p AS sp, t.p AS tp") {
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errorMessages shouldBe empty
  }

  // pattern comprehension
  ignore("MATCH (n) WITH [ p = (n)--(m) ((a)-->(b))+  | p ] as paths RETURN count(*)") {
    // this currently fails with a parse error
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  // this query may not be super useful but at least it works w/o juxtaposition
  ignore("MATCH (n) WITH [ p = ((a)-->(b))+  | p ] as paths RETURN count(*)") {
    // this currently fails with a parse error
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  // pattern expression
  ignore("MATCH (n) WHERE (n)--() (()-->())+ RETURN count(*)") {
    // this currently fails with a parse error
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }

  // this query may not be super useful but at least it works w/o juxtaposition
  ignore("MATCH (n) WHERE (()-->())+ RETURN count(*)") {
    // this currently fails with a parse error
    runSemanticAnalysisWithSemanticFeatures(SemanticFeature.QuantifiedPathPatterns).errors shouldBe empty
  }
}
