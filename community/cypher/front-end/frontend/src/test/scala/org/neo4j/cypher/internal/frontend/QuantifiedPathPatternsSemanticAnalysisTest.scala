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

import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.label_expressions.UpdateStatement
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory.SyntaxException
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

abstract class QuantifiedPathPatternsInDifferentClausesSemanticAnalysisTest(statement: UpdateStatement)
    extends CypherFunSuite
    with SemanticAnalysisTestSuiteWithDefaultQuery
    with TestName {

  override def defaultQuery: String = s"$statement $testName"

  test("((a)-[:Rel]->(b)){2}") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      s"Quantified path patterns cannot be used in a $statement clause, but only in a MATCH clause."
    )
  }
}

class QuantifiedPathPatternsInCreateClausesSemanticAnalysisTest
    extends QuantifiedPathPatternsInDifferentClausesSemanticAnalysisTest(UpdateStatement.CREATE)

class QuantifiedPathPatternsInMergeClausesSemanticAnalysisTest
    extends QuantifiedPathPatternsInDifferentClausesSemanticAnalysisTest(UpdateStatement.MERGE)

class QuantifiedPathPatternsSemanticAnalysisTest extends NameBasedSemanticAnalysisTestSuite {

  test("MATCH ((a)-[]->(b))+ RETURN a") {
    val result = runSemanticAnalysis()
    result.errors shouldBe empty
    val innerA = Variable("a")(InputPosition(8, 1, 9))
    result.semanticTable.types(innerA).specified shouldBe CTNode.invariant
    val outerA = Variable("a")(InputPosition(28, 1, 29))
    result.semanticTable.types(outerA).specified shouldBe CTList(CTNode).invariant
  }

  test("MATCH (p = (a)-[]->(b))+ RETURN p") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Assigning a path in a quantified path pattern is not yet supported."
    )
  }

  test("MATCH (a) (()--(x {prop: a.prop}))+ (b) (()--())+ (c) RETURN *") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.\n" +
        "In this case, a is defined in the same `MATCH` clause as (()--(x {prop: a.prop}))+."
    )
  }

  test("MERGE (var0 WHERE COUNT { ((var1)--())+ } > 1 ) RETURN *") {
    // This test asserts that we give semantic errors instead of throwing "java.util.NoSuchElementException: key not found"
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Node pattern predicates are not allowed in a MERGE clause, but only in a MATCH clause or inside a pattern comprehension",
      "Subquery expressions are not allowed in a MERGE clause."
    )
  }

  test("MATCH (p = (a)--(b))+ (p = (c)--(d))+ RETURN p") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "The variable `p` occurs in multiple quantified path patterns and needs to be renamed.",
      "Assigning a path in a quantified path pattern is not yet supported.",
      "Assigning a path in a quantified path pattern is not yet supported.",
      "Variable `p` already declared"
    )
  }

  test("MATCH (p = (a)--(b))+ (p = (c)--(d)) RETURN p") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Assigning a path in a quantified path pattern is not yet supported.",
      "Type mismatch: p defined with conflicting type List<T> (expected Path)",
      "Sub-path assignment is currently not supported."
    )
  }

  test("MATCH (p = (a)--(b))+ MATCH (p = (c)--(d))+ RETURN p") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Assigning a path in a quantified path pattern is not yet supported.",
      "The variable `p` is already defined in a previous clause, it cannot be referenced as a node or as a relationship variable inside of a quantified path pattern.",
      "Assigning a path in a quantified path pattern is not yet supported.",
      "Variable `p` already declared"
    )
  }

  test("MATCH p = (p = (a)--(b))+ (c)--(d) RETURN p") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Assigning a path in a quantified path pattern is not yet supported.",
      "Variable `p` already declared"
    )
  }

  // nested shortest path
  test("MATCH (p = shortestPath((a)-[]->(b)))+ RETURN p") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Assigning a path in a quantified path pattern is not yet supported.",
      "shortestPath(...) is only allowed as a top-level element and not inside a quantified path pattern",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  test("MATCH shortestPath( ((a)-[]->(b))+ ) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "shortestPath(...) contains quantified pattern. This is currently not supported.",
      "shortestPath(...) requires a pattern containing a single relationship"
    )
  }

  test("MATCH (shortestPath((a)-[]->(b))) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      // this is the error message that we ultimately expect
      "shortestPath(...) is only allowed as a top-level element and not inside a parenthesized path pattern"
    )
  }

  test("MATCH shortestPath((n)-[]->+({s: 1})) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldBe Seq(
      "shortestPath(...) contains quantified pattern. This is currently not supported.",
      "shortestPath(...) requires a pattern containing a single relationship"
    )
  }

  // minimum node count
  test("MATCH ((a)-[]->(b)){0,} RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """A top-level path pattern in a `MATCH` clause must be written such that it always evaluates to at least one node pattern.
        |In this case, `((a)-->(b)){0}` would result in an empty pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-[]->(b))* RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """A top-level path pattern in a `MATCH` clause must be written such that it always evaluates to at least one node pattern.
        |In this case, `((a)-->(b)){0}` would result in an empty pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-[]->(b)){0,}((c)-[]->(d)){0,} RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """A top-level path pattern in a `MATCH` clause must be written such that it always evaluates to at least one node pattern.
        |In this case, `((a)-->(b)){0} ((c)-->(d)){0}` would result in an empty pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-[]->(b)){0,}((c)-[]->(d)){1,} RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ((a)-[]->(b)){1,}((c)-[]->(d)){0,} RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (x)((a)-[]->(b)){0, } RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ((a)-[]->(b)){1,} RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ((a)-[]->(b)){0, 5}(y) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  // quantifier
  test("MATCH (x)((a)-[]->(b)){0} RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "A quantifier for a path pattern must not be limited by 0."
    )
  }

  test("MATCH (x)((a)-[]->(b)){,0} RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "A quantifier for a path pattern must not be limited by 0."
    )
  }

  test("MATCH (x)((a)-[]->(b)){2,1} RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """A quantifier for a path pattern must not have a lower bound which exceeds its upper bound.
        |In this case, the lower bound 2 is greater than the upper bound 1.""".stripMargin
    )
  }

  test("MATCH (a)-[]->{9223372036854775808}(b) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "integer is too large"
    )
  }

  test("MATCH (a)-[]->{1, 9223372036854775808}(b) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "integer is too large"
    )
  }

  test("MATCH (a)-[]->{9223372036854775808,}(b) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "integer is too large"
    )
  }

  test("MATCH (x) ((a)-[]->(b)){0, 1_000_000} RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  // single node pattern
  test("MATCH ((n)){1, 5} RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """A quantified path pattern needs to have at least one relationship.
        |In this case, the quantified path pattern ((n)){1, 5} consists of only one node.""".stripMargin
    )
  }

  test("MATCH ((n) (m)){1, 5} RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      // this is the error message that we ultimately expect
      """A quantified path pattern needs to have at least one relationship.
        |In this case, the quantified path pattern ((n) (m)){1, 5} consists of only nodes.""".stripMargin,
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, both (n) and (m) are single nodes.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH (x) (((a)-[b]->(c))*)+ RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Quantified path patterns are not allowed to be nested."
    )
  }

  test("MATCH ((a)-->(b)-[r]->*(c))+ RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Quantified path patterns are not allowed to be nested."
    )
  }

  test("MATCH ((a)-[*]->(b))+ RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Variable length relationships cannot be part of a quantified path pattern.",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // relationship quantification
  test("MATCH (a)-[*]->+(b) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Variable length relationships cannot be part of a quantified path pattern.",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  test("MATCH (a)-[r]->*(b) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  // variable overlap
  test("MATCH ((a)-->(b)-->(a)-->(c))+ RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (c) ((a)-->(b))+ (d)-->(c) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ((a)-->(b))+ ((b)-->(c))+ RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "The variable `b` occurs in multiple quantified path patterns and needs to be renamed.",
      "Type mismatch: b defined with conflicting type List<Node> (expected Node)",
      "Variable `b` already declared"
    )
  }

  test("MATCH (()-[r]->())+ (()-[r]->())+ RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "The variable `r` occurs in multiple quantified path patterns and needs to be renamed.",
      "Type mismatch: r defined with conflicting type List<Relationship> (expected Relationship)",
      "Variable `r` already declared"
    )
  }

  test("MATCH ((a)-[b]->(c))* (d)-[e]->(a) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "The variable `a` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      "Type mismatch: a defined with conflicting type List<Node> (expected Node)"
    )
  }

  test("MATCH (a)-[e]->(d) ((a)-[b]->(c))*  RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "The variable `a` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      "Variable `a` already declared"
    )
  }

  test("MATCH (()-[r]->())* ()-[r]->() RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "The variable `r` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      "Type mismatch: r defined with conflicting type List<Relationship> (expected Relationship)"
    )
  }

  test("MATCH ()-[r]->() (()-[r]->())*  RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "The variable `r` occurs both inside and outside a quantified path pattern and needs to be renamed.",
      "Variable `r` already declared"
    )
  }

  test("MATCH ((a)-[b]->(c))* (d)-[e]->()((a)-[f]->(g)){2,} RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "The variable `a` occurs in multiple quantified path patterns and needs to be renamed.",
      "Type mismatch: a defined with conflicting type List<Node> (expected Node)",
      "Variable `a` already declared"
    )
  }

  test("MATCH ((a)-[b]->(c))* (d)-[b]->+(f) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "The variable `b` occurs in multiple quantified path patterns and needs to be renamed.",
      "Type mismatch: b defined with conflicting type List<Relationship> (expected Relationship)",
      "Variable `b` already declared"
    )
  }

  test("MATCH (a)-->(b) MATCH (x)--(y) ((a)-->(t)){1,5} ()-->(z) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "The variable `a` is already defined in a previous clause, it cannot be referenced as a node or as a relationship variable inside of a quantified path pattern.",
      "Variable `a` already declared"
    )
  }

  test("MATCH ((a)-->(b))+ MATCH (x)--(y) ((a)-->(t)){1,5} ()-->(z) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "The variable `a` is already defined in a previous clause, it cannot be referenced as a node or as a relationship variable inside of a quantified path pattern.",
      "Type mismatch: a defined with conflicting type List<Node> (expected Node)",
      "Variable `a` already declared"
    )
  }

  // parenthesized path patterns
  test("MATCH ((a)-->(b)) (x) RETURN x") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, ((a)-->(b)) is a (non-quantified) parenthesized path pattern and (x) is a single node.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH (x) ((a)-->(b)) RETURN x") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, (x) is a single node and ((a)-->(b)) is a (non-quantified) parenthesized path pattern.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-->(b)) (x)-->(y) RETURN x") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, ((a)-->(b)) is a (non-quantified) parenthesized path pattern and (x)-->(y) is a simple path pattern.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-->(b)) ((x)-->(y)) RETURN x") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, both ((a)-->(b)) and ((x)-->(y)) are (non-quantified) parenthesized path patterns.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH (x) (y) RETURN x") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """Juxtaposition is currently only supported for quantified path patterns.
        |In this case, both (x) and (y) are single nodes.
        |That is, neither of these is a quantified path pattern.""".stripMargin
    )
  }

  test("MATCH ((a)-->(b)) ((x)-->(y))* RETURN x") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (p = (a)-->(b)) ((x)-->(y))* RETURN x") {
    runSemanticAnalysis().errorMessages shouldBe Seq(
      "Sub-path assignment is currently not supported."
    )
  }

  // Predicates

  test("MATCH ((a)-->(b) WHERE b.prop > 7)+ RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ((a)-->(b) WHERE a.prop < b.prop)+ RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ((a)-[:R]->(b) WHERE (a)-[:S]->(b))+ RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ((a)-[:R]->(b) WHERE EXISTS { MATCH (a)-[:S]->(b) })+ RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH ((a)-[:R]->(b) WHERE COUNT { MATCH (a)-[:S]->(b) } > 1)+ RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (x) MATCH ((a)-->(b) WHERE a.prop < x.prop)+ RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (x) MATCH ((a WHERE a.prop < x.prop)-->(b))+ RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (x) MATCH ((a)-[r:REL WHERE r.prop < x.prop]->(b))+ RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  // accessing non-local variables outside of the quantification
  test("MATCH (x)-->(y)((a)-[e]->(b) WHERE a.h > x.h)* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, x is defined in the same `MATCH` clause as ((a)-[e]->(b) WHERE a.h > x.h)*.""".stripMargin
    )
  }

  test("MATCH (x)-->(y)((a)-[e]->(b) WHERE a.h > u.h)* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, u is defined in the same `MATCH` clause as ((a)-[e]->(b) WHERE a.h > u.h)*.""".stripMargin
    )
  }

  test("MATCH (x)-->(y)((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, x is defined in the same `MATCH` clause as ((a)-[e]->(b {h: x.h}))*.""".stripMargin
    )
  }

  test("MATCH (x)-->(y), ((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, x is defined in the same `MATCH` clause as ((a)-[e]->(b {h: x.h}))*.""".stripMargin
    )
  }

  test("MATCH (x) ((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, x is defined in the same `MATCH` clause as ((a)-[e]->(b {h: x.h}))*.""".stripMargin
    )
  }

  test("MATCH (x) ((a)-[e {h: x.h}]->(b))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, x is defined in the same `MATCH` clause as ((a)-[e {h: x.h}]->(b))*.""".stripMargin
    )
  }

  test("MATCH (x)-->(y) ((a)-[e]->(b {h: u.h}))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, u is defined in the same `MATCH` clause as ((a)-[e]->(b {h: u.h}))*.""".stripMargin
    )
  }

  test("MATCH p=(x)-->(y), ((a)-[e]->(b {h: nodes(p)[0].prop}))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      """From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
        |In this case, p is defined in the same `MATCH` clause as ((a)-[e]->(b {h: (nodes(p)[0]).prop}))*.""".stripMargin
    )
  }

  test("MATCH (x)-->(y) MATCH (y) ((a)-[e]->(b {h: x.h}))* (s)-->(u) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (s)-->(u) MATCH (x)-->(y)((a)-[e]->(b {h: u.h}))* (s) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (a), (b) MATCH (a) ((n)-[]->(m) WHERE n.prop > a.prop AND n.prop > b.prop)+ (b) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (a), (b) MATCH (a2) ((n)-[]->(m) WHERE ALL(a IN n.prop WHERE a > 2) )+ (b2) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  test("MATCH (a) ((n)-[]->(m) WHERE ALL(a IN n.prop WHERE a > 2) )+ (b) RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  // access group variables without aggregation
  test("MATCH (x)-->(y)((a)-[e]->(b))+(s)-->(u) WHERE e.weight < 4 RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Type mismatch: expected Map, Node, Relationship, Point, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was List<Relationship>"
    )
  }

  // path assignment with quantified path patterns
  test("MATCH p = ((a)-[]->(b))+ RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test("MATCH p = (x)-->(y) ((a)-[]->(b))+ RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  // Mixing with legacy var-length

  // Different clauses
  test("MATCH (x)-[*]->(y) MATCH ((a)-[]->(b))+ RETURN count(*)") {
    runSemanticAnalysis().errors shouldBe empty
  }

  // Mixed quantifier in same pattern element
  test("MATCH (x)-[*]->(y) ((a)-[]->(b))+ RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // Two legacy var-length
  test("MATCH (x)-[*]->(y) ((a)-[]->(b))+ (n)-[*]->(m) RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed.",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // Mixed quantifier in same clause
  test("MATCH (x)-[*]->(y), ((a)-[]->(b))+ RETURN count(*)") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // Mixed quantifier (quantified relationship) in same clause
  test("MATCH (n) RETURN [(n)-->+(m) | m], [(n)-[*3]-(m) | m]") {
    // quantified relationships are not implemented yet. Once this is the case, please change to the test below
    the[SyntaxException].thrownBy(
      runSemanticAnalysis()
    ).getMessage should include("Invalid input '+': expected \"(\"")
    // runSemanticAnalysis().errorMessages shouldEqual Seq(
    //   "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed. This relationship can be expressed as '--{3}'"
    // )
  }

  // ... on same element pattern
  test("MATCH ()-[r:A*]->*() RETURN r") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Variable length relationships cannot be part of a quantified path pattern.",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  test("MATCH ()-[r:A*1..2]->{1,2}() RETURN r") {
    runSemanticAnalysis().errorMessages shouldEqual Seq(
      "Variable length relationships cannot be part of a quantified path pattern.",
      "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed."
    )
  }

  // ... in different statements
  test("MATCH (s)-[:A*2..2]->(n) MATCH (n)-[:B]->{2}(t) RETURN s.p AS sp, t.p AS tp") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  // should not throw error about mixing if they are in different scopes
  test("MATCH ((a)--(b) WHERE EXISTS { (c)-[r*]-(d) })+ RETURN 1") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test("MATCH ((a)--(b WHERE EXISTS { (c)-[r*]-(d) }))+ RETURN 1") {
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  test("MATCH (a)-[r*]-(b WHERE EXISTS { (a)(()-[r1]->())*(b) }) RETURN 1") {
    runSemanticAnalysis().errorMessages shouldBe empty
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
    runSemanticAnalysis().errorMessages shouldBe empty
  }

  // pattern comprehension
  ignore("MATCH (n) WITH [ p = (n)--(m) ((a)-->(b))+  | p ] as paths RETURN count(*)") {
    // this currently fails with a parse error
    runSemanticAnalysis().errors shouldBe empty
  }

  // this query may not be super useful but at least it works w/o juxtaposition
  ignore("MATCH (n) WITH [ p = ((a)-->(b))+  | p ] as paths RETURN count(*)") {
    // this currently fails with a parse error
    runSemanticAnalysis().errors shouldBe empty
  }

  // pattern expression
  ignore("MATCH (n) WHERE (n)--() (()-->())+ RETURN count(*)") {
    // this currently fails with a parse error
    runSemanticAnalysis().errors shouldBe empty
  }

  // this query may not be super useful but at least it works w/o juxtaposition
  ignore("MATCH (n) WHERE (()-->())+ RETURN count(*)") {
    // this currently fails with a parse error
    runSemanticAnalysis().errors shouldBe empty
  }
}
