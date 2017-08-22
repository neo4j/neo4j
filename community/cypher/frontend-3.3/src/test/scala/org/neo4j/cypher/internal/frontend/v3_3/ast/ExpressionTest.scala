/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.DummyPosition
import org.neo4j.cypher.internal.frontend.v3_3.IdentityMap
import org.neo4j.cypher.internal.frontend.v3_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_3.SemanticState

class ExpressionTest extends CypherFunSuite with AstConstructionTestSupport {

  val expression = DummyExpression(CTAny, DummyPosition(0))

  test("shouldReturnCalculatedType") {
    expression.types(SemanticState.clean) should equal(TypeSpec.all)
  }

  test("shouldReturnTypeSetOfAllIfTypesRequestedButNotEvaluated") {
    expression.types(SemanticState.clean) should equal(TypeSpec.all)
  }

  test("shouldReturnSpecifiedAndConstrainedTypes") {
    val state = (
      expression.specifyType(CTNode | CTInteger) chain
        expression.expectType(CTNumber.covariant)
    )(SemanticState.clean).state

    expression.types(state) should equal(CTInteger.invariant)
  }

  test("shouldRaiseTypeErrorWhenMismatchBetweenSpecifiedTypeAndExpectedType") {
    val result = (
      expression.specifyType(CTNode | CTInteger) chain
        expression.expectType(CTString.covariant)
    )(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.position should equal(expression.position)
    expression.types(result.state) shouldBe empty
    result.errors.head.msg should equal("Type mismatch: expected String but was Integer or Node")
  }

  test("shouldRaiseTypeErrorWithCustomMessageWhenMismatchBetweenSpecifiedTypeAndExpectedType") {
    val result = (
      expression.specifyType(CTNode | CTInteger) chain
        expression.expectType(CTString.covariant,
                              (expected: String, existing: String) => s"lhs was $expected yet rhs was $existing")
    )(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.position should equal(expression.position)
    expression.types(result.state) shouldBe empty

    assert(result.errors.size === 1)
    assert(result.errors.head.position === expression.position)
    assert(result.errors.head.msg == "Type mismatch: lhs was String yet rhs was Integer or Node")
    assert(expression.types(result.state).isEmpty)
  }

  test("should compute dependencies of simple expressions") {
    varFor("a").dependencies should equal(Set(varFor("a")))
    SignedDecimalIntegerLiteral("1")(pos).dependencies should equal(Set())
  }

  test("should compute dependencies of composite expressions") {
    Add(varFor("a"), Subtract(SignedDecimalIntegerLiteral("1")(pos), varFor("b")) _)(pos).dependencies should equal(
      Set(varFor("a"), varFor("b")))
  }

  test("should compute dependencies for filtering expressions") {
    // extract(x IN (n)-->(k) | head(nodes(x)) )
    val pat: RelationshipsPattern = RelationshipsPattern(
      RelationshipChain(
        NodePattern(Some(varFor("n")), Seq.empty, None) _,
        RelationshipPattern(None, Seq.empty, None, None, SemanticDirection.OUTGOING) _,
        NodePattern(Some(varFor("k")), Seq.empty, None) _
      ) _
    ) _
    val expr: Expression = ExtractExpression(
      varFor("x"),
      PatternExpression(pat),
      None,
      Some(FunctionInvocation(FunctionName("head") _, FunctionInvocation(FunctionName("nodes") _, varFor("x")) _) _)
    ) _

    expr.dependencies should equal(Set(varFor("n"), varFor("k")))
  }

  test("should compute dependencies for nested filtering expressions") {
    // extract(x IN (n)-->(k) | extract(y IN [1,2,3] | y) )
    val pat: RelationshipsPattern = RelationshipsPattern(
      RelationshipChain(
        NodePattern(Some(varFor("n")), Seq.empty, None) _,
        RelationshipPattern(None, Seq.empty, None, None, SemanticDirection.OUTGOING) _,
        NodePattern(Some(varFor("k")), Seq.empty, None) _
      ) _
    ) _
    val innerExpr: Expression = ExtractExpression(
      varFor("y"),
      ListLiteral(Seq(literalInt(1), literalInt(2), literalInt(3))) _,
      None,
      Some(varFor("y"))
    ) _
    val expr: Expression = ExtractExpression(
      varFor("x"),
      PatternExpression(pat),
      None,
      Some(innerExpr)
    ) _

    expr.dependencies should equal(Set(varFor("n"), varFor("k")))
  }

  test("should compute dependencies for pattern comprehensions") {
    // [ (n)-->(k) | k ]
    val pat: RelationshipsPattern = RelationshipsPattern(
      RelationshipChain(
        NodePattern(Some(varFor("n")), Seq.empty, None) _,
        RelationshipPattern(None, Seq.empty, None, None, SemanticDirection.OUTGOING) _,
        NodePattern(Some(varFor("k")), Seq.empty, None) _
      ) _
    ) _
    val expr = PatternComprehension(
      namedPath = None,
      pattern = pat,
      predicate = None,
      projection = varFor("k")
    )(pos)

    expr.withOuterScope(Set(varFor("n"), varFor("k"))).dependencies should equal(Set(varFor("n"), varFor("k")))
    expr.withOuterScope(Set.empty).dependencies should equal(Set.empty)
  }

  test("should compute inputs of composite expressions") {
    val identA = varFor("a")
    val identB = varFor("b")
    val lit1   = SignedDecimalIntegerLiteral("1")(pos)
    val sub    = Subtract(lit1, identB)(pos)
    val add    = Add(identA, sub)(pos)

    IdentityMap(add.inputs: _*) should equal(
      IdentityMap(
        identA -> Set.empty,
        identB -> Set.empty,
        lit1   -> Set.empty,
        sub    -> Set.empty,
        add    -> Set.empty
      ))
  }

  test("should compute inputs for filtering expressions") {
    // given
    val pat = PatternExpression(
      RelationshipsPattern(
        RelationshipChain(
          NodePattern(Some(varFor("n")), Seq.empty, None) _,
          RelationshipPattern(None, Seq.empty, None, None, SemanticDirection.OUTGOING) _,
          NodePattern(Some(varFor("k")), Seq.empty, None) _
        ) _
      ) _)

    val callNodes: Expression = FunctionInvocation(FunctionName("nodes") _, varFor("x")) _
    val callHead: Expression  = FunctionInvocation(FunctionName("head") _, callNodes) _

    // extract(x IN (n)-->(k) | head(nodes(x)) )
    val expr: Expression = ExtractExpression(
      varFor("x"),
      pat,
      None,
      Some(callHead)
    ) _

    // when
    val inputs = IdentityMap(expr.inputs: _*)

    // then
    inputs(callNodes) should equal(Set(varFor("x")))
    inputs(callHead) should equal(Set(varFor("x")))
    inputs(expr) should equal(Set.empty)
    inputs(pat) should equal(Set.empty)
  }
}
