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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.neo4j.cypher.internal.ast.IsNormalized
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.NFCNormalForm
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.rewriting.conditions.noReferenceEqualityAmongVariables
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.Extractors.SetExtractor

class SimplifyPredicatesTest extends CypherFunSuite {

  test("double negation is removed by keeping an extra not") {
    // not(not(not(P))) <=> not(P)
    assertRewrittenMatches("NOT NOT NOT 'P'", { case Not(StringLiteral("P")) => () })
  }

  test("repeated double negation is removed") {
    // not(not(not(not(P)))) <=> bool(P)
    assertRewrittenMatches("NOT NOT NOT NOT 'P'", { case CoerceToPredicate(StringLiteral("P")) => () })
  }

  test("double negation is removed") {
    // not(not(P)) <=> bool(P)
    assertRewrittenMatches("NOT NOT 'P'", { case CoerceToPredicate(StringLiteral("P")) => () })

    // not(not(TRUE)) <=> TRUE
    assertRewrittenMatches("NOT NOT TRUE", { case True() => () })
  }

  test("double negation on pattern comprehension") {
    // NOT NOT ()--() -> bool(()--())
    assertRewrittenMatches("NOT NOT ()--()", { case CoerceToPredicate(PatternExpression(_)) => () })
  }

  test("double negation on null") {
    // NOT NOT null -> null
    assertRewrittenMatches("NOT NOT null", { case Null() => () })
  }

  test("OR + double negation") {
    // or(not(not(P)), not(not(Q))) <=> or(P, Q)
    assertRewrittenMatches(
      "NOT NOT 'P' OR NOT NOT 'Q'",
      { case Ors(SetExtractor(StringLiteral("P"), StringLiteral("Q"))) => () }
    )
  }

  test("NOT IS NULL is rewritten") {
    // not(isNull(P)) <=> isNotNull(P)
    assertRewrittenMatches("NOT( 'P' IS NULL )", { case IsNotNull(StringLiteral("P")) => () })
  }

  test("NOT IS NOT NULL is rewritten") {
    // not(isNotNull(P)) <=> isNull(P)
    assertRewrittenMatches("NOT( 'P' IS NOT NULL )", { case IsNull(StringLiteral("P")) => () })
  }

  test("IS NOT :: is rewritten") {
    // isNotTyped(P) <=> not(isTyped(P, INTEGER))
    assertRewrittenMatches(
      "'P' IS NOT :: INTEGER",
      { case Not(IsTyped(StringLiteral("P"), IntegerType(true))) => () }
    )
  }

  test("NOT IS NOT :: is rewritten") {
    // not(isNotTyped(P), STRING) <=> isTyped(P, STRING)
    assertRewrittenMatches(
      "NOT( 'P' IS NOT :: STRING )",
      { case IsTyped(StringLiteral("P"), StringType(true)) => () }
    )
  }

  test("NOT IS :: is not rewritten") {
    assertRewrittenMatches(
      "NOT( 'P' IS :: BOOL )",
      { case Not(IsTyped(StringLiteral("P"), BooleanType(true))) => () }
    )
  }

  test("IS NOT NORMALIZED is rewritten") {
    // IsNotNormalized(P) <=> not(IsNormalized(P, INTEGER))
    assertRewrittenMatches(
      "'P' IS NOT NORMALIZED",
      { case Not(IsNormalized(StringLiteral("P"), NFCNormalForm)) => () }
    )
  }

  test("NOT IS NOT NORMALIZED is rewritten") {
    // not(IsNotNormalized(P), STRING) <=> IsNormalized(P, STRING)
    assertRewrittenMatches(
      "NOT( 'P' IS NOT NORMALIZED )",
      { case IsNormalized(StringLiteral("P"), NFCNormalForm) => () }
    )
  }

  test("NOT IS NORMALIZED is not rewritten") {
    assertRewrittenMatches(
      "NOT( 'P' IS NORMALIZED )",
      { case Not(IsNormalized(StringLiteral("P"), NFCNormalForm)) => () }
    )
  }

  test("Simplify OR of identical expressions with interspersed condition") {
    // We should be able to remove one of those redundant $n = 2.
    assertRewrittenMatches("$n = 2 OR $n = 1 OR $n = 2", { case Ors(SetExtractor(Equals(_, _), Equals(_, _))) => () })
  }

  test("Simplify negated false") {
    assertRewrittenMatches("$n.a OR NOT false", { case True() => () })
  }

  test("Simplify negated true") {
    assertRewrittenMatches("$n.a AND NOT true", { case False() => () })
  }

  test("Simplify AND that contains only True") {
    assertRewrittenMatches("true AND true", { case True() => () })
  }

  test("Simplify OR that contains only False") {
    assertRewrittenMatches("false OR false", { case False() => () })
  }

  test("double negation around AND that contains only True") {
    assertRewrittenMatches("NOT NOT (true AND true)", { case True() => () })
  }

  test("double negation around AND that contains only False") {
    assertRewrittenMatches("NOT NOT (false AND false)", { case False() => () })
  }

  test("Do not simplify expressions with different auto extracted parameters") {
    val position = InputPosition(0, 0, 0)
    // AST for $n = 2 OR $n = 3
    val ast = Ors(Seq(
      Equals(
        ExplicitParameter("n", CTAny)(position),
        AutoExtractedParameter("AUTOINT0", CTInteger)(position)
      )(position),
      Equals(
        ExplicitParameter("n", CTAny)(position),
        AutoExtractedParameter("AUTOINT1", CTInteger)(position)
      )(position)
    ))(position)
    val rewriter = flattenBooleanOperators andThen simplifyPredicates(SemanticState.clean)
    val result = ast.rewrite(rewriter)
    ast should equal(result)
  }

  test("should not simplify self-negation") {
    // because in ternary logic NULL AND not NULL = NULL, we cannot simplify this to false, as one might be tempted to do
    assertRewrittenMatches("$n.a AND NOT $n.a", { case Ands(_) => () })
  }

  test("should not simplify self-equality of types that are not equal to themselves") {
    // because in ternary logic NULL = NULL => NULL, we cannot simplify this to true, as one might be tempted to do
    assertRewrittenMatches("{n: null} = {n: null}", { case Equals(_, _) => () })
    assertRewrittenMatches("null = null", { case Equals(_, _) => () })
    assertRewrittenMatches("NaN = NaN", { case Equals(_, _) => () })
    assertRewrittenMatches("$param = $param", { case Equals(_, _) => () })
  }

  test("Simplify AND of identical value") {
    // and(eq($n, 2), eq($n, 2)) => eq($n, 2)
    assertRewrittenMatches("$n = 2 AND $n = 2", { case Equals(_, _) => () })
  }

  test("Simplify OR of identical value") {
    // or(eq($n, 2), eq($n, 2)) => eq($n, 2)
    assertRewrittenMatches("$n = 2 OR $n = 2", { case Equals(_, _) => () })
  }

  test("Do not simplify OR of different value") {
    // or(eq($n, 2), eq($n, 3)) => or(eq($n, 2), eq($n, 3))
    assertRewrittenMatches("$n = 2 OR $n = 3", { case Ors(SetExtractor(Equals(_, _), Equals(_, _))) => () })
  }

  ignore("Simplify AND of identical value spread apart") {
    assertRewrittenMatches(
      "$n = 2 AND $m = 3 AND $n = 2",
      { case Ands(SetExtractor(Equals(_, _), Equals(_, _))) => () }
    )
  }

  ignore("Simplify OR of identical value spread apart") {
    assertRewrittenMatches("$n = 2 OR $m = 3 OR $n = 2", { case Ors(SetExtractor(Equals(_, _), Equals(_, _))) => () })
  }

  ignore("Simplify AND of identical value with parenthesis") {
    assertRewrittenMatches(
      "$n = 2 AND ($n = 2 AND $m = 3)",
      { case Ands(SetExtractor(Equals(_, _), Equals(_, _))) => () }
    )
  }

  test("Simplify AND of lists") {
    assertRewrittenMatches("[] AND [] AND []", { case CoerceToPredicate(ListLiteral(Seq())) => () })
  }

  test("Simplify AND of different data types") {
    assertRewrittenMatches("$n = 2 AND $n = 2.0", { case Ands(SetExtractor(Equals(_, _), Equals(_, _))) => () })
  }

  test("Simplify AND of identical value with greater than") {
    assertRewrittenMatches("$n > 2 AND $n > 2", { case GreaterThan(_, _) => () })
  }

  test("Simplify AND of identical expressions with function") {
    // For expressions that are non-idempotent (or not referentially transparent) like rand(), this rewrite can affect semantics
    assertRewrittenMatches("rand() = 1 AND rand() = 1", { case Equals(_, _) => () })
  }

  test("should split all() into multiple expressions without duplicating variable references") {
    assertRewrittenMatches("all(x IN list WHERE x > 123 AND x < 321 AND x % 2 = 0)") {
      case rewrittenExpr @ Ands(SetExtractor(
          _: AllIterablePredicate,
          _: AllIterablePredicate,
          _: AllIterablePredicate
        )) =>
        noReferenceEqualityAmongVariables(rewrittenExpr) shouldBe empty
    }
  }

  private val exceptionFactory = new OpenCypherExceptionFactory(None)

  private def assertRewrittenMatches(originalQuery: String, matcher: PartialFunction[Any, Unit]): Unit = {
    val original = JavaCCParser.parse("RETURN " + originalQuery, exceptionFactory)
    val checkResult = original.semanticCheck.run(SemanticState.clean, SemanticCheckContext.default)
    val rewriter = flattenBooleanOperators andThen simplifyPredicates(checkResult.state)
    val result = original.endoRewrite(rewriter)
    val maybeReturnExp = result.folder.treeFind({
      case UnaliasedReturnItem(expression, _) => {
        assert(matcher.lift(expression).isDefined, expression)
        true
      }
    }: PartialFunction[AnyRef, Boolean])
    assert(maybeReturnExp.isDefined, "Could not find return in parsed query!")
  }

  private def assertRewrittenMatches(originalQuery: String)(matcher: PartialFunction[Any, Unit])(implicit
  d: DummyImplicit): Unit = {
    assertRewrittenMatches(originalQuery, matcher)
  }
}
