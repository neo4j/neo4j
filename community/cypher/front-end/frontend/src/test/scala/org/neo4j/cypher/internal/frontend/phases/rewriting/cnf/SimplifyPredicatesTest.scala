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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

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
    assertRewrittenMatches("NOT NOT 'P' OR NOT NOT 'Q'", { case Ors(List(StringLiteral("P"), StringLiteral("Q"))) => () })
  }

  test("Do not simplify expressions with different auto extracted parameters") {
    val position = InputPosition(0, 0, 0)
    // AST for $n = 2 OR $n = 3
    val ast = Ors(Seq(
      Equals(ExplicitParameter("n", CTAny)(position), AutoExtractedParameter("AUTOINT0", CTInteger, SignedDecimalIntegerLiteral("2")(position))(position))(position),
      Equals(ExplicitParameter("n", CTAny)(position), AutoExtractedParameter("AUTOINT1", CTInteger, SignedDecimalIntegerLiteral("3")(position))(position))(position)
    ))(position)
    val rewriter = flattenBooleanOperators andThen simplifyPredicates(SemanticState.clean)
    val result = ast.rewrite(rewriter)
    ast should equal(result)
  }


  private val exceptionFactory = new OpenCypherExceptionFactory(None)
  private def assertRewrittenMatches(originalQuery: String, matcher: PartialFunction[Any, Unit]): Unit = {
    val original = JavaCCParser.parse("RETURN " +  originalQuery, exceptionFactory, new AnonymousVariableNameGenerator())
    val checkResult = original.semanticCheck(SemanticState.clean)
    val rewriter = flattenBooleanOperators andThen simplifyPredicates(checkResult.state)
    val result = original.rewrite(rewriter)
    val maybeReturnExp = result.treeFind ({
      case UnaliasedReturnItem(expression, _) => {
        assert(matcher.isDefinedAt(expression), expression)
        true
      }
    } : PartialFunction[AnyRef, Boolean])
    assert(maybeReturnExp.isDefined, "Could not find return in parsed query!")
  }
}
