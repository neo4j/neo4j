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

package org.neo4j.cypher.internal.frontend.phases.rewriting

import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaCCParser
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.mergeDuplicateBooleanOperatorsRewriter
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class MergeBooleanOperatorsTest extends CypherFunSuite {


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
    assertRewrittenMatches("$n = 2 OR $n = 3", { case Or(Equals(_, _), Equals(_, _)) => () })
  }

  ignore("Simplify AND of identical value spread apart") {
    assertRewrittenMatches("$n = 2 AND $m = 3 AND $n = 2", { case And(Equals(_, _), Equals(_, _)) => () })
  }

  ignore("Simplify OR of identical value spread apart") {
    assertRewrittenMatches("$n = 2 OR $m = 3 OR $n = 2", { case Or(Equals(_, _), Equals(_, _)) => () })
  }

  ignore("Simplify AND of identical value with parenthesis") {
    assertRewrittenMatches("$n = 2 AND ($n = 2 AND $m = 3)", { case And(Equals(_, _), Equals(_, _)) => () })
  }

  test("Simplify AND of lists") {
    assertRewrittenMatches("[] AND [] AND []", { case CoerceToPredicate(ListLiteral(List())) => () })
  }

  test("Simplify AND of different data types") {
    assertRewrittenMatches("$n = 2 AND $n = 2.0", { case And(Equals(_, _), Equals(_, _)) => () })
  }

  test("Simplify AND of identical value with greater than") {
    assertRewrittenMatches("$n > 2 AND $n > 2", { case GreaterThan(_, _) => () })
  }

  test("Simplify AND of identical expressions with function") {
    // For expressions that are non-idempotent (or not referentially transparent) like rand(), this rewrite can affect semantics
    assertRewrittenMatches("rand() = 1 AND rand() = 1", { case Equals(_, _) => () })
  }

  test("Do not simplify expressions with different auto extracted parameters") {
    val position = InputPosition(0, 0, 0)
    // AST for $n = 2 OR $n = 3
    val ast = Ors(Seq(
      Equals(ExplicitParameter("n", CTAny)(position), AutoExtractedParameter("AUTOINT0", CTInteger, SignedDecimalIntegerLiteral("2")(position))(position))(position),
      Equals(ExplicitParameter("n", CTAny)(position), AutoExtractedParameter("AUTOINT1", CTInteger, SignedDecimalIntegerLiteral("3")(position))(position))(position)
    ))(position)
    val rewriter = mergeDuplicateBooleanOperatorsRewriter(SemanticState.clean)
    val result = ast.rewrite(rewriter)
    ast should equal(result)
  }


  private val exceptionFactory = new OpenCypherExceptionFactory(None)
  private def assertRewrittenMatches(originalQuery: String, matcher: PartialFunction[Any, Unit]): Unit = {
    val original = JavaCCParser.parse("RETURN " +  originalQuery, exceptionFactory, new AnonymousVariableNameGenerator())
    val checkResult = original.semanticCheck(SemanticState.clean)
    val rewriter = mergeDuplicateBooleanOperatorsRewriter(checkResult.state)
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
