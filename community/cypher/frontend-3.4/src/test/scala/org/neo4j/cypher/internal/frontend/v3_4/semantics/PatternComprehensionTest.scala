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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.apa.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions._

class PatternComprehensionTest extends SemanticFunSuite {

  val n = NodePattern(Some(variable("n")), Seq.empty, None)(pos)
  val x = NodePattern(Some(variable("x")), Seq.empty, None)(pos)
  val r = RelationshipPattern(None, Seq.empty, None, None, SemanticDirection.OUTGOING)(pos)
  val pattern = RelationshipsPattern(RelationshipChain(n, r, x)(pos))(pos)
  val property = Property(variable("x"), PropertyKeyName("prop")(pos))(pos)
  val failingProperty = Property(variable("missing"), PropertyKeyName("prop")(pos))(pos)
  val stringLiteral = StringLiteral("APA")(pos)

  test("pattern comprehension on a property returns the expected type") {
    val expression = PatternComprehension(None, pattern, None, property)(pos)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe empty
    types(expression)(result.state) should equal(CTList(CTAny).covariant)
  }

  test("pattern comprehension with literal string projection has correct type") {
    val expression = PatternComprehension(None, pattern, None, stringLiteral)(pos)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe empty
    types(expression)(result.state) should equal(CTList(CTString).invariant)
  }

  test("inner projection using missing identifier reports error") {
    val expression = PatternComprehension(None, pattern, None, failingProperty)(pos)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("inner filter using missing identifier reports error") {
    val expression = PatternComprehension(None, pattern, Some(failingProperty), stringLiteral)(pos)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("pattern can't reuse identifier with different type") {
    val expression = PatternComprehension(None, pattern, None, stringLiteral)(pos)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTBoolean).right.get
    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe Seq(
      SemanticError("Type mismatch: n already defined with conflicting type Boolean (expected Node)", pos, pos)
    )
  }
}
