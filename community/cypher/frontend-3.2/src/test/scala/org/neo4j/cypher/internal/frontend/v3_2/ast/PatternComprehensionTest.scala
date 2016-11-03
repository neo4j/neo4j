/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.frontend.v3_2.ast

import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_2.{SemanticDirection, SemanticError, SemanticState}

class PatternComprehensionTest extends CypherFunSuite with AstConstructionTestSupport {

  val n = NodePattern(Some(varFor("n")), Seq.empty, None)(pos)
  val x = NodePattern(Some(varFor("x")), Seq.empty, None)(pos)
  val r = RelationshipPattern(None, optional = false, Seq.empty, None, None, SemanticDirection.OUTGOING)(pos)
  val pattern = RelationshipsPattern(RelationshipChain(n, r, x)(pos))(pos)
  val property = Property(varFor("x"), PropertyKeyName("prop")(pos))(pos)
  val failingProperty = Property(varFor("missing"), PropertyKeyName("prop")(pos))(pos)
  val stringLiteral = StringLiteral("APA")(pos)

  test("pattern comprehension on a property returns the expected type") {
    val expression = PatternComprehension(None, pattern, None, property)(pos)

    val result = expression.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)

    result.errors shouldBe empty
    expression.types(result.state) should equal(CTList(CTAny).covariant)
  }

  test("pattern comprehension with literal string projection has correct type") {
    val expression = PatternComprehension(None, pattern, None, stringLiteral)(pos)

    val result = expression.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)

    result.errors shouldBe empty
    expression.types(result.state) should equal(CTList(CTString).invariant)
  }

  test("inner projection using missing identifier reports error") {
    val expression = PatternComprehension(None, pattern, None, failingProperty)(pos)

    val result = expression.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)

    val errors: Seq[SemanticError] = result.errors
    errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("inner filter using missing identifier reports error") {
    val expression = PatternComprehension(None, pattern, Some(failingProperty), stringLiteral)(pos)

    val result = expression.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)

    result.errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("pattern can't reuse identifier with different type") {
    val expression = PatternComprehension(None, pattern, None, stringLiteral)(pos)

    val semanticState = SemanticState.clean.declareVariable(varFor("n"), CTBoolean).right.get
    val result = expression.semanticCheck(Expression.SemanticContext.Simple)(semanticState)

    result.errors shouldBe Seq(
      SemanticError("Type mismatch: n already defined with conflicting type Boolean (expected Node)", pos, pos)
    )
  }
}
