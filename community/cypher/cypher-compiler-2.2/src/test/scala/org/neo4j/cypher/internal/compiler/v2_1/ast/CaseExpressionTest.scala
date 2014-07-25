/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.symbols._

class CaseExpressionTest extends CypherFunSuite {

  test("shouldHaveMergedTypesOfAllAlternativesInSimpleCase") {
    val caseExpression = CaseExpression(
      expression = Some(DummyExpression(CTString)),
      alternatives = Seq(
        (
          DummyExpression(CTString),
          DummyExpression(CTFloat)
        ), (
          DummyExpression(CTString),
          DummyExpression(CTInteger)
        )
      ),
      default = Some(DummyExpression(CTFloat))
    )(DummyPosition(2))

    val result = caseExpression.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    caseExpression.types(result.state) should equal(CTNumber.invariant)
  }

  test("shouldHaveMergedTypesOfAllAlternativesInGenericCase") {
    val caseExpression = CaseExpression(
      None,
      Seq(
        (
          DummyExpression(CTBoolean),
          DummyExpression(CTFloat | CTString)
        ), (
          DummyExpression(CTBoolean),
          DummyExpression(CTInteger)
        )
      ),
      Some(DummyExpression(CTFloat | CTNode))
    )(DummyPosition(2))

    val result = caseExpression.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors shouldBe empty
    caseExpression.types(result.state) should equal(CTNumber | CTAny)
  }

  test("shouldTypeCheckPredicatesInGenericCase") {
    val caseExpression = CaseExpression(
      None,
      Seq(
        (
          DummyExpression(CTBoolean),
          DummyExpression(CTFloat)
        ), (
          DummyExpression(CTString, DummyPosition(12)),
          DummyExpression(CTInteger)
        )
      ),
      Some(DummyExpression(CTFloat))
    )(DummyPosition(2))

    val result = caseExpression.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    result.errors should have size 1
    result.errors.head.msg should equal("Type mismatch: expected Boolean but was String")
    result.errors.head.position should equal(DummyPosition(12))
  }

}
