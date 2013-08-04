/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.experimental.ast

import org.neo4j.cypher.internal.symbols._
import org.junit.Test
import org.scalatest.Assertions
import org.neo4j.cypher.internal.parser.experimental._

class CaseExpressionTest extends Assertions {

  @Test
  def shouldHaveMergedTypesOfAllAlternativesInSimpleCase() {
    val caseExpression = CaseExpression(
      Some(DummyExpression(TypeSet(StringType()), DummyToken(2, 3))),
      Seq(
        (
          DummyExpression(TypeSet(StringType()), DummyToken(5, 7)),
          DummyExpression(TypeSet(DoubleType()), DummyToken(10, 11))
        ), (
          DummyExpression(TypeSet(StringType()), DummyToken(12, 15)),
          DummyExpression(TypeSet(IntegerType()), DummyToken(17, 20))
        )
      ),
      Some(DummyExpression(TypeSet(DoubleType()), DummyToken(22, 25))),
      DummyToken(2, 25)
    )

    val result = caseExpression.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assert(result.errors === Seq())
    assert(caseExpression.types(result.state) === Set(NumberType()))
  }

  @Test
  def shouldHaveMergedTypesOfAllAlternativesInGenericCase() {
    val caseExpression = CaseExpression(
      None,
      Seq(
        (
          DummyExpression(TypeSet(BooleanType()), DummyToken(5, 7)),
          DummyExpression(TypeSet(DoubleType(), StringType()), DummyToken(10, 11))
        ), (
          DummyExpression(TypeSet(BooleanType()), DummyToken(12, 15)),
          DummyExpression(TypeSet(IntegerType()), DummyToken(17, 20))
        )
      ),
      Some(DummyExpression(TypeSet(DoubleType(), NodeType()), DummyToken(22, 25))),
      DummyToken(2, 25)
    )

    val result = caseExpression.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assert(result.errors === Seq())
    assert(caseExpression.types(result.state) === Set(NumberType(), ScalarType()))
  }

  @Test
  def shouldTypeCheckPredicatesInGenericCase() {
    val caseExpression = CaseExpression(
      None,
      Seq(
        (
          DummyExpression(TypeSet(BooleanType()), DummyToken(5, 7)),
          DummyExpression(TypeSet(DoubleType()), DummyToken(10, 11))
        ), (
          DummyExpression(TypeSet(StringType()), DummyToken(12, 15)),
          DummyExpression(TypeSet(IntegerType()), DummyToken(17, 20))
        )
      ),
      Some(DummyExpression(TypeSet(DoubleType()), DummyToken(22, 25))),
      DummyToken(2, 25)
    )

    val result = caseExpression.semanticCheck(Expression.SemanticContext.Simple)(SemanticState.clean)
    assert(result.errors.size === 1)
    assert(result.errors.head.msg === "Type mismatch: expected Boolean but was String")
    assert(result.errors.head.token === DummyToken(12,15))
  }

}
