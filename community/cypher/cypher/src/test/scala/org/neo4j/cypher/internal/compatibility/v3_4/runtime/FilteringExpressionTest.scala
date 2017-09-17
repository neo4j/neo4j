/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.apa.v3_4.DummyPosition
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticCheckResult, SemanticError, SemanticExpressionCheck, SemanticState}
import org.neo4j.cypher.internal.apa.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, FilteringExpression, Variable}

class FilteringExpressionTest extends CypherFunSuite {

  case class TestableFilteringExpression(variable: Variable, expression: Expression, innerPredicate: Option[Expression]) extends FilteringExpression {
    def name = "Testable Filter Expression"
    def position = DummyPosition(0)

    def toCommand(command: expressions.Expression, name: String, inner: Predicate) = ???

    def toPredicate(command: expressions.Expression, name: String, inner: Predicate) = ???
  }

  test("shouldSemanticCheckPredicateInStateContainingTypedVariable") {
    val expression = DummyExpression(CTList(CTNode) | CTBoolean | CTList(CTString), DummyPosition(5))

    val error = SemanticError("dummy error", DummyPosition(8))
    val predicate = CustomExpression(
      (ctx, self) => s => {
        s.symbolTypes("x") should equal(CTNode | CTString)
        SemanticCheckResult.error(s, error)
      }
    )

    val filter = TestableFilteringExpression(Variable("x")(DummyPosition(2)), expression, Some(predicate))
    val result = SemanticExpressionCheck.simple(filter)(SemanticState.clean)
    result.errors should equal(Seq(error))
    result.state.symbol("x") should equal(None)
  }
}
