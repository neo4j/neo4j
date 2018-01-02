/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class SimpleCaseTest extends CypherFunSuite {

  test("case_with_single_alternative_works") {
    //GIVEN
    val caseExpr = case_(1,
      1 -> "one"
    )

    //WHEN
    val result = caseExpr(ExecutionContext.empty)(QueryStateHelper.empty)

    //THEN
    result should equal("one")
  }

  test("case_with_two_alternatives_picks_the_second") {
    //GIVEN
    val caseExpr = case_(2,
      1 -> "one",
      2 -> "two"
    )

    //WHEN
    val result = caseExpr(ExecutionContext.empty)(QueryStateHelper.empty)

    //THEN
    result should equal("two")
  }

  test("case_with_no_match_returns_null") {
    //GIVEN
    val caseExpr = case_(3,
      1 -> "one",
      2 -> "two"
    )

    //WHEN
    val result = caseExpr(ExecutionContext.empty)(QueryStateHelper.empty)

    //THEN
    result should equal(null.asInstanceOf[Any])
  }

  test("case_with_no_match_returns_default") {
    //GIVEN
    val caseExpr = case_(3,
      1 -> "one",
      2 -> "two"
    ) defaultsTo "default"

    //WHEN
    val result = caseExpr(ExecutionContext.empty)(QueryStateHelper.empty)

    //THEN
    result should equal("default")
  }

  test("when_the_input_expression_is_null_return_the_else_case") {
    //GIVEN
    val caseExpr = case_(null,
      1 -> "one",
      2 -> "two"
    ) defaultsTo "default"

    //WHEN
    val result = caseExpr(ExecutionContext.empty)(QueryStateHelper.empty)

    //THEN
    assert(result == "default")
  }

  private def case_(in: Any, alternatives: (Any, Any)*): SimpleCase = {
    val mappedAlt: Seq[(Expression, Expression)] = alternatives.map {
      case (a, b) => (Literal(a), Literal(b))
    }

    SimpleCase(Literal(in), mappedAlt, None)
  }

  implicit class SimpleCasePimp(in:SimpleCase) {
    def defaultsTo(a:Any): SimpleCase = SimpleCase(in.expression, in.alternatives, Some(Literal(a)))
  }
}
