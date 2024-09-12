/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.CoercedPredicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Equals
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.stringValue

class CaseTest extends CypherFunSuite {

  test("case_with_single_alternative_works") {
    // GIVEN
    val caseExpr = genericCase(
      (1, 1) -> "one"
    )

    // WHEN
    val result = caseExpr(CypherRow.empty, QueryStateHelper.empty)

    // THEN
    result should equal(stringValue("one"))
  }

  test("case_with_two_alternatives_picks_the_second") {
    // GIVEN
    val caseExpr = genericCase(
      (1, 2) -> "one",
      (2, 2) -> "two"
    )

    // WHEN
    val result = caseExpr(CypherRow.empty, QueryStateHelper.empty)

    // THEN
    result should equal(stringValue("two"))
  }

  test("case_with_no_match_returns_null") {
    // GIVEN
    val caseExpr = genericCase(
      (1, 2) -> "one",
      (2, 3) -> "two"
    )

    // WHEN
    val result = caseExpr(CypherRow.empty, QueryStateHelper.empty)

    // THEN
    result should equal(NO_VALUE)
  }

  test("case_with_no_match_returns_default") {
    // GIVEN
    val caseExpr = genericCase(
      (1, 2) -> "one",
      (2, 3) -> "two"
    ) defaultsTo "other"

    // WHEN
    val result = caseExpr(CypherRow.empty, QueryStateHelper.empty)

    // THEN
    result should equal(stringValue("other"))
  }

  test("case_with_a_single_null_value_uses_the_default") {
    // GIVEN CASE WHEN null THEN 42 ELSE "defaults"
    val caseExpr = CaseExpression(IndexedSeq(CoercedPredicate(Null()) -> literal(42)), Some(literal("defaults")))

    // WHEN
    val result = caseExpr(CypherRow.empty, QueryStateHelper.empty)

    // THEN
    assert(result === stringValue("defaults"))
  }

  test("simple_case_with_single_alternative_works") {
    // GIVEN
    val caseExpr = simpleCase(1, 1 -> "one")

    // WHEN
    val result = caseExpr(CypherRow.empty, QueryStateHelper.empty)

    // THEN
    result should equal(stringValue("one"))
  }

  test("simple_case_with_two_alternatives_picks_the_second") {
    // GIVEN
    val caseExpr = simpleCase(2, 1 -> "one", 2 -> "two")

    // WHEN
    val result = caseExpr(CypherRow.empty, QueryStateHelper.empty)

    // THEN
    result should equal(stringValue("two"))
  }

  test("simple_case_with_no_match_returns_null") {
    // GIVEN
    val caseExpr = simpleCase(3, 1 -> "one", 2 -> "two")

    // WHEN
    val result = caseExpr(CypherRow.empty, QueryStateHelper.empty)

    // THEN
    result should equal(NO_VALUE)
  }

  test("simple_case_with_no_match_returns_default") {
    // GIVEN
    val caseExpr = simpleCase(3, 1 -> "one", 2 -> "two") defaultsTo "default"

    // WHEN
    val result = caseExpr(CypherRow.empty, QueryStateHelper.empty)

    // THEN
    result should equal(stringValue("default"))
  }

  test("simple_when_the_input_expression_is_null_return_the_else_case") {
    // GIVEN
    val caseExpr = simpleCase(null, 1 -> "one", 2 -> "two") defaultsTo "default"

    // WHEN
    val result = caseExpr(CypherRow.empty, QueryStateHelper.empty)

    // THEN
    assert(result == stringValue("default"))
  }

  test("simple case arguments should contain all children") {
    val caseExpr = CaseExpression(IndexedSeq((Equals(literal(1), literal(2)), literal(3))), Some(literal(4)))
    caseExpr.arguments should contain.allOf(Equals(literal(1), literal(2)), literal(3), literal(4))
  }

  private def simpleCase(in: Any, alternatives: (Any, Any)*): CaseExpression = {
    val mappedAlt: IndexedSeq[(Predicate, Expression)] = alternatives.toIndexedSeq.map {
      case (a, b) => (Equals(literal(in), literal(a)), literal(b))
    }

    CaseExpression(mappedAlt, None)
  }

  private def genericCase(alternatives: ((Any, Any), Any)*): CaseExpression = {
    val mappedAlt: IndexedSeq[(Predicate, Expression)] = alternatives.toIndexedSeq.map {
      case ((a, b), c) => (Equals(literal(a), literal(b)), literal(c))
    }

    CaseExpression(mappedAlt, None)
  }

  implicit class SimpleCasePimp(in: CaseExpression) {
    def defaultsTo(a: Any): CaseExpression = CaseExpression(in.alternatives, Some(literal(a)))
  }
}
