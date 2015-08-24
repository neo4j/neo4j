/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Interpolation, Literal, Null}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{MatchDynamicRegex, MatchLiteralRegex}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class MatchRegexTest extends CypherFunSuite {

  test("MatchLiteralRegex: should not match if the lhs expression evaluates to null") {
    val expression = new MatchLiteralRegex(Null(), Literal(".*"))
    expression.isMatch(null)(QueryStateHelper.empty) should equal(None)
  }

  test("MatchDynamicRegex: should not match if the lhs expression evaluates to null") {
    val expression = new MatchDynamicRegex(Null(), Literal(".*"))
    expression.isMatch(null)(QueryStateHelper.empty) should equal(None)
  }

  test("MatchDynamicRegex: should not match if the lhs expression evaluates to something that is not a string"){
    val expression = new MatchDynamicRegex(Literal(5), Literal(".*"))
    expression.isMatch(null)(QueryStateHelper.empty) should equal(None)
  }

  test("MatchLiteralRegex: should not match if the lhs expression evaluates to something that is not a string"){
    val expression = new MatchLiteralRegex(Literal(5), Literal(".*"))
    expression.isMatch(null)(QueryStateHelper.empty) should equal(None)
  }

  test("MatchDynamicRegex: should match pattern to string") {
    val expression1 = new MatchDynamicRegex(Literal("value"), Literal("v[a-z]+"))
    expression1.isMatch(null)(QueryStateHelper.empty) should equal(Some(true))

    val expression2 = new MatchDynamicRegex(Literal("NO-MATCH"), Literal("v[a-z]+"))
    expression2.isMatch(null)(QueryStateHelper.empty) should equal(Some(false))
  }

  test("MatchLiteralRegex: should match pattern to string") {
    val expression1 = new MatchLiteralRegex(Literal("value"), Literal("v[a-z]+"))
    expression1.isMatch(null)(QueryStateHelper.empty) should equal(Some(true))

    val expression2 = new MatchLiteralRegex(Literal("NO-MATCH"), Literal("v[a-z]+"))
    expression2.isMatch(null)(QueryStateHelper.empty) should equal(Some(false))
  }

  test("MatchDynamicRegex: should match interpolated string part just as string") {
    val expression1 = MatchDynamicRegex(Literal("value"), Interpolation(NonEmptyList(Left(Literal("value")))))
    expression1.isMatch(null)(QueryStateHelper.empty) should equal(Some(true))

    val expression2 = MatchDynamicRegex(Literal("value2"), Interpolation(NonEmptyList(Left(Literal("value")))))
    expression2.isMatch(null)(QueryStateHelper.empty) should equal(Some(false))

    val expression3 = MatchDynamicRegex(Literal("value"), Interpolation(NonEmptyList(Left(Literal(".+")))))
    expression3.isMatch(null)(QueryStateHelper.empty) should equal(Some(false))
  }

  test("MatchDynamicRegex: should match literal string part just as regex") {
    val expression1 = MatchDynamicRegex(Literal("value"), Interpolation(NonEmptyList(Right(".+"))))
    expression1.isMatch(null)(QueryStateHelper.empty) should equal(Some(true))

    val expression2 = MatchDynamicRegex(Literal("value"), Interpolation(NonEmptyList(Right("V.+"))))
    expression2.isMatch(null)(QueryStateHelper.empty) should equal(Some(false))
  }

  test("MatchDynamicRegex: should match complex interpolations") {
    val expression1 = MatchDynamicRegex(Literal("abbbc"), Interpolation(NonEmptyList(Left(Literal("a")), Right("b+"), Left(Literal("c")))))
    expression1.isMatch(null)(QueryStateHelper.empty) should equal(Some(true))
  }
}
