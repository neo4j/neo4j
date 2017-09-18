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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions.{Literal, Null}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.predicates.{LiteralRegularExpression, RegularExpression}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite

class RegularExpressionPredicateTest extends CypherFunSuite {
  test("LiteralRegEx: should not match if the lhs expression evaluates to null") {
    val expression = LiteralRegularExpression(Null(), Literal(".*"))
    expression.isMatch(null, QueryStateHelper.empty) should equal(None)
  }

  test("RegEx: should not match if the lhs expression evaluates to null") {
    val expression = RegularExpression(Null(), Literal(".*"))
    expression.isMatch(null, QueryStateHelper.empty) should equal(None)
  }

  test("RegEx: should not match if the lhs expression evaluates to something that is not a string"){
    val expression = RegularExpression(Literal(5), Literal(".*"))
    expression.isMatch(null, QueryStateHelper.empty) should equal(None)
  }

  test("LiteralRegEx: should not match if the lhs expression evaluates to something that is not a string"){
    val expression = LiteralRegularExpression(Literal(5), Literal(".*"))
    expression.isMatch(null, QueryStateHelper.empty) should equal(None)
  }

  test("RegEx: should match pattern to string") {
    val expression1 = RegularExpression(Literal("value"), Literal("v[a-z]+"))
    expression1.isMatch(null, QueryStateHelper.empty) should equal(Some(true))

    val expression2 = RegularExpression(Literal("NO-MATCH"), Literal("v[a-z]+"))
    expression2.isMatch(null, QueryStateHelper.empty) should equal(Some(false))
  }

  test("LiteralRegEx: should match pattern to string") {
    val expression1 = LiteralRegularExpression(Literal("value"), Literal("v[a-z]+"))
    expression1.isMatch(null, QueryStateHelper.empty) should equal(Some(true))

    val expression2 = LiteralRegularExpression(Literal("NO-MATCH"), Literal("v[a-z]+"))
    expression2.isMatch(null, QueryStateHelper.empty) should equal(Some(false))
  }
}
