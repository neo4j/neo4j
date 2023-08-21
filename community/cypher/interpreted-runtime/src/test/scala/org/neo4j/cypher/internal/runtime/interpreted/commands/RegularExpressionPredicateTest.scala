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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Null
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsFalse
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsTrue
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.IsUnknown
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.LiteralRegularExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.RegularExpression
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class RegularExpressionPredicateTest extends CypherFunSuite {

  test("LiteralRegEx: should not match if the lhs expression evaluates to null") {
    val expression = LiteralRegularExpression(Null(), literal(".*"))
    expression.isMatch(null, QueryStateHelper.empty) should equal(IsUnknown)
  }

  test("RegEx: should not match if the lhs expression evaluates to null") {
    val expression = RegularExpression(Null(), literal(".*"))
    expression.isMatch(null, QueryStateHelper.empty) should equal(IsUnknown)
  }

  test("RegEx: should not match if the lhs expression evaluates to something that is not a string") {
    val expression = RegularExpression(literal(5), literal(".*"))
    expression.isMatch(null, QueryStateHelper.empty) should equal(IsUnknown)
  }

  test("LiteralRegEx: should not match if the lhs expression evaluates to something that is not a string") {
    val expression = LiteralRegularExpression(literal(5), literal(".*"))
    expression.isMatch(null, QueryStateHelper.empty) should equal(IsUnknown)
  }

  test("RegEx: should match pattern to string") {
    val expression1 = RegularExpression(literal("value"), literal("v[a-z]+"))
    expression1.isMatch(null, QueryStateHelper.empty) should equal(IsTrue)

    val expression2 = RegularExpression(literal("NO-MATCH"), literal("v[a-z]+"))
    expression2.isMatch(null, QueryStateHelper.empty) should equal(IsFalse)
  }

  test("LiteralRegEx: should match pattern to string") {
    val expression1 = LiteralRegularExpression(literal("value"), literal("v[a-z]+"))
    expression1.isMatch(null, QueryStateHelper.empty) should equal(IsTrue)

    val expression2 = LiteralRegularExpression(literal("NO-MATCH"), literal("v[a-z]+"))
    expression2.isMatch(null, QueryStateHelper.empty) should equal(IsFalse)
  }
}
