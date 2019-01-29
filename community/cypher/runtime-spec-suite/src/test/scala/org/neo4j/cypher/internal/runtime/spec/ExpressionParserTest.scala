/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.v4_0.expressions.{Add, HasLabels, LabelName, Property, PropertyKeyName, SignedDecimalIntegerLiteral, Variable}
import org.neo4j.cypher.internal.v4_0.logical.plans.CachedNodeProperty
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.test_helpers.{CypherFunSuite, TestName}

class ExpressionParserTest extends CypherFunSuite with TestName
{
  private val pos = InputPosition.NONE

  test("a AS b") {
    ExpressionParser.parseProjections(testName) should be(Map("b" -> Variable("a")(pos)))
  }

  // Finds cached node property
  test("cached[n.prop] AS b") {
    ExpressionParser.parseProjections(testName) should be(Map("b" -> CachedNodeProperty("n", PropertyKeyName("prop")(pos))(pos)))
  }

  test("b.foo + 5 AS abc09") {
    ExpressionParser.parseProjections(testName) should be(Map("abc09" -> Add(Property(Variable("b")(pos), PropertyKeyName("foo")(pos))(pos), SignedDecimalIntegerLiteral("5")(pos))(pos)))
  }

  // Finds nested cached node property
  test("cached[b.foo] + 5 AS abc09") {
    ExpressionParser.parseProjections(testName) should be(Map("abc09" -> Add(CachedNodeProperty("b", PropertyKeyName("foo")(pos))(pos), SignedDecimalIntegerLiteral("5")(pos))(pos)))
  }

  test("n:Label") {
    ExpressionParser.parseExpression(testName) should be(HasLabels(Variable("n")(pos), Seq(LabelName("Label")(pos)))(pos))
  }
}
