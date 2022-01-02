/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.DummyPosition

class ListComprehensionTest extends JavaccParserTestBase[Expression, Any] {
  private val t = DummyPosition(0)

  implicit private val parser: JavaccRule[Expression] = JavaccRule.fromParser(_.ListComprehension())

  test("tests") {

    parsing("[ a in p WHERE a.foo > 123 ]") shouldGive
      expressions.ListComprehension(ExtractScope(expressions.Variable("a")(t),
                                         Some(GreaterThan(
                                           Property(expressions.Variable("a")(t),
                                                    expressions.PropertyKeyName("foo")(t))(t),
                                           SignedDecimalIntegerLiteral("123")(t))(t)),
                                         None)(t),
                            expressions.Variable("p")(t))(t)

    parsing("[ a in p | a.foo ]") shouldGive
      expressions.ListComprehension(ExtractScope(expressions.Variable("a")(t),
                                         None,
                                         Some(Property(expressions.Variable("a")(t),expressions.PropertyKeyName("foo")(t))(t))
                                        )(t),
                            expressions.Variable("p")(t))(t)

    parsing("[ a in p WHERE a.foo > 123 | a.foo ]") shouldGive
      expressions.ListComprehension(ExtractScope(expressions.Variable("a")(t),
                                         Some(GreaterThan(
                                           Property(expressions.Variable("a")(t),
                                                    expressions.PropertyKeyName("foo")(t))(t),
                                           SignedDecimalIntegerLiteral("123")(t))(t)),
                                         Some(Property(expressions.Variable("a")(t),expressions.PropertyKeyName("foo")(t))(t))
                                        )(t),
                            expressions.Variable("p")(t))(t)
  }

  override def convert(result: Expression): Any = result
}
