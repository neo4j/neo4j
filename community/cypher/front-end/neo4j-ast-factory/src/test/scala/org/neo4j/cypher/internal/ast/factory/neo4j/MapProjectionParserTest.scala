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
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.DummyPosition

class MapProjectionParserTest extends JavaccParserTestBase[Any, Any] {

  private val t = DummyPosition(0)

  test("testIdentifierCanContainASCII") {
    implicit val parser: JavaccRule[Expression] = JavaccRule.MapProjection

    parsing("abc{}") shouldGive expressions.MapProjection(expressions.Variable("abc")(t), Seq.empty)(t)

    parsing("abc{.id}") shouldGive
      expressions.MapProjection(
        expressions.Variable("abc")(t),
        Seq(expressions.PropertySelector(expressions.Variable("id")(t))(t))
      )(t)

    parsing("abc{id}") shouldGive
      expressions.MapProjection(
        expressions.Variable("abc")(t),
        Seq(expressions.VariableSelector(expressions.Variable("id")(t))(t))
      )(t)

    parsing("abc { id : 42 }") shouldGive
      expressions.MapProjection(
        expressions.Variable("abc")(t),
        Seq(expressions.LiteralEntry(expressions.PropertyKeyName("id")(t), SignedDecimalIntegerLiteral("42")(t))(t))
      )(t)

    parsing("abc { `a p a` : 42 }") shouldGive
      expressions.MapProjection(
        expressions.Variable("abc")(t),
        Seq(expressions.LiteralEntry(expressions.PropertyKeyName("a p a")(t), SignedDecimalIntegerLiteral("42")(t))(t))
      )(t)

    parsing("abc { id : 42, .foo, bar }") shouldGive
      expressions.MapProjection(
        expressions.Variable("abc")(t),
        Seq(
          expressions.LiteralEntry(expressions.PropertyKeyName("id")(t), SignedDecimalIntegerLiteral("42")(t))(t),
          expressions.PropertySelector(expressions.Variable("foo")(t))(t),
          expressions.VariableSelector(expressions.Variable("bar")(t))(t)
        )
      )(t)
  }

  def convert(result: Any): Any = result
}
