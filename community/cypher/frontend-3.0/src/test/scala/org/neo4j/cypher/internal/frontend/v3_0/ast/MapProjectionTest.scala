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
package org.neo4j.cypher.internal.frontend.v3_0.ast

import org.neo4j.cypher.internal.frontend.v3_0.parser.{Expressions, ParserTest}
import org.neo4j.cypher.internal.frontend.v3_0.{DummyPosition, ast}

class MapProjectionTest extends ParserTest[Any, Any] with Expressions {

  val t = DummyPosition(0)

  test("testIdentifierCanContainASCII") {
    implicit val parserToTest = MapProjection

    parsing("abc{}") shouldGive ast.MapProjection(ast.Identifier("abc")(t), Seq.empty)(t)

    parsing("abc{.id}") shouldGive
      ast.MapProjection(ast.Identifier("abc")(t),
        Seq(ast.PropertySelector(ast.Identifier("id")(t))(t)))(t)

    parsing("abc{id}") shouldGive
      ast.MapProjection(ast.Identifier("abc")(t),
        Seq(ast.IdentifierSelector(ast.Identifier("id")(t))(t)))(t)

    parsing("abc { id : 42 }") shouldGive
      ast.MapProjection(ast.Identifier("abc")(t),
        Seq(ast.LiteralEntry(ast.PropertyKeyName("id")(t), SignedDecimalIntegerLiteral("42")(t))(t)))(t)

    parsing("abc { id : 42, .foo, bar }") shouldGive
      ast.MapProjection(ast.Identifier("abc")(t),
        Seq(
          ast.LiteralEntry(ast.PropertyKeyName("id")(t), SignedDecimalIntegerLiteral("42")(t))(t),
          ast.PropertySelector(ast.Identifier("foo")(t))(t),
          ast.IdentifierSelector(ast.Identifier("bar")(t))(t)
        )
      )(t)
  }

  def convert(result: Any): Any = result
}
