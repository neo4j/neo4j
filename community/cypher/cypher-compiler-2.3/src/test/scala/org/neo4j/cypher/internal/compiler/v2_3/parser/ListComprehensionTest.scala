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
package org.neo4j.cypher.internal.compiler.v2_3.parser

import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.GreaterThan
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_3.commands.{expressions => legacy}
import org.neo4j.cypher.internal.frontend.v2_3.ast
import org.neo4j.cypher.internal.frontend.v2_3.parser.{Expressions, ParserTest}
import org.parboiled.scala._

class ListComprehensionTest extends ParserTest[ast.ListComprehension, legacy.Expression] with Expressions {
  implicit val parserToTest = ListComprehension ~ EOI

  test("tests") {
    val filterCommand = legacy.FilterFunction(
      legacy.Identifier("p"),
      "a",
      GreaterThan(legacy.Property(legacy.Identifier("a"), PropertyKey("foo")), legacy.Literal(123)))

    parsing("[ a in p WHERE a.foo > 123 ]") shouldGive filterCommand

    parsing("[ a in p | a.foo ]") shouldGive
      legacy.ExtractFunction(legacy.Identifier("p"), "a", legacy.Property(legacy.Identifier("a"), PropertyKey("foo")))

    parsing("[ a in p WHERE a.foo > 123 | a.foo ]") shouldGive
      legacy.ExtractFunction(filterCommand, "a", legacy.Property(legacy.Identifier("a"), PropertyKey("foo")))
  }

  def convert(astNode: ast.ListComprehension): legacy.Expression = toCommandExpression(astNode)
}
