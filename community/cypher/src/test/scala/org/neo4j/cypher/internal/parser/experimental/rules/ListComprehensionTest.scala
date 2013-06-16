/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.experimental.rules

import org.junit.Test
import org.parboiled.scala._
import org.neo4j.cypher.internal.parser.ParserExperimentalTest
import org.neo4j.cypher.internal.parser.experimental.ast
import org.neo4j.cypher.internal.commands
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions}


class ListComprehensionTest extends ParserExperimentalTest[ast.ListComprehension, commandexpressions.Expression] with Expressions {

  @Test def tests() {
    implicit val parserToTest = ListComprehension ~ EOI

    val filterCommand = commandexpressions.FilterFunction(
      commandexpressions.Identifier("p"),
      "a",
      commands.GreaterThan(commandexpressions.Property(commandexpressions.Identifier("a"), "foo"), commandexpressions.Literal(123)))

    parsing("[ a in p WHERE a.foo > 123 ]") shouldGive filterCommand

    parsing("[ a in p | a.foo ]") shouldGive
      commandexpressions.ExtractFunction(commandexpressions.Identifier("p"), "a", commandexpressions.Property(commandexpressions.Identifier("a"), "foo"))

    parsing("[ a in p WHERE a.foo > 123 | a.foo ]") shouldGive
      commandexpressions.ExtractFunction(filterCommand, "a", commandexpressions.Property(commandexpressions.Identifier("a"), "foo"))
  }

  def convert(astNode: ast.ListComprehension): commandexpressions.Expression = astNode.toCommand
}
