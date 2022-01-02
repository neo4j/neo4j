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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaccParserTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaccRule
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Id

class MapLiteralTest extends JavaccParserTestBase[internal.expressions.Expression, commands.expressions.Expression] {
  implicit val parserToTest: JavaccRule[internal.expressions.Expression] = JavaccRule.fromParser(_.MapLiteral())

  test("literal_maps") {
    parsing("{ name: 'Andres' }") shouldGive
      commands.expressions.LiteralMap(Map("name" -> literal("Andres")))

    parsing("{ meta : { name: 'Andres' } }") shouldGive
      commands.expressions.LiteralMap(Map("meta" -> commands.expressions.LiteralMap(Map("name" -> literal("Andres")))))

    parsing("{ }") shouldGive
      commands.expressions.LiteralMap(Map())
  }

  test("nested_map_support") {
    parsing("{ key: 'value' }") shouldGive
      commands.expressions.LiteralMap(Map("key" -> literal("value")))

    parsing("{ inner1: { inner2: 'Value' } }") shouldGive
      commands.expressions.LiteralMap(Map("inner1" -> commands.expressions.LiteralMap(Map("inner2" -> literal("Value")))))
  }

  private val converters = new ExpressionConverters(CommunityExpressionConverter(ReadTokenContext.EMPTY, new AnonymousVariableNameGenerator()))
  def convert(astNode: internal.expressions.Expression): commands.expressions.Expression = converters.toCommandExpression(Id.INVALID_ID, astNode)
}
