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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.SelectivityTrackerRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Id

class MapLiteralTest extends AstParsingTestBase {

  test("literal_maps") {
    assertCommand("{ name: 'Andres' }", commands.expressions.LiteralMap(Map("name" -> lit("Andres"))))

    assertCommand(
      "{ meta : { name: 'Andres' } }",
      commands.expressions.LiteralMap(Map("meta" -> commands.expressions.LiteralMap(Map("name" -> lit("Andres")))))
    )

    assertCommand("{ }", commands.expressions.LiteralMap(Map()))
  }

  test("nested_map_support") {
    assertCommand("{ key: 'value' }", commands.expressions.LiteralMap(Map("key" -> lit("value"))))

    assertCommand(
      "{ inner1: { inner2: 'Value' } }",
      commands.expressions.LiteralMap(
        Map("inner1" -> commands.expressions.LiteralMap(Map("inner2" -> lit("Value"))))
      )
    )
  }

  private def assertCommand(cypher: String, expected: commands.expressions.Expression): Unit = {
    cypher should parse[Expression].withAstLike(e => convert(e) shouldBe expected)
  }

  private def lit(o: Any) = LiteralHelper.literal(o)

  private val converters =
    new ExpressionConverters(CommunityExpressionConverter(
      ReadTokenContext.EMPTY,
      new AnonymousVariableNameGenerator(),
      new SelectivityTrackerRegistrator(),
      CypherRuntimeConfiguration.defaultConfiguration
    ))

  private def convert(astNode: internal.expressions.Expression): commands.expressions.Expression =
    converters.toCommandExpression(Id.INVALID_ID, astNode)
}
