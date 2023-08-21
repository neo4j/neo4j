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
import org.neo4j.cypher.internal.ast.factory.neo4j.JavaccRule
import org.neo4j.cypher.internal.ast.factory.neo4j.ParserTestBase
import org.neo4j.cypher.internal.cst.factory.neo4j.AntlrRule
import org.neo4j.cypher.internal.cst.factory.neo4j.Cst
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper.literal
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Equals
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Id

// TODO: This should be tested without using the legacy expressions and moved to the semantics module
class ExpressionsTest
    extends ParserTestBase[
      Cst.Expression,
      internal.expressions.Expression,
      commands.expressions.Expression
    ] {

  implicit private val javaccRule: JavaccRule[internal.expressions.Expression] = JavaccRule.Expression
  implicit private val antlrRule: AntlrRule[Cst.Expression] = AntlrRule.Expression

  test("simple_cases") {
    parsing("CASE 1 WHEN 1 THEN 'ONE' END") shouldGive
      commands.expressions.SimpleCase(literal(1), Seq((literal(1), literal("ONE"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
         END"""
    ) shouldGive
      commands.expressions.SimpleCase(literal(1), Seq((literal(1), literal("ONE")), (literal(2), literal("TWO"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
                  ELSE 'DEFAULT'
         END"""
    ) shouldGive
      commands.expressions.SimpleCase(
        literal(1),
        Seq((literal(1), literal("ONE")), (literal(2), literal("TWO"))),
        Some(literal("DEFAULT"))
      )
  }

  test("generic_cases") {
    parsing("CASE WHEN true THEN 'ONE' END") shouldGive
      commands.expressions.GenericCase(IndexedSeq((True(), literal("ONE"))), None)

    val alt1 = (Equals(literal(1), literal(2)), literal("ONE"))
    val alt2 = (predicates.Equals(literal(2), literal("apa")), literal("TWO"))

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
         END"""
    ) shouldGive
      commands.expressions.GenericCase(IndexedSeq(alt1, alt2), None)

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
                        ELSE 'OTHER'
         END"""
    ) shouldGive
      commands.expressions.GenericCase(IndexedSeq(alt1, alt2), Some(literal("OTHER")))
  }

  test("array_indexing") {
    val collection = commands.expressions.ListLiteral(literal(1), literal(2), literal(3), literal(4))

    parsing("[1,2,3,4][1..2]") shouldGive
      commands.expressions.ListSlice(collection, Some(literal(1)), Some(literal(2)))

    parsing("[1,2,3,4][1..2][2..3]") shouldGive
      commands.expressions.ListSlice(
        commands.expressions.ListSlice(collection, Some(literal(1)), Some(literal(2))),
        Some(literal(2)),
        Some(literal(3))
      )

    parsing("collection[1..2]") shouldGive
      commands.expressions.ListSlice(commands.expressions.Variable("collection"), Some(literal(1)), Some(literal(2)))

    parsing("[1,2,3,4][2]") shouldGive
      commands.expressions.ContainerIndex(collection, literal(2))

    parsing("[[1,2]][0][6]") shouldGive
      commands.expressions.ContainerIndex(
        commands.expressions.ContainerIndex(
          commands.expressions.ListLiteral(commands.expressions.ListLiteral(literal(1), literal(2))),
          literal(0)
        ),
        literal(6)
      )

    parsing("collection[1..2][0]") shouldGive
      commands.expressions.ContainerIndex(
        commands.expressions.ListSlice(commands.expressions.Variable("collection"), Some(literal(1)), Some(literal(2))),
        literal(0)
      )

    parsing("collection[..-2]") shouldGive
      commands.expressions.ListSlice(commands.expressions.Variable("collection"), None, Some(literal(-2)))

    parsing("collection[1..]") shouldGive
      commands.expressions.ListSlice(commands.expressions.Variable("collection"), Some(literal(1)), None)
  }

  test("literal_maps") {
    parsing("{ name: 'Andres' }") shouldGive
      commands.expressions.LiteralMap(Map("name" -> literal("Andres")))

    parsing("{ meta : { name: 'Andres' } }") shouldGive
      commands.expressions.LiteralMap(Map("meta" -> commands.expressions.LiteralMap(Map("name" -> literal("Andres")))))

    parsing("{ }") shouldGive
      commands.expressions.LiteralMap(Map())
  }

  test("better_map_support") {
    parsing("map.key1.key2.key3") shouldGive
      commands.expressions.Property(
        commands.expressions.Property(
          commands.expressions.Property(commands.expressions.Variable("map"), PropertyKey("key1")),
          PropertyKey("key2")
        ),
        PropertyKey("key3")
      )

    parsing("({ key: 'value' }).key") shouldGive
      commands.expressions.Property(commands.expressions.LiteralMap(Map("key" -> literal("value"))), PropertyKey("key"))

    parsing("({ inner1: { inner2: 'Value' } }).key") shouldGive
      commands.expressions.Property(
        commands.expressions.LiteralMap(
          Map("inner1" -> commands.expressions.LiteralMap(Map("inner2" -> literal("Value"))))
        ),
        PropertyKey("key")
      )

  }

  private val converters =
    new ExpressionConverters(CommunityExpressionConverter(
      ReadTokenContext.EMPTY,
      new AnonymousVariableNameGenerator(),
      CypherRuntimeConfiguration.defaultConfiguration
    ))

  def convert(astNode: internal.expressions.Expression): commands.expressions.Expression =
    converters.toCommandExpression(Id.INVALID_ID, astNode)
}
