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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.NotAntlr
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.SelectivityTrackerRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.LiteralHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Equals
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Id

// TODO: This should be tested without using the legacy expressions and moved to the semantics module
class ExpressionsTest extends AstParsingTestBase {

  test("simple_cases") {
    assertCommand(
      "CASE 1 WHEN 1 THEN 'ONE' END",
      commands.expressions.CaseExpression(IndexedSeq((Equals(lit(1), lit(1)), lit("ONE"))), None)
    )

    assertCommand(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
         END""",
      commands.expressions.CaseExpression(
        IndexedSeq(
          (Equals(lit(1), lit(1)), lit("ONE")),
          (Equals(lit(1), lit(2)), lit("TWO"))
        ),
        None
      )
    )
    assertCommand(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
                  ELSE 'DEFAULT'
         END""",
      commands.expressions.CaseExpression(
        IndexedSeq(
          (Equals(lit(1), lit(1)), lit("ONE")),
          (Equals(lit(1), lit(2)), lit("TWO"))
        ),
        Some(lit("DEFAULT"))
      )
    )
  }

  test("generic_cases") {
    assertCommand(
      "CASE WHEN true THEN 'ONE' END",
      commands.expressions.CaseExpression(IndexedSeq((True(), lit("ONE"))), None)
    )

    val alt1 = (Equals(lit(1), lit(2)), lit("ONE"))
    val alt2 = (predicates.Equals(lit(2), lit("apa")), lit("TWO"))

    assertCommand(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
         END""",
      commands.expressions.CaseExpression(IndexedSeq(alt1, alt2), None)
    )
    assertCommand(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
                        ELSE 'OTHER'
         END""",
      commands.expressions.CaseExpression(IndexedSeq(alt1, alt2), Some(lit("OTHER")))
    )
  }

  test("array_indexing") {
    val collection = commands.expressions.ListLiteral(lit(1), lit(2), lit(3), lit(4))

    assertCommand("[1,2,3,4][1..2]", commands.expressions.ListSlice(collection, Some(lit(1)), Some(lit(2))))

    assertCommand(
      "[1,2,3,4][1..2][2..3]",
      commands.expressions.ListSlice(
        commands.expressions.ListSlice(collection, Some(lit(1)), Some(lit(2))),
        Some(lit(2)),
        Some(lit(3))
      )
    )

    assertCommand(
      "collection[1..2]",
      commands.expressions.ListSlice(commands.expressions.Variable("collection"), Some(lit(1)), Some(lit(2)))
    )
    assertCommand("[1,2,3,4][2]", commands.expressions.ContainerIndex(collection, lit(2)))
    assertCommand(
      "[[1,2]][0][6]",
      commands.expressions.ContainerIndex(
        commands.expressions.ContainerIndex(
          commands.expressions.ListLiteral(commands.expressions.ListLiteral(lit(1), lit(2))),
          lit(0)
        ),
        lit(6)
      )
    )
    assertCommand(
      "collection[1..2][0]",
      commands.expressions.ContainerIndex(
        commands.expressions.ListSlice(commands.expressions.Variable("collection"), Some(lit(1)), Some(lit(2))),
        lit(0)
      )
    )
    assertCommand(
      "collection[..-2]",
      commands.expressions.ListSlice(commands.expressions.Variable("collection"), None, Some(lit(-2)))
    )
    assertCommand(
      "collection[1..]",
      commands.expressions.ListSlice(commands.expressions.Variable("collection"), Some(lit(1)), None)
    )
  }

  test("better_map_support") {
    assertCommand(
      "map.key1.key2.key3",
      commands.expressions.Property(
        commands.expressions.Property(
          commands.expressions.Property(commands.expressions.Variable("map"), PropertyKey("key1")),
          PropertyKey("key2")
        ),
        PropertyKey("key3")
      )
    )

    assertCommand(
      "({ key: 'value' }).key",
      commands.expressions.Property(commands.expressions.LiteralMap(Map("key" -> lit("value"))), PropertyKey("key"))
    )
    assertCommand(
      "({ inner1: { inner2: 'Value' } }).key",
      commands.expressions.Property(
        commands.expressions.LiteralMap(
          Map("inner1" -> commands.expressions.LiteralMap(Map("inner2" -> lit("Value"))))
        ),
        PropertyKey("key")
      )
    )

  }

  private def assertCommand(cypher: String, expected: commands.expressions.Expression): Unit = {
    cypher should parse[Expression](NotAntlr).withAstLike(e => convert(e) shouldBe expected)
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
