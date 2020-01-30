/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Equals
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.True
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal
import org.parboiled.scala.EOI

class CaseExpressionTest extends ParserTest[internal.expressions.Expression, commands.expressions.Expression] with Expressions {
  implicit val parserToTest = CaseExpression ~ EOI

  test("simple_cases") {
    parsing("CASE 1 WHEN 1 THEN 'ONE' END") shouldGive
      commands.expressions.SimpleCase(commands.expressions.Literal(1), Seq((commands.expressions.Literal(1), commands.expressions.Literal("ONE"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
         END""") shouldGive
      commands.expressions.SimpleCase(commands.expressions.Literal(1), Seq((commands.expressions.Literal(1), commands.expressions.Literal("ONE")), (commands.expressions.Literal(2), commands.expressions.Literal("TWO"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
                  ELSE 'DEFAULT'
         END""") shouldGive
      commands.expressions.SimpleCase(commands.expressions.Literal(1), Seq((commands.expressions.Literal(1), commands.expressions.Literal("ONE")), (commands.expressions.Literal(2), commands.expressions.Literal("TWO"))), Some(commands.expressions.Literal("DEFAULT")))
  }

  test("generic_cases") {
    parsing("CASE WHEN true THEN 'ONE' END") shouldGive
      commands.expressions.GenericCase(IndexedSeq((True(), commands.expressions.Literal("ONE"))), None)

    val alt1 = (Equals(commands.expressions.Literal(1), commands.expressions.Literal(2)), commands.expressions.Literal("ONE"))
    val alt2 = (predicates.Equals(commands.expressions.Literal(2), commands.expressions.Literal("apa")), commands.expressions.Literal("TWO"))

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
         END""") shouldGive
      commands.expressions.GenericCase(IndexedSeq(alt1, alt2), None)

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
                        ELSE 'OTHER'
         END""") shouldGive
      commands.expressions.GenericCase(IndexedSeq(alt1, alt2), Some(commands.expressions.Literal("OTHER")))
  }

  private val converters = new ExpressionConverters(CommunityExpressionConverter(TokenContext.EMPTY))
  def convert(astNode: internal.expressions.Expression): commands.expressions.Expression = converters.toCommandExpression(Id.INVALID_ID, astNode)
}
