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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{CommunityExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.{Equals, True}
import org.neo4j.cypher.internal.runtime.interpreted.commands.{predicates, expressions => legacy}
import org.neo4j.cypher.internal.v3_4.{expressions => ast}
import org.neo4j.cypher.internal.frontend.v3_4.parser.{Expressions, ParserTest}
import org.parboiled.scala._

class CaseExpressionTest extends ParserTest[ast.Expression, legacy.Expression] with Expressions {
  implicit val parserToTest = CaseExpression ~ EOI

  test("simple_cases") {
    parsing("CASE 1 WHEN 1 THEN 'ONE' END") shouldGive
      legacy.SimpleCase(legacy.Literal(1), Seq((legacy.Literal(1), legacy.Literal("ONE"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
         END""") shouldGive
      legacy.SimpleCase(legacy.Literal(1), Seq((legacy.Literal(1), legacy.Literal("ONE")), (legacy.Literal(2), legacy.Literal("TWO"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
                  ELSE 'DEFAULT'
         END""") shouldGive
      legacy.SimpleCase(legacy.Literal(1), Seq((legacy.Literal(1), legacy.Literal("ONE")), (legacy.Literal(2), legacy.Literal("TWO"))), Some(legacy.Literal("DEFAULT")))
  }

  test("generic_cases") {
    parsing("CASE WHEN true THEN 'ONE' END") shouldGive
      legacy.GenericCase(IndexedSeq((True(), legacy.Literal("ONE"))), None)

    val alt1 = (Equals(legacy.Literal(1), legacy.Literal(2)), legacy.Literal("ONE"))
    val alt2 = (predicates.Equals(legacy.Literal(2), legacy.Literal("apa")), legacy.Literal("TWO"))

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
         END""") shouldGive
      legacy.GenericCase(IndexedSeq(alt1, alt2), None)

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
                        ELSE 'OTHER'
         END""") shouldGive
      legacy.GenericCase(IndexedSeq(alt1, alt2), Some(legacy.Literal("OTHER")))
  }

  private val converters = new ExpressionConverters(CommunityExpressionConverter)
  def convert(astNode: ast.Expression): legacy.Expression = converters.toCommandExpression(astNode)
}
