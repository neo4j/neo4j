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
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.NotAntlr
import org.neo4j.cypher.internal.expressions.CaseExpression
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
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Id

class CaseExpressionTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("simple_cases") {
    "CASE 1 WHEN 1 THEN 'ONE' END" should parse[CaseExpression](NotAntlr).withAstLike(convertsTo(
      commands.expressions.SimpleCase(lit(1), Seq((lit(1), lit("ONE"))), None)
    ))

    """CASE 1
         WHEN 1 THEN 'ONE'
         WHEN 2 THEN 'TWO'
       END""" should parse[CaseExpression](NotAntlr).withAstLike(convertsTo(
      commands.expressions.SimpleCase(lit(1), Seq((lit(1), lit("ONE")), (lit(2), lit("TWO"))), None)
    ))

    """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
         END""" should parse[CaseExpression](NotAntlr).withAstLike(convertsTo(
      commands.expressions.SimpleCase(lit(1), Seq((lit(1), lit("ONE")), (lit(2), lit("TWO"))), None)
    ))
    """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
         END""" should parse[CaseExpression](NotAntlr).withAstLike(convertsTo(
      commands.expressions.SimpleCase(lit(1), Seq((lit(1), lit("ONE")), (lit(2), lit("TWO"))), None)
    ))

    """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
                  ELSE 'DEFAULT'
         END""" should parse[CaseExpression](NotAntlr).withAstLike(convertsTo(
      commands.expressions.SimpleCase(lit(1), Seq((lit(1), lit("ONE")), (lit(2), lit("TWO"))), Some(lit("DEFAULT")))
    ))
  }

  test("generic_cases") {
    "CASE WHEN true THEN 'ONE' END" should parse[CaseExpression](NotAntlr).withAstLike(convertsTo(
      commands.expressions.GenericCase(IndexedSeq((True(), lit("ONE"))), None)
    ))

    val alt1 = (Equals(lit(1), lit(2)), lit("ONE"))
    val alt2 = (predicates.Equals(lit(2), lit("apa")), lit("TWO"))

    """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
         END""" should parse[CaseExpression](NotAntlr).withAstLike(convertsTo(
      commands.expressions.GenericCase(IndexedSeq(alt1, alt2), None)
    ))

    """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
                        ELSE 'OTHER'
         END""" should parse[CaseExpression](NotAntlr).withAstLike(convertsTo(
      commands.expressions.GenericCase(IndexedSeq(alt1, alt2), Some(lit("OTHER")))
    ))
  }

  private val converters =
    new ExpressionConverters(CommunityExpressionConverter(
      ReadTokenContext.EMPTY,
      new AnonymousVariableNameGenerator(),
      new SelectivityTrackerRegistrator(),
      CypherRuntimeConfiguration.defaultConfiguration
    ))

  private def lit(o: Any) = LiteralHelper.literal(o)

  private def convertsTo(expected: commands.expressions.Expression)(astNode: internal.expressions.Expression) =
    convert(astNode) shouldBe expected

  private def convert(astNode: internal.expressions.Expression): commands.expressions.Expression =
    converters.toCommandExpression(Id.INVALID_ID, astNode)
}
