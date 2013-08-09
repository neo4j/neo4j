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
package org.neo4j.cypher.internal.parser.legacy

import org.neo4j.cypher.internal.commands._
import expressions._
import org.junit.Test

class ExpressionsTest extends Expressions with MatchClause with ParserTest {

  @Test def simple_cases() {
    implicit val parserToTest = simpleCase

    parsing("CASE 1 WHEN 1 THEN 'ONE' END") shouldGive
      SimpleCase(Literal(1), Seq((Literal(1), Literal("ONE"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
         END""") shouldGive
      SimpleCase(Literal(1), Seq((Literal(1), Literal("ONE")), (Literal(2), Literal("TWO"))), None)

    parsing(
      """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
                  ELSE 'DEFAULT'
         END""") shouldGive
      SimpleCase(Literal(1), Seq((Literal(1), Literal("ONE")), (Literal(2), Literal("TWO"))), Some(Literal("DEFAULT")))
  }

  @Test def generic_cases() {
    implicit val parserToTest = genericCase

    parsing("CASE WHEN true THEN 'ONE' END") shouldGive
      GenericCase(Seq((True(), Literal("ONE"))), None)

    val alt1 = (Equals(Literal(1), Literal(2)), Literal("ONE"))
    val alt2 = (Equals(Literal(2), Literal("apa")), Literal("TWO"))

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
         END""") shouldGive
      GenericCase(Seq(alt1, alt2), None)

    parsing(
      """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
                        ELSE 'OTHER'
         END""") shouldGive
      GenericCase(Seq(alt1, alt2), Some(Literal("OTHER")))
  }

  @Test def literal_maps() {
    implicit val parserToTest = literalMapExpression

    parsing("{ name: 'Andres' }") shouldGive
      LiteralMap(Map("name" -> Literal("Andres")))

    parsing("{ } ") shouldGive
      LiteralMap(Map())

    parsing("{ meta : { name: 'Andres' } } ") shouldGive
      LiteralMap(Map("meta" -> LiteralMap(Map("name" -> Literal("Andres")))))
  }
}
