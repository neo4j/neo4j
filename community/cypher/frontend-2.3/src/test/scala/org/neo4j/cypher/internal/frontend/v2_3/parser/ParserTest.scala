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
package org.neo4j.cypher.internal.frontend.v2_3.parser

import org.neo4j.cypher.internal.frontend.v2_3.InputPosition
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.parboiled.errors.InvalidInputError
import org.parboiled.scala._

trait ParserTest[T, J] extends CypherFunSuite {

  def convert(astNode: T): J

  class ResultCheck(val actuals: Seq[J], text: String) {

    def or(other: ResultCheck) = new ResultCheck(actuals ++ other.actuals, text)

    def shouldGive(expected: J) {
      actuals foreach {
        actual =>
          actual should equal(expected)
      }
    }

    def shouldGive(expected: ((InputPosition) => J)) {
      shouldGive(expected(InputPosition(0,0,0)))
    }

    def shouldMatch(expected: PartialFunction[J, Unit]) {
      actuals foreach {
        actual => expected.isDefinedAt(actual) should equal(true)
      }
    }

    override def toString: String = s"ResultCheck( $text -> $actuals )"
  }

  def parsing(s: String)(implicit p: Rule1[T]): ResultCheck = convertResult(parseRule(p ~ EOI, s), s)

  def partiallyParsing(s: String)(implicit p: Rule1[T]): ResultCheck = convertResult(parseRule(p, s), s)

  def assertFails(s: String)(implicit p: Rule1[T]) {
    parseRule(p, s).result match {
      case None    =>
      case Some(_) => fail(s"'$s' should not have been parsed correctly")
    }
  }

  private def parseRule(rule: Rule1[T], text: String): ParsingResult[T] =
    ReportingParseRunner(rule).run(text)

  private def convertResult(r: ParsingResult[T], input: String) = r.result match {
    case Some(t) => new ResultCheck(Seq(convert(t)), input)
    case None    => fail(s"'$input' failed with: " + r.parseErrors.map {
      case error: InvalidInputError =>
        val position = BufferPosition(error.getInputBuffer, error.getStartIndex)
        val message = new InvalidInputErrorFormatter().format(error)
        s"$message ($position)"
      case error                    =>
        error.getClass.getSimpleName
    }.mkString(","))
  }
}
