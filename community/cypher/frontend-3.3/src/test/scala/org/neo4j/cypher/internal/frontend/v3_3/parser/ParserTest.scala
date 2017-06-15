/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3.parser

import org.neo4j.cypher.internal.frontend.v3_3.InputPosition
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
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
