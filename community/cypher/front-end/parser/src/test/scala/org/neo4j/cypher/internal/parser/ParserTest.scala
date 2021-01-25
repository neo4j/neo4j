/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.parboiled.errors.InvalidInputError
import org.parboiled.errors.ParserRuntimeException
import org.parboiled.scala.EOI
import org.parboiled.scala.ParsingResult
import org.parboiled.scala.ReportingParseRunner
import org.parboiled.scala.Rule1

trait ParserTest[T, J] extends CypherFunSuite {

  type Extra

  def convert(astNode: T): J

  def convert(astNode: T, extra: Extra): J = convert(astNode)

  class ResultCheck(val actuals: Seq[J], text: String) {

    def or(other: ResultCheck) = new ResultCheck(actuals ++ other.actuals, text)

    def shouldGive(expected: J) {
      actuals foreach {
        actual =>
          actual should equal(expected)
      }
    }

    def shouldGive(expected: InputPosition => J) {
      shouldGive(expected(InputPosition(0,0,0)))
    }

    def shouldMatch(expected: PartialFunction[J, Unit]) {
      actuals foreach {
        actual => expected.isDefinedAt(actual) should equal(true)
      }
    }

    def shouldVerify(expected: J => Unit): Unit = {
      actuals foreach expected
    }

    override def toString: String = s"ResultCheck( $text -> $actuals )"
  }

  def parsing(s: String)(implicit p: Rule1[T]): ResultCheck = convertResult(parseRule(p ~ EOI, s), None, s)

  def parsingWith(s: String, extra: Extra)(implicit p: Rule1[T]): ResultCheck = convertResult(parseRule(p ~ EOI, s), Some(extra), s)

  def partiallyParsing(s: String)(implicit p: Rule1[T]): ResultCheck = convertResult(parseRule(p, s), None, s)

  def assertFails(s: String)(implicit p: Rule1[T]) {
    try {
      parseRule(p ~ EOI, s).result match {
        case None    =>
        case Some(thing) => fail(s"'$s' should not have been parsed correctly, parsed as $thing")
      }
    } catch {
      case _: ParserRuntimeException => // If encountered a ParserRuntimeException, it means the parsing failed
    }
  }

  private def parseRule(rule: Rule1[T], text: String): ParsingResult[T] =
    ReportingParseRunner(rule).run(text)

  private def convertResult(r: ParsingResult[T], extra: Option[Extra], input: String) = r.result match {
    case Some(t) =>
      val converted = extra match {
        case None    => convert(t)
        case Some(e) => convert(t, e)
      }
      new ResultCheck(Seq(converted), input)
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
