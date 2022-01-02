/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait JavaccParserTestBase[T, J] extends CypherFunSuite {

  type Extra

  def convert(astNode: T): J

  def convert(astNode: T, extra: Extra): J = convert(astNode)

  case class ResultCheck(actuals: Seq[J], text: String) {

    def or(other: ResultCheck): ResultCheck = copy(actuals = actuals ++ other.actuals)

    def shouldGive(expected: J): Unit = {
      actuals foreach {
        actual =>
          actual should equal(expected)
      }
    }

    def shouldGive(expected: InputPosition => J): Unit = {
      shouldGive(expected(InputPosition(0,0,0)))
    }

    def shouldMatch(expected: PartialFunction[J, Unit]): Unit = {
      actuals foreach {
        actual => expected.isDefinedAt(actual) should equal(true)
      }
    }

    def shouldVerify(expected: J => Unit): Unit = {
      actuals foreach expected
    }

    override def toString: String = s"ResultCheck( $text -> $actuals )"
  }

  def parsing(s: String)(implicit p: JavaccRule[T]): ResultCheck = convertResult(parseRule(p, s), None, s)

  def parsingWith(s: String, extra: Extra)(implicit p: JavaccRule[T]): ResultCheck = convertResult(parseRule(p, s), Some(extra), s)

  def partiallyParsing(s: String)(implicit p: JavaccRule[T]): ResultCheck = convertResult(parseRule(p, s), None, s)

  def assertFails(s: String)(implicit p: JavaccRule[T]): Unit = {
    parseRule(p, s).toOption match {
      case None        =>
      case Some(thing) => fail(s"'$s' should not have been parsed correctly, parsed as $thing")
    }
  }

  private def parseRule(rule: JavaccRule[T], queryText: String): Try[T] = Try(rule(queryText))

  private def convertResult(r: Try[T], extra: Option[Extra], input: String) = r match {
    case Success(t) =>
      val converted = extra match {
        case None => convert(t)
        case Some(e) => convert(t, e)
      }
      ResultCheck(Seq(converted), input)

    case Failure(exception) => fail(s"'$input' failed with: $exception")
  }
}
