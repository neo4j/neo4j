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
package org.neo4j.cypher.internal.parser

import v2_0.Base
import org.scalatest.Assertions
import util.parsing.input.CharSequenceReader


trait ParserTest extends Base with Assertions {

  class ResultCheck[T](val actuals: Seq[T], text: String) {

    def or(other: ResultCheck[T]) = new ResultCheck[T](actuals ++ other.actuals, text)

    def shouldGive(expected: T) {
      actuals foreach {
        actual => assert(actual === expected, s"'$text' was not parsed successfully")
      }
    }
  }

  def parsing[T](s: String)(implicit p: Parser[T]): ResultCheck[T] = convertResult(parsePhrase(p, s), s)

  def partiallyParsing[T](s: String)(implicit p: Parser[T]): ResultCheck[T] = convertResult(parse(p, s), s)

  def assertFails[T](s: String)(implicit p: Parser[T]) {
    parsePhrase(p, s) match {
      case _: NoSuccess =>
      case _            => fail(s"'$s' should not have been parsed correctly")
    }
  }

  private def parsePhrase[T](parser: Parser[T], text: String): ParserTest.this.type#ParseResult[T] = {
    //wrap the parser in the phrase parse to make sure all input is consumed
    val phraseParser = phrase(parser)
    //we need to wrap the string in a reader so our parser can digest it
    val input = new CharSequenceReader(text)
    phraseParser(input)
  }

  private def convertResult[T](r: ParseResult[T], s: String) = r match {
    case Success(t, _)     => new ResultCheck[T](Seq(t), s)
    case NoSuccess(msg, _) => fail(s"Could not parse '$s': $msg")
  }
}
