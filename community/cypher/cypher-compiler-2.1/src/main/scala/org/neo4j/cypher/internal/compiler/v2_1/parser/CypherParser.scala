/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.parser

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.parboiled.errors.{InvalidInputError, ParseError}
import org.parboiled.scala._

case class CypherParser(monitor: ParserMonitor[ast.Statement]) extends Parser with Statement with Expressions {

  @throws(classOf[SyntaxException])
  def parse(queryText: String): ast.Statement = {
    monitor.startParsing(queryText)
    val parsingResults = ReportingParseRunner(CypherParser.Statements).run(queryText)
    parsingResults.result match {
      case Some(statements) =>
        if (statements.size == 1) {
          val statement = statements.head
          monitor.finishParsingSuccess(queryText, statement)
          statement
        } else {
          monitor.finishParsingError(queryText, Seq.empty)
          throw new SyntaxException(s"Expected exactly one statement per query but got: ${statements.size}")
        }
      case _ => {
        val parseErrors: List[ParseError] = parsingResults.parseErrors
        monitor.finishParsingError(queryText, parseErrors)
        parseErrors.map { error =>
          val message = if (error.getErrorMessage != null) {
            error.getErrorMessage
          } else {
            error match {
              case invalidInput: InvalidInputError => new InvalidInputErrorFormatter().format(invalidInput)
              case _ => error.getClass.getSimpleName
            }
          }
          val position = BufferPosition(error.getInputBuffer, error.getStartIndex)
          throw new SyntaxException(s"$message ($position)", queryText, position.offset)
        }

        throw new ThisShouldNotHappenError("cleishm", "Parsing failed but no parse errors were provided")
      }
    }
  }
}

object CypherParser extends Parser with Statement with Expressions {
  val Statements: Rule1[Seq[ast.Statement]] = rule {
    oneOrMore(WS ~ Statement ~ WS, separator = ch(';')) ~~ optional(ch(';')) ~~ EOI.label("end of input")
  }
}

trait ParserMonitor[T] {
  def startParsing(query: String)
  def finishParsingSuccess(query: String, result: T)
  def finishParsingError(query:String, errors: Seq[ParseError])
}
