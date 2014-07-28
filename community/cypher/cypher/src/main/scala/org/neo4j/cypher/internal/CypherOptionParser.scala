/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.v2_1.parser.{BufferPosition, Base, IdentifierStart, IdentifierPart}
import org.parboiled.scala._
import org.parboiled.errors.{InvalidInputError, ParseError}
import org.neo4j.cypher.internal.compiler.v2_1.InvalidInputErrorFormatter
import org.neo4j.cypher.SyntaxException
import org.neo4j.helpers.ThisShouldNotHappenError

final case class CypherQueryWithOptions(query: String, options: Seq[CypherOption] = Seq.empty)

object CypherOptionParser extends Parser with Base {

  def QueryWithOptions: Rule1[CypherQueryWithOptions] =
    AllOptions ~ optional(WS) ~ AnySomething ~~> ( (options: Seq[CypherOption], text: String) => CypherQueryWithOptions(text, options) )

  def AllOptions: Rule1[Seq[CypherOption]] = zeroOrMore(AnyCypherOption, WS)

  def AnyCypherOption: Rule1[CypherOption] = Version | Profile | Explain

  def AnySomething: Rule1[String] = rule("Query") { oneOrMore(org.parboiled.scala.ANY) ~> identity }

  def Version: Rule1[VersionOption] =
    rule("CYPHER") {
      keyword("CYPHER") ~ WS ~ VersionNumber
    }

  def VersionNumber =
    rule("Version") { group(Digits ~ "." ~ Digits ~ optional(VersionName) ) ~> VersionOption }

  def Digits =
    oneOrMore("0" - "9")

  def VersionName: Rule0 =
    IdentifierStart ~ zeroOrMore(IdentifierPart)

  def Profile = keyword("PROFILE") ~ push(ProfileOption)

  def Explain = keyword("EXPLAIN") ~ push(ExplainOption)

  def apply(input: String): CypherQueryWithOptions = {
    val parsingResult = ReportingParseRunner(CypherOptionParser.QueryWithOptions).run(input)

    parsingResult.result match {
      case Some(result) =>
        result

      case _ =>
        val parseErrors: List[ParseError] = parsingResult.parseErrors
        parseErrors.map {
          error =>
            val message = if (error.getErrorMessage != null) {
              error.getErrorMessage
            } else {
              error match {
                case invalidInput: InvalidInputError => new InvalidInputErrorFormatter().format(invalidInput)
                case _ => error.getClass.getSimpleName
              }
            }
            val position = BufferPosition(error.getInputBuffer, error.getStartIndex)
            throw new SyntaxException(s"$message ($position)", input, position.offset)
        }

        throw new ThisShouldNotHappenError("boggle", "Option parsing failed but no parse errors were provided")
    }
  }
}
