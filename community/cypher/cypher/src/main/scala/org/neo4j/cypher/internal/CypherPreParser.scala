/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.options.CypherExecutionMode
import org.neo4j.cypher.internal.parser.Base
import org.neo4j.cypher.internal.util.InputPosition
import org.parboiled.scala.Rule0
import org.parboiled.scala.Rule1
import org.parboiled.scala.group

sealed trait PreParserOption
sealed abstract class ModePreParserOption(val value: CypherExecutionMode) extends PreParserOption
case object ExplainModePreParserOption extends ModePreParserOption(CypherExecutionMode.explain)
case object ProfileModePreParserOption extends ModePreParserOption(CypherExecutionMode.profile)
final case class VersionPreParserOption(value: String) extends PreParserOption
final case class KeyValuePreParserOption(key: String, value: String) extends PreParserOption

final case class PreParsedStatement(statement: String, options: List[PreParserOption], offset: InputPosition)

case object CypherPreParser extends org.parboiled.scala.Parser with Base {
  def apply(input: String): PreParsedStatement = parseOrThrow(input, Neo4jCypherExceptionFactory(input, None), None, QueryWithOptions)

  def QueryWithOptions: Rule1[Seq[PreParsedStatement]] =
    WS ~ AllOptions ~ WS ~ AnySomething ~~>>
      ( (options: List[PreParserOption], text: String) => pos => Seq(PreParsedStatement(text, options, pos)))

  def AllOptions: Rule1[List[PreParserOption]] = zeroOrMore(AnyCypherOption, WS) ~~> (_.flatten)

  def AnyCypherOption: Rule1[List[PreParserOption]] = Cypher | ((Explain | Profile) ~~> (m => List(m)))

  def Explain: Rule1[ModePreParserOption] = keyword("EXPLAIN") ~ push(ExplainModePreParserOption)

  def Profile: Rule1[ModePreParserOption] = keyword("PROFILE") ~ push(ProfileModePreParserOption)

  def Cypher: Rule1[List[PreParserOption]] = rule("CYPHER options") {
    keyword("CYPHER") ~~ optional(Version) ~~ KeyValueOptions ~~> ((ver, opts) => ver.toList ++ opts)
  }

  def Version: Rule1[VersionPreParserOption] = rule("Version") {
    group(Digits ~ "." ~ Digits) ~> VersionPreParserOption
  }

  def Digits: Rule0 = oneOrMore("0" - "9")

  def KeyValueOptions: Rule1[List[KeyValuePreParserOption]] = zeroOrMore(KeyValueOption, WS)

  def KeyValueOption: Rule1[KeyValuePreParserOption] = rule("cypher option")(
    (UnescapedSymbolicNameString ~~ "=" ~~ UnescapedSymbolicNameString) ~~> KeyValuePreParserOption
  )

  def AnySomething: Rule1[String] = rule("Query") { oneOrMore(org.parboiled.scala.ANY) ~> identity }
}
