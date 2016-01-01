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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.v2_1.parser._
import org.parboiled.scala._

final case class CypherQueryWithOptions(statement: String, options: Seq[CypherOption] = Seq.empty)

case class CypherOptionParser(monitor: ParserMonitor[CypherQueryWithOptions]) extends Parser with Base {
  def apply(input: String): CypherQueryWithOptions = parseOrThrow(input, QueryWithOptions, Some(monitor))

  def QueryWithOptions: Rule1[CypherQueryWithOptions] =
    AllOptions ~ optional(WS) ~ AnySomething ~~> ( (options: Seq[CypherOption], text: String) => CypherQueryWithOptions(text, options) )

  def AllOptions: Rule1[Seq[CypherOption]] = zeroOrMore(AnyCypherOption, WS)

  def AnyCypherOption: Rule1[CypherOption] = Version //TODO: Enable in 2.2  | Profile | Explain

  def AnySomething: Rule1[String] = rule("Query") { oneOrMore(org.parboiled.scala.ANY) ~> identity }

  def Version: Rule1[VersionOption] =
    rule("CYPHER") {
      keyword("CYPHER") ~ WS ~ VersionNumber
    }

  def VersionNumber =
    rule("Version") { group(Digits ~ "." ~ Digits ~ optional("." ~ VersionName) ) ~> VersionOption }

  def Digits =
    oneOrMore("0" - "9")

  def VersionName: Rule0 =
    IdentifierStart ~ zeroOrMore(IdentifierPart)

  def Profile = keyword("PROFILE") ~ push(ProfileOption)

  def Explain = keyword("EXPLAIN") ~ push(ExplainOption)
}
