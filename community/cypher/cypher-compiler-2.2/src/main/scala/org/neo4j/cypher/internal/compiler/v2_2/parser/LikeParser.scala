/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.parser

import org.parboiled.scala._

//TODO should option be supported? In standard?
/**
 * Parser that parses a Like expression into [[ParsedLike]] which contains tokens of [[LikeOp]].
 *
 * A Like expression can contain the special characters:
 *  - % wildcard
 *  - _ matches exactly one character
 *  - [XY] optionally match any of the characters
 */
case object LikeParser extends Parser {

  def apply(input: String): ParsedLike = ReportingParseRunner(LikeRule).run(input).result match {
    case Some(v) => v
    case None => throw new IllegalArgumentException(s"$input is not valid to use with LIKE")
  }

  /**Base rule*/
  def LikeRule: Rule1[ParsedLike] = rule {zeroOrMore(MatchAllRule | MatchSingleCharRule | SetMatchRule | StringSegmentRule) ~~> ParsedLike ~ EOI}

  def StringSegmentRule: Rule1[LikeOp] = rule {oneOrMore(NormalChar) ~> StringSegment}

  def RawCharacterRule: Rule1[RawCharacter] = rule {NormalChar ~> RawCharacter}

  def MatchAllRule: Rule1[LikeOp] = rule {PercentChar ~ push(MatchAll)}

  def MatchSingleCharRule: Rule1[LikeOp] = rule {UnderscoreChar ~ push(MatchSingleChar)}

  def SetMatchRule: Rule1[LikeOp] = rule {
    "[" ~ zeroOrMore(RawCharacterRule) ~ "]" ~~> SetMatch
  }

  def PercentChar: Rule0 = rule {"%"}

  def UnderscoreChar: Rule0 = rule {"_"}

  def NormalChar: Rule0 = noneOf("%_[]")
}

sealed trait LikeOp

/** Contains a string that needs quoting for use in regular expression*/
case class StringSegment(v: String) extends LikeOp

/** Contains a raw unquoted character*/
case class RawCharacter(v: String) extends LikeOp

/** Matches a %*/
case object MatchAll extends LikeOp

/** Matches a _*/
case object MatchSingleChar extends LikeOp

/** Matches optional pattern, [abcd]*/
case class SetMatch(alternatives: Seq[RawCharacter]) extends LikeOp

/** Contains a sequence of parsed LIKE tokens*/
case class ParsedLike(ops: Seq[LikeOp])
