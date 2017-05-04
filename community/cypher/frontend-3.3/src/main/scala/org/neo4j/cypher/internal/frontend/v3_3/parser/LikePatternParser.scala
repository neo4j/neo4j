/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_3.parser

import org.parboiled.scala._
import org.parboiled.scala.parserunners.ReportingParseRunner

/**
 * Parser that parses a Like pattern into [[ParsedLikePattern]] which contains tokens of [[LikePatternOp]].
 *
 * A Like pattern can contain the special characters:
 *
 *  - % wildcard
 *  - _ matches exactly one character
 *
 */
case object LikePatternParser extends Parser {

  def apply(input: String): ParsedLikePattern =
    ReportingParseRunner(LikeRule).run(input).result.getOrElse(throw new IllegalArgumentException(s"$input is not valid to use with LIKE")).compact

  /** Base rule */
  def LikeRule: Rule1[ParsedLikePattern] = rule { zeroOrMore(MatchManyRule | MatchSingleRule | MatchTextRule | MatchEscapedCharRule) ~~> ParsedLikePattern ~ EOI }

  def MatchManyRule: Rule1[LikePatternOp] = rule { "%" ~ push(MatchMany) }

  def MatchSingleRule: Rule1[LikePatternOp] = rule { "_" ~ push(MatchSingle) }

  def MatchTextRule: Rule1[LikePatternOp]= oneOrMore(noneOf("%_\\")) ~> MatchText

  def MatchEscapedCharRule: Rule1[LikePatternOp] =  rule { "\\" ~ (ANY ~> MatchText) }
}

/** Contains a sequence of parsed LIKE tokens*/
case class ParsedLikePattern(ops: List[LikePatternOp]) {
 def compact: ParsedLikePattern = {
   val newOps = ops.foldLeft(List.empty[LikePatternOp]) {
     case (MatchText(fst) :: tl, MatchText(snd)) =>
       MatchText(fst ++ snd) :: tl

     case (acc, op) =>
       op :: acc
   }
   ParsedLikePattern(newOps.reverse)
 }
 override def toString = ops.mkString("\"","","\"")
}

sealed trait LikePatternOp

sealed trait WildcardLikePatternOp extends LikePatternOp

/** Contains a string that needs quoting for use in regular expression */
case class MatchText(text: String) extends LikePatternOp  {
  override def toString = text
}

/** Matches a % */
case object MatchMany extends WildcardLikePatternOp {
  override def toString = "%"
}

/** Matches a _*/
case object MatchSingle extends WildcardLikePatternOp {
  override def toString = "_"
}

