/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.v3_4.parser

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

