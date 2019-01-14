/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.frontend.v3_4.parser.Base
import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.parboiled.scala._

final case class PreParsedStatement(statement: String, options: Seq[PreParserOption], offset: InputPosition)

case object CypherPreParser extends Parser with Base {
  def apply(input: String): PreParsedStatement = parseOrThrow(input, None, QueryWithOptions)

  def QueryWithOptions: Rule1[Seq[PreParsedStatement]] =
    WS ~ AllOptions ~ WS ~ AnySomething ~~>>
      ( (options: Seq[PreParserOption], text: String) => pos => Seq(PreParsedStatement(text, options, pos)))

  def AllOptions: Rule1[Seq[PreParserOption]] = zeroOrMore(AnyCypherOption, WS)

  def AnyCypherOption: Rule1[PreParserOption] = Cypher | Explain | Profile

  def AnySomething: Rule1[String] = rule("Query") { oneOrMore(org.parboiled.scala.ANY) ~> identity }

  def Cypher: Rule1[ConfigurationOptions] = rule("CYPHER options") {
    keyword("CYPHER") ~~
      optional(VersionNumber) ~~
      zeroOrMore(PlannerOption | RuntimeOption | StrategyOption | DebugFlag, WS) ~~> ConfigurationOptions
  }

  def PlannerOption: Rule1[PreParserOption] = rule("planner option") (
      option("planner", "cost") ~ push(CostPlannerOption)
    | option("planner", "rule") ~ push(RulePlannerOption)
    | option("planner", "greedy") ~ push(GreedyPlannerOption)
    | option("planner", "idp") ~ push(IDPPlannerOption)
    | option("planner", "dp") ~ push(DPPlannerOption)
  )

  def RuntimeOption: Rule1[RuntimePreParserOption] = rule("runtime option")(
    option("runtime", "interpreted") ~ push(InterpretedRuntimeOption)
      | option("runtime", "compiled") ~ push(CompiledRuntimeOption)
      | option("runtime", "slotted") ~ push(SlottedRuntimeOption)
      | option("runtime", "morsel") ~ push(MorselRuntimeOption)
  )

  def StrategyOption: Rule1[UpdateStrategyOption] = rule("strategy option")(
    option("updateStrategy", "eager") ~ push(EagerOption)
  )

  def VersionNumber: Rule1[VersionOption] = rule("Version") {
    group(Digits ~ "." ~ Digits) ~> VersionOption
  }

  def DebugFlag: Rule1[DebugOption] = rule("debug option") {
    keyword("debug") ~~ "=" ~~ SymbolicNameString ~~> DebugOption
  }

  def Digits: Rule0 = oneOrMore("0" - "9")

  def Profile: Rule1[ExecutionModePreParserOption] = keyword("PROFILE") ~ push(ProfileOption)

  def Explain: Rule1[ExecutionModePreParserOption] = keyword("EXPLAIN") ~ push(ExplainOption)

  def option(key: String, value: String): Rule0 = {
    keyword(key) ~~ "=" ~~ keyword(value)
  }
}
