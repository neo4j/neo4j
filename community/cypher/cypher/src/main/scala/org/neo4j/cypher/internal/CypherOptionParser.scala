/*
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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.compiler.v2_2.InputPosition
import org.neo4j.cypher.internal.compiler.v2_2.parser._
import org.parboiled.scala._

final case class CypherQueryWithOptions(statement: String, options: Seq[CypherOption], offset: InputPosition)

case class CypherOptionParser(monitor: ParserMonitor[CypherQueryWithOptions]) extends Parser with Base {
  def apply(input: String): CypherQueryWithOptions = parseOrThrow(input, None, QueryWithOptions, Some(monitor))

  def QueryWithOptions: Rule1[Seq[CypherQueryWithOptions]] =
    WS ~ AllOptions ~ WS ~ AnySomething ~~>>
      ( (options: Seq[CypherOption], text: String) => pos => Seq(CypherQueryWithOptions(text, options, pos)))

  def AllOptions: Rule1[Seq[CypherOption]] = zeroOrMore(AnyCypherOption, WS)

  def AnyCypherOption: Rule1[CypherOption] = Cypher | Explain | Profile | PlannerDeprecated

  def AnySomething: Rule1[String] = rule("Query") { oneOrMore(org.parboiled.scala.ANY) ~> identity }

  def Cypher = rule("CYPHER options") {
    keyword("CYPHER") ~~
      optional(VersionNumber) ~~
      zeroOrMore(PlannerOption) ~~> ConfigurationOptions
  }

  def PlannerOption: Rule1[CypherOption] = rule("planner option") (
    option("planner", "cost") ~ push(CostPlannerOption)
  | option("planner", "rule") ~ push(RulePlannerOption)
  | option("planner", "idp") ~ push(IDPPlannerOption)
  | option("planner", "dp") ~ push(DPPlannerOption)
  )

  @deprecated
  def PlannerDeprecated = rule("PLANNER") (
    keyword("PLANNER COST") ~ push(CostPlannerOption)
      | keyword("PLANNER IDP") ~ push(IDPPlannerOption)
      | keyword("PLANNER DP") ~ push(DPPlannerOption)
      | keyword("PLANNER RULE") ~ push(RulePlannerOption)
  )

  def VersionNumber = rule("Version") {
    group(Digits ~ "." ~ Digits) ~> VersionOption
  }

  def Digits = oneOrMore("0" - "9")

  def Profile = keyword("PROFILE") ~ push(ProfileOption)

  def Explain = keyword("EXPLAIN") ~ push(ExplainOption)

  def option(key: String, value: String): Rule0 = {
    keyword(key) ~ WS ~ "=" ~ WS ~keyword(value)
  }
}
