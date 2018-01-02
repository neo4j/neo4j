/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.frontend.v2_3.InputPosition
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class CypherPreParserTest extends CypherFunSuite with TableDrivenPropertyChecks {

  val queries = Table(
    ("query", "expected"),
    ("CYPHER 1.9 MATCH", PreParsedStatement("MATCH", Seq(ConfigurationOptions(Some(VersionOption("1.9")), Seq.empty)), (1, 12, 11))),
    ("CYPHER 2.0 THAT", PreParsedStatement("THAT", Seq(ConfigurationOptions(Some(VersionOption("2.0")), Seq.empty)), (1, 12, 11))),
    ("CYPHER 2.1 YO", PreParsedStatement("YO", Seq(ConfigurationOptions(Some(VersionOption("2.1")), Seq.empty)), (1, 12, 11))),
    ("CYPHER 2.2 PRO", PreParsedStatement("PRO", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty)), (1, 12, 11))),
    ("PROFILE THINGS", PreParsedStatement("THINGS", Seq(ProfileOption), (1, 9, 8))),
    ("EXPLAIN THIS", PreParsedStatement("THIS", Seq(ExplainOption), (1, 9, 8))),
    ("CYPHER 2.2 PLANNER COST PROFILE PATTERN", PreParsedStatement("PATTERN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty), CostPlannerOption, ProfileOption), (1, 33, 32))),
    ("EXPLAIN CYPHER 2.1 YALL", PreParsedStatement("YALL", Seq(ExplainOption, ConfigurationOptions(Some(VersionOption("2.1")), Seq.empty)), (1, 20, 19))),
    ("CYPHER 2.2 PLANNER COST RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty), CostPlannerOption), (1, 25, 24))),
    ("PLANNER COST RETURN", PreParsedStatement("RETURN", Seq(CostPlannerOption), (1, 14, 13))),
    ("CYPHER 2.2 PLANNER RULE RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty), RulePlannerOption), (1, 25, 24))),
    ("PLANNER RULE RETURN", PreParsedStatement("RETURN", Seq(RulePlannerOption), (1, 14, 13))),
    ("CYPHER 2.2 PLANNER IDP RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty), IDPPlannerOption), (1, 24, 23))),
    ("CYPHER 2.2 PLANNER DP RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty), DPPlannerOption), (1, 23, 22))),
    ("PLANNER IDP RETURN", PreParsedStatement("RETURN", Seq(IDPPlannerOption), (1, 13, 12))),
    ("PLANNER DP RETURN", PreParsedStatement("RETURN", Seq(DPPlannerOption), (1, 12, 11))),
    ("CYPHER planner=cost RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions(None, Seq(CostPlannerOption))), (1, 21, 20))),
    ("CYPHER 2.2 planner=cost RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq(CostPlannerOption))), (1, 25, 24))),
    ("CYPHER 2.2 planner = idp RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq(IDPPlannerOption))), (1, 26, 25))),
    ("CYPHER planner =dp RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions(None, Seq(
      DPPlannerOption))), (1, 20, 19))),

    ("CYPHER runtime=interpreted RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions(None, Seq(InterpretedRuntimeOption))), (1, 28, 27))),
    ("CYPHER runtime=compiled RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions(None, Seq(InterpretedRuntimeOption))), (1, 25, 24))),

    ("CYPHER 2.3 planner=cost runtime=interpreted RETURN", PreParsedStatement("RETURN", Seq(
      ConfigurationOptions(Some(VersionOption("2.3")), Seq(CostPlannerOption, InterpretedRuntimeOption))), (1, 45, 44))),
    ("CYPHER 2.3 planner=dp runtime=interpreted RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions(
      Some(VersionOption("2.3")), Seq(DPPlannerOption, InterpretedRuntimeOption))), (1, 43, 42))),
    ("CYPHER 2.3 planner=idp runtime=interpreted RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions
      (Some(VersionOption("2.3")), Seq(IDPPlannerOption, InterpretedRuntimeOption))), (1, 44, 43))),
    ("CYPHER 2.3 planner=idp runtime=interpreted RETURN", PreParsedStatement("RETURN", Seq(ConfigurationOptions
      (Some(VersionOption("2.3")), Seq(IDPPlannerOption, InterpretedRuntimeOption))), (1, 44, 43))),
    ("explainmatch", PreParsedStatement("explainmatch", Seq.empty, (1, 1, 0)))
    )

  test("run the tests") {
    forAll(queries) {
      case (query, expected) => parse(query) should equal(expected)
    }
  }

  private def parse(arg:String): PreParsedStatement = {
    CypherPreParser(arg)
  }

  private implicit def lift(pos: (Int, Int, Int)): InputPosition = InputPosition(pos._3, pos._1, pos._2)
}
