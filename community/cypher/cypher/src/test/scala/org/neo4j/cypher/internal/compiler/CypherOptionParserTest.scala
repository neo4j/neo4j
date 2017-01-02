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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.InputPosition
import org.neo4j.cypher.internal.compiler.v2_2.parser.ParserMonitor
import org.scalatest.prop.TableDrivenPropertyChecks

class CypherOptionParserTest extends CypherFunSuite with TableDrivenPropertyChecks {

  val queries = Table(
    ("query", "expected"),
    ("CYPHER 1.9 MATCH", CypherQueryWithOptions("MATCH", Seq(ConfigurationOptions(Some(VersionOption("1.9")), Seq.empty)), (1, 12, 11))),
    ("CYPHER 2.0 THAT", CypherQueryWithOptions("THAT", Seq(ConfigurationOptions(Some(VersionOption("2.0")), Seq.empty)), (1, 12, 11))),
    ("CYPHER 2.1 YO", CypherQueryWithOptions("YO", Seq(ConfigurationOptions(Some(VersionOption("2.1")), Seq.empty)), (1, 12, 11))),
    ("CYPHER 2.2 HO", CypherQueryWithOptions("HO", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty)), (1, 12, 11))),
    ("PROFILE THINGS", CypherQueryWithOptions("THINGS", Seq(ProfileOption), (1, 9, 8))),
    ("EXPLAIN THIS", CypherQueryWithOptions("THIS", Seq(ExplainOption), (1, 9, 8))),
    ("CYPHER 2.2 PLANNER COST PROFILE PATTERN", CypherQueryWithOptions("PATTERN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty), CostPlannerOption, ProfileOption), (1, 33, 32))),
    ("EXPLAIN CYPHER 2.1 YALL", CypherQueryWithOptions("YALL", Seq(ExplainOption, ConfigurationOptions(Some(VersionOption("2.1")), Seq.empty)), (1, 20, 19))),
    ("CYPHER 2.2 PLANNER COST RETURN", CypherQueryWithOptions("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty), CostPlannerOption), (1, 25, 24))),
    ("PLANNER COST RETURN", CypherQueryWithOptions("RETURN", Seq(CostPlannerOption), (1, 14, 13))),
    ("CYPHER 2.2 PLANNER RULE RETURN", CypherQueryWithOptions("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty), RulePlannerOption), (1, 25, 24))),
    ("PLANNER RULE RETURN", CypherQueryWithOptions("RETURN", Seq(RulePlannerOption), (1, 14, 13))),
    ("CYPHER 2.2 PLANNER IDP RETURN", CypherQueryWithOptions("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty), IDPPlannerOption), (1, 24, 23))),
    ("CYPHER 2.2 PLANNER DP RETURN", CypherQueryWithOptions("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty), DPPlannerOption), (1, 23, 22))),
    ("PLANNER IDP RETURN", CypherQueryWithOptions("RETURN", Seq(IDPPlannerOption), (1, 13, 12))),
    ("PLANNER DP RETURN", CypherQueryWithOptions("RETURN", Seq(DPPlannerOption), (1, 12, 11))),
    ("CYPHER planner=cost RETURN", CypherQueryWithOptions("RETURN", Seq(ConfigurationOptions(None, Seq(CostPlannerOption))), (1, 21, 20))),
    ("CYPHER 2.2 planner=cost RETURN", CypherQueryWithOptions("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq(CostPlannerOption))), (1, 25, 24))),
    ("CYPHER 2.2 planner = idp RETURN", CypherQueryWithOptions("RETURN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq(IDPPlannerOption))), (1, 26, 25))),
    ("CYPHER planner =dp RETURN", CypherQueryWithOptions("RETURN", Seq(ConfigurationOptions(None, Seq(DPPlannerOption))), (1, 20, 19))),
    ("explainmatch", CypherQueryWithOptions("explainmatch", Seq.empty, (1, 1, 0))),
    ("""    CYPHER 2.2 PLANNER COST PROFILE PATTERN""", CypherQueryWithOptions("PATTERN", Seq(ConfigurationOptions(Some(VersionOption("2.2")), Seq.empty), CostPlannerOption, ProfileOption), (1, 37, 36)))
  )

  test("run the tests") {
    forAll(queries) {
      case (query, expected) => parse(query) should equal(expected)
    }
  }

  private def parse(arg:String): CypherQueryWithOptions = {
    CypherOptionParser(mock[ParserMonitor[CypherQueryWithOptions]]).apply(arg)
  }

  private implicit def lift(pos: (Int, Int, Int)): InputPosition = InputPosition(pos._3, pos._1, pos._2)
}
