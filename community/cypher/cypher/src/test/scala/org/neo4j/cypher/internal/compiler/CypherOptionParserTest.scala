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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal._
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_3.InputPosition
import org.neo4j.cypher.internal.compiler.v2_3.parser.ParserMonitor

class CypherOptionParserTest extends CypherFunSuite {

  def parse(arg:String): CypherQueryWithOptions = {
    CypherOptionParser(mock[ParserMonitor[CypherQueryWithOptions]]).apply(arg)
  }

  test("should parse version") {
    parse("CYPHER 1.9 MATCH") should equal(CypherQueryWithOptions("MATCH", Seq(VersionOption("1.9")), (1, 12, 11)))
    parse("CYPHER 2.0 THAT") should equal(CypherQueryWithOptions("THAT", Seq(VersionOption("2.0")), (1, 12, 11)))
    parse("CYPHER 2.1 YO") should equal(CypherQueryWithOptions("YO", Seq(VersionOption("2.1")), (1, 12, 11)))
    parse("CYPHER 2.2 HO") should equal(CypherQueryWithOptions("HO", Seq(VersionOption("2.2")), (1, 12, 11)))
    parse("CYPHER 2.3 HO") should equal(CypherQueryWithOptions("HO", Seq(VersionOption("2.3")), (1, 12, 11)))
  }

  test("should parse profile") {
    parse("PROFILE THINGS") should equal(CypherQueryWithOptions("THINGS", Seq(ProfileOption), (1, 9, 8)))
  }

  test("should parse explain") {
    parse("EXPLAIN THIS") should equal(CypherQueryWithOptions("THIS", Seq(ExplainOption), (1, 9, 8)))
  }

  test("should parse multiple options") {
    parse("CYPHER 2.2 PLANNER COST PROFILE PATTERN") should equal(
      CypherQueryWithOptions("PATTERN", Seq(VersionOption("2.2"), CostPlannerOption, ProfileOption), (1, 33, 32))
    )
    parse("EXPLAIN CYPHER 2.1 YALL") should equal(
      CypherQueryWithOptions("YALL", Seq(ExplainOption, VersionOption("2.1")), (1, 20, 19))
    )
  }

  test("should parse version and planner/compiler") {
    parse("CYPHER 2.3 PLANNER COST RETURN") should equal(
      CypherQueryWithOptions("RETURN", Seq(VersionOption("2.3"), CostPlannerOption), (1, 25, 24))
    )
    parse("PLANNER COST RETURN") should equal(
      CypherQueryWithOptions("RETURN",Seq(CostPlannerOption), (1, 14, 13))
    )
    parse("CYPHER 2.3 PLANNER RULE RETURN") should equal(
      CypherQueryWithOptions("RETURN", Seq(VersionOption("2.3"), RulePlannerOption), (1, 25, 24))
    )
    parse("PLANNER RULE RETURN") should equal(CypherQueryWithOptions("RETURN", Seq(RulePlannerOption), (1, 14, 13)))
    parse("CYPHER 2.3 PLANNER IDP RETURN") should equal(
      CypherQueryWithOptions("RETURN", Seq(VersionOption("2.3"), IDPPlannerOption), (1, 24, 23))
    )
    parse("CYPHER 2.3 PLANNER DP RETURN") should equal(
      CypherQueryWithOptions("RETURN", Seq(VersionOption("2.3"), DPPlannerOption), (1, 23, 22))
    )
    parse("PLANNER IDP RETURN") should equal(CypherQueryWithOptions("RETURN",Seq(IDPPlannerOption), (1, 13, 12)))
    parse("PLANNER DP RETURN") should equal(CypherQueryWithOptions("RETURN",Seq(DPPlannerOption), (1, 12, 11)))
  }

  test("should require whitespace between option and query") {
    parse("explainmatch") should equal(CypherQueryWithOptions("explainmatch", Seq.empty, (1, 1, 0)))
  }

  private implicit def lift(pos: (Int, Int, Int)): InputPosition = InputPosition(pos._3, pos._1, pos._2)
}
