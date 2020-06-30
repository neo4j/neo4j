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

import org.neo4j.cypher.CypherExpressionEngineOption
import org.neo4j.cypher.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.CypherOperatorEngineOption
import org.neo4j.cypher.CypherPlannerOption
import org.neo4j.cypher.CypherReplanOption
import org.neo4j.cypher.CypherRuntimeOption
import org.neo4j.cypher.CypherVersion
import org.neo4j.cypher.internal.cache.TestExecutorCaffeineCacheFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InvalidArgumentException

class PreParserTest extends CypherFunSuite {

  val preParser = new PreParser(CypherVersion.default,
    CypherPlannerOption.default,
    CypherRuntimeOption.default,
    CypherExpressionEngineOption.default,
    CypherOperatorEngineOption.default,
    CypherInterpretedPipesFallbackOption.default,
    0,
    TestExecutorCaffeineCacheFactory)

  test("should not allow inconsistent runtime options") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER runtime=slotted runtime=interpreted RETURN 42"))
  }

  test("should not allow multiple versions") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER 3.5 CYPHER 4.1 RETURN 42"))
  }

  test("should not allow both EXPLAIN and PROFILE") {
    intercept[InvalidArgumentException](preParser.preParseQuery("EXPLAIN PROFILE RETURN 42"))
    intercept[InvalidArgumentException](preParser.preParseQuery("PROFILE EXPLAIN RETURN 42"))
  }

  test("should accept just one operator execution mode") {
    preParser.preParseQuery("CYPHER operatorEngine=interpreted RETURN 42").options.operatorEngine should equal(CypherOperatorEngineOption.interpreted)
  }

  test("should accept just one replan strategy") {
    preParser.preParseQuery("CYPHER replan=force RETURN 42").options.replan should equal(CypherReplanOption.force)
    preParser.preParseQuery("CYPHER replan=skip RETURN 42").options.replan should equal(CypherReplanOption.skip)
    preParser.preParseQuery("CYPHER replan=default RETURN 42").options.replan should equal(CypherReplanOption.default)
  }

  test("should not allow multiple conflicting replan strategies") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER replan=force replan=skip RETURN 42"))
  }

  test("should accept just one interpreted pipes fallback mode") {
    preParser.preParseQuery("CYPHER interpretedPipesFallback=disabled RETURN 42").options.interpretedPipesFallback should
      equal(CypherInterpretedPipesFallbackOption.disabled)
    preParser.preParseQuery("CYPHER interpretedPipesFallback=default RETURN 42").options.interpretedPipesFallback should
      equal(CypherInterpretedPipesFallbackOption.whitelistedPlansOnly)
    preParser.preParseQuery("CYPHER interpretedPipesFallback=all RETURN 42").options.interpretedPipesFallback should
      equal(CypherInterpretedPipesFallbackOption.allPossiblePlans)
  }

  test("should not allow multiple conflicting interpreted pipes fallback modes") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER interpretedPipesFallback=all interpretedPipesFallback=disabled RETURN 42"))
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER interpretedPipesFallback=default interpretedPipesFallback=disabled RETURN 42"))
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER interpretedPipesFallback=default interpretedPipesFallback=all RETURN 42"))
  }

  test("should only allow interpreted pipes fallback mode in pipelined runtime") {
    intercept[InvalidArgumentException](preParser.preParseQuery("CYPHER runtime=slotted interpretedPipesFallback=all RETURN 42"))
  }

  test("should parse all variants of periodic commit") {
    val variants =
      List(
        "USING PERIODIC COMMIT",
        " USING PERIODIC COMMIT",
        "USING  PERIODIC COMMIT",
        "USING PERIODIC  COMMIT",
        """USING
           PERIODIC
           COMMIT""",
        "CYPHER 3.5 planner=cost debug=ofCourse USING PERIODIC COMMIT",
        "CYPHER 4.1 planner=cost debug=ofCourse USING PERIODIC COMMIT",
        "CYPHER 4.2 planner=cost debug=ofCourse USING PERIODIC COMMIT",
        "using periodic commit",
        "UsING pERIOdIC COMmIT"
      )

    for (x <- variants) {
      val query = " LOAD CSV file://input.csv AS row CREATE (n)"
      preParser.preParseQuery(x+query).options.isPeriodicCommit should be(true)
    }
  }

  test("should not call periodic commit on innocent (but evil) queries") {
    val queries =
      List(
        "MATCH (n) RETURN n",
        "CREATE ({name: 'USING PERIODIC COMMIT'})",
        "CREATE ({`USING PERIODIC COMMIT`: true})",
        "CREATE (:`USING PERIODIC COMMIT`)",
        "CYPHER 3.5 debug=usingPeriodicCommit PROFILE CREATE ({name: 'USING PERIODIC COMMIT'})",
        "CYPHER 4.1 debug=usingPeriodicCommit PROFILE CREATE ({name: 'USING PERIODIC COMMIT'})",
        "CYPHER 4.2 debug=usingPeriodicCommit PROFILE CREATE ({name: 'USING PERIODIC COMMIT'})",
        """CREATE ({name: '
          |USING PERIODIC COMMIT')""".stripMargin
      )

    for (query <- queries) {
      preParser.preParseQuery(query).options.isPeriodicCommit should be(false)
    }
  }
}
