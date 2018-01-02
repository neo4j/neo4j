/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class ExplainAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("normal query is marked as such") {
    createNode()
    val result = executeWith(Configs.All + Configs.Morsel, "match (n) return n")

    result.planDescriptionRequested should equal(false)
    result shouldNot be(empty)
  }

  test("explain query is marked as such") {
    createNode()
    val result = executeWith(Configs.All + Configs.Morsel, "explain match (n) return n")

    result.planDescriptionRequested should equal(true)
    result should be(empty)
  }


  test("EXPLAIN for Cypher 3.1") {
    val result = eengine.execute("explain match (n) return n", Map.empty[String, Object])
    result.resultAsString()
    result.getExecutionPlanDescription.toString should include("Estimated Rows")
  }

  test("should handle query with nested expression") {
    val query = """EXPLAIN
                  |WITH
                  |   ['Herfstvakantie Noord'] AS periodName
                  |MATCH (perStart:Day)<-[:STARTS]-(per:Periode)-[:ENDS]->(perEnd:Day) WHERE per.naam=periodName
                  |WITH perStart,perEnd
                  |
                  |MATCH perDays=shortestPath((perStart)-[:NEXT*]->(perEnd))
                  |UNWIND nodes(perDays) as perDay
                  |WITH perDay ORDER by perDay.day
                  |
                  |MATCH (bknStart:Day)-[:NEXT*0..]->(perDay)
                  |WHERE (bknStart)<-[:FROM_DATE]-(:Boeking)
                  |WITH distinct bknStart, collect(distinct perDay) as perDays
                  |
                  |MATCH (bknStart)<-[:FROM_DATE]-(bkn:Boeking)-[:TO_DATE]->(bknEnd)
                  |WITH bknEnd, collect(bkn) as bookings, perDays
                  |WHERE any(perDay IN perDays WHERE perDays = bknEnd OR exists((perDay)-[:NEXT*]->(bknEnd)))
                  |
                  |RETURN count(*), count(distinct bknEnd), avg(size(bookings)),avg(size(perDays));""".stripMargin

    val result = executeWith(Configs.Interpreted, query)
    val plan = result.executionPlanDescription().toString
    result.close()

    plan.toString should include("NestedPlanExpression(VarExpand-Argument)")
  }
}
