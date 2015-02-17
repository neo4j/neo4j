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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compiler.v2_2.executionplan.InternalExecutionResult

/**
 * Runs the 14 LDBC queries and checks so that the result is what is expected.
 */
class LdbcAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {
  import org.neo4j.cypher.LdbcQueries._

  LDBC_QUERIES.foreach { ldbcQuery =>
    test(ldbcQuery.name) {
      //given
      execute(ldbcQuery.createQuery, ldbcQuery.createParams.toSeq: _*)
      ldbcQuery.constraintQueries.foreach(execute(_))

      //when
      val result = executeWithNewPlanner(s"PLANNER COST ${ldbcQuery.query}", ldbcQuery.params.toSeq: _*).result

      //then
      result should equal(ldbcQuery.expectedResult)
    }
  }

  /**
   * Get rid of Arrays to make it easier to compare results by equality.
   */
  implicit class RichInternalExecutionResults(res: InternalExecutionResult) {
    implicit def result: Seq[Map[String, Any]] = res.toList.map((map: Map[String, Any]) =>
      map.map {
        case (k, a: Array[_]) => k -> a.toList
        case m => m
      }
    )
  }

}
