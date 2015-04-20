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
package org.neo4j.cypher

/**
 * Runs the 14 LDBC queries and checks so that the result is what is expected.
 */
class LdbcAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {
  import org.neo4j.cypher.LdbcQueries._

  LDBC_QUERIES.foreach { ldbcQuery =>
    test(ldbcQuery.name) {
      //given
      executeWithRulePlannerOnly(ldbcQuery.createQuery, ldbcQuery.createParams.toSeq: _*)
      ldbcQuery.constraintQueries.foreach(executeWithRulePlannerOnly(_))

      //when
      val result = executeWithAllPlanners(ldbcQuery.query, ldbcQuery.params.toSeq: _*).toComparableList

      //then
      result should equal(ldbcQuery.expectedResult)
    }
  }
}
