/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.InternalPlanDescription.Arguments.EstimatedRows

/**
  * Runs the 14 LDBC queries and checks so that the result is what is expected.
  */
class LdbcAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  import LdbcQueries._

  LDBC_QUERIES.foreach { ldbcQuery =>
    test(ldbcQuery.name) {
      // given
      eengine.execute(ldbcQuery.createQuery, ldbcQuery.createParams)
      ldbcQuery.constraintQueries.foreach(query => eengine.execute(query, Map.empty[String, Any]))

      // when
      val result =
        executeWith(ldbcQuery.expectedToSucceedIn, ldbcQuery.query, params = ldbcQuery.params)
          .toComparableResult

      //then
      result should equal(ldbcQuery.expectedResult)
    }
  }

  test("LDBC query 12 should not get a bad plan because of lost precision in selectivity calculation") {
    // given
    val ldbcQuery = LdbcQueries.Query12
    eengine.execute(ldbcQuery.createQuery, ldbcQuery.createParams)
    ldbcQuery.constraintQueries.foreach(query => eengine.execute(query, Map.empty[String, Any]))

    val updatedLdbc12 =
      """PROFILE MATCH (:Person {id:{1}})-[:KNOWS]-(friend:Person)
        |MATCH (friend)<-[:COMMENT_HAS_CREATOR]-(comment:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass)-[:IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
        |WHERE tagClass.name = {2} OR baseTagClass.name = {2}
        |RETURN friend.id AS friendId, friend.firstName AS friendFirstName, friend.lastName AS friendLastName, collect(DISTINCT tag.name) AS tagNames, count(DISTINCT comment) AS count
        |ORDER BY count DESC, friendId ASC
        |LIMIT {3}
      """.stripMargin

    val params: Map[String, Any] = Map("1" -> 0, "2" -> 1, "3" -> 10)

    val result =
    // when
      executeWith(ldbcQuery.expectedToSucceedIn, ldbcQuery.query, params = params)

    // no precision loss resulting in insane numbers
    all(collectEstimations(result.executionPlanDescription())) should be > 0.0
    all(collectEstimations(result.executionPlanDescription())) should be < 10.0
  }

  private def collectEstimations(plan: InternalPlanDescription): Seq[Double] = {
    plan.arguments.collectFirst {
      case EstimatedRows(estimate) => estimate
    }.get +:
      plan.children.toIndexedSeq.flatMap(collectEstimations)
  }
}
