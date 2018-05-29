/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments.EstimatedRows
import org.neo4j.cypher.{ExecutionEngineFunSuite, LdbcQueries, NewPlannerTestSupport}

/**
 * Runs the 14 LDBC queries and checks so that the result is what is expected.
 */
class LdbcAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {
  import org.neo4j.cypher.LdbcQueries._

  LDBC_QUERIES.foreach { ldbcQuery =>
    test(ldbcQuery.name) {
      try {
        //given
        eengine.execute(ldbcQuery.createQuery, ldbcQuery.createParams)
        ldbcQuery.constraintQueries.foreach(updateWithBothPlannersAndCompatibilityMode(_))

        //when
        val result = if (ldbcQuery.supportedInCompiledRuntime)
          executeWithAllPlannersAndRuntimesAndCompatibilityMode(ldbcQuery.query, ldbcQuery.params.toSeq: _*).toComparableResult
        else
          executeWithAllPlannersAndCompatibilityMode(ldbcQuery.query, ldbcQuery.params.toSeq: _*).toComparableResult

        //then
        result should equal(ldbcQuery.expectedResult)

      } catch {
        case e: Throwable =>
          System.err.println(
            s"""|QUERY *************
                |${ldbcQuery.query}
                |
                |PREPARE QUERY *************
                |${ldbcQuery.createQuery}
                |
                |""".stripMargin)
          throw e
      }
    }
  }

  test("LDBC query 12 should not get a bad plan because of lost precision in selectivity calculation") {
    updateWithBothPlannersAndCompatibilityMode(LdbcQueries.Query12.createQuery, LdbcQueries.Query12.createParams.toSeq: _*)
    LdbcQueries.Query12.constraintQueries.foreach(updateWithBothPlannersAndCompatibilityMode(_))

    val updatedLdbc12 =
      """MATCH (:Person {id:{1}})-[:KNOWS]-(friend:Person)
        |MATCH (friend)<-[:COMMENT_HAS_CREATOR]-(comment:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_TAG]->(tag:Tag)-[:HAS_TYPE]->(tagClass:TagClass)-[:IS_SUBCLASS_OF*0..]->(baseTagClass:TagClass)
        |WHERE tagClass.name = {2} OR baseTagClass.name = {2}
        |RETURN friend.id AS friendId, friend.firstName AS friendFirstName, friend.lastName AS friendLastName, collect(DISTINCT tag.name) AS tagNames, count(DISTINCT comment) AS count
        |ORDER BY count DESC, friendId ASC
        |LIMIT {3}
      """.stripMargin

    val params: Map[String, Any] = Map("1" -> 0, "2" -> 1, "3" -> 10)

    val result = executeWithAllPlannersAndCompatibilityMode(s"PROFILE $updatedLdbc12", params.toSeq:_*)

    // no precision loss resulting in insane numbers
    all (collectEstimations(result.executionPlanDescription())) should be > 0.0
    all (collectEstimations(result.executionPlanDescription())) should be < 10.0
  }

  private def collectEstimations(plan: InternalPlanDescription): Seq[Double] = {
    plan.arguments.collectFirst {
      case EstimatedRows(estimate) => estimate
    }.get +:
      plan.children.toIndexedSeq.flatMap(collectEstimations)
  }
}
