/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast.{UnsignedIntegerLiteral, Expression, Identifier, SignedIntegerLiteral}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer

class WithPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport {

  import QueryPlanProducer._

  test("should build plans for simple WITH that adds a constant to the rows") {
    implicit val statistics = hardcodedStatistics
    implicit val planContext = newMockedPlanContext
    implicit val planner = newPlanner(newMetricsFactory)

    val result = produceQueryPlan("MATCH (a) WITH a LIMIT 1 RETURN 1 as `b`")
    val expected =
      planRegularProjection(
        planTailApply(
          planStarProjection(
            planLimit(
              planAllNodesScan("a"),
              UnsignedIntegerLiteral("1") _
            ),
            Map[String, Expression]("a" -> ident("a"))
          ),
          planSingleRow()
        ),
        Map[String, Expression]("b" -> SignedIntegerLiteral("1") _)
      )

    result should equal(expected)
  }

  test("should build plans for two matches separated by WITH") {
    implicit val statistics = hardcodedStatistics
    implicit val planContext = newMockedPlanContext
    implicit val planner = newPlanner(newMetricsFactory)

    val rel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq(), SimplePatternLength)

    val result = produceQueryPlan("MATCH (a) WITH a LIMIT 1 MATCH (a)-[r]->(b) RETURN b")
    val expected =
      planRegularProjection(
        planTailApply(
          planStarProjection(
            planLimit(
              planAllNodesScan("a"),
              UnsignedIntegerLiteral("1") _
            ),
            Map[String, Expression]("a" -> ident("a"))
          ),
          planExpand(
            planArgumentRow(Set("a")),
            "a", Direction.OUTGOING, Seq(), "b", "r", SimplePatternLength, rel
          )
        ),
        Map[String, Expression]("b" -> ident("b"))
      )

    result should equal(expected)
  }
}
