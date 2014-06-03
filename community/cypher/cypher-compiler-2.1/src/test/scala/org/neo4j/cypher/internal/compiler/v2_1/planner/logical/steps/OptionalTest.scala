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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{Cardinality, QueryGraphSolver, QueryGraphSolvingContext, PlanTable}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps.QueryPlanProducer._
import scala.collection.mutable

class OptionalTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("should introduce apply for unsolved exclusive optional match") {
    // OPTIONAL MATCH (a)-[r]->(b)

    val patternRel = PatternRelationship("r", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val optionalMatch = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships = Set(patternRel)
    )
    val qg = QueryGraph().withAddedOptionalMatch(optionalMatch)

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: SingleRow => Cardinality(1.0)
      case _            => Cardinality(1000.0)
    })

    val fakePlan = newMockedQueryPlan(Set(IdName("a"), IdName("b")))

    implicit val context = newMockedQueryGraphSolvingContext(
      planContext = newMockedPlanContext,
      query = qg,
      strategy = newMockedStrategy(fakePlan),
      metrics = factory.newMetrics(hardcodedStatistics, newMockedSemanticTable)
    )

    val planTable = PlanTable(Map())

    optional(planTable).plans should equal(Seq(planOptional(fakePlan)))
  }

  test("should solve multiple optional matches") {
    // OPTIONAL MATCH (a)-[r]->(b)

    val patternRel1 = PatternRelationship("r1", ("a", "b"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val patternRel2 = PatternRelationship("r2", ("b", "c"), Direction.OUTGOING, Seq.empty, SimplePatternLength)
    val optionalMatch1 = QueryGraph(
      patternNodes = Set("a", "b"),
      patternRelationships = Set(patternRel1)
    )
    val optionalMatch2 = QueryGraph(
      patternNodes = Set("a", "c"),
      patternRelationships = Set(patternRel2)
    )
    val qg = QueryGraph().
      withAddedOptionalMatch(optionalMatch1).
      withAddedOptionalMatch(optionalMatch2)

    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: SingleRow => Cardinality(1.0)
      case _            => Cardinality(1000.0)
    })

    val fakePlan1 = newMockedQueryPlan(Set(IdName("a"), IdName("b")))
    val fakePlan2 = newMockedQueryPlan(Set(IdName("a"), IdName("c")))

    implicit val context = newMockedQueryGraphSolvingContext(
      planContext = newMockedPlanContext,
      query = qg,
      strategy = FakePlanningStrategy(fakePlan1, fakePlan2),
      metrics = factory.newMetrics(hardcodedStatistics, newMockedSemanticTable)
    )

    val planTable = PlanTable(Map())

    optional(planTable).plans should equal(Seq(planOptional(fakePlan1)))
  }
}

case class FakePlanningStrategy(plans: QueryPlan*) extends QueryGraphSolver {
  val queue = mutable.Queue[QueryPlan](plans:_*)
  override def plan(implicit context: QueryGraphSolvingContext, leafPlan: Option[QueryPlan]): QueryPlan = queue.dequeue()
}
