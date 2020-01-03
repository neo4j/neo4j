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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.{Metrics, PlannerDefaults}
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics._
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.v4_0.util.Cardinality
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

trait CardinalityModelTestHelper extends CardinalityTestHelper {

  self: CypherFunSuite with LogicalPlanningTestSupport =>

  def createQueryGraphCardinalityModel(stats: GraphStatistics): QueryGraphCardinalityModel

  def givenPattern(pattern: String) = TestUnit(pattern)
  def givenPredicate(pattern: String) = TestUnit("MATCH " + pattern)

  implicit class RichTestUnit(testUnit: CardinalityTestHelper#TestUnit) {
    def shouldHaveQueryGraphCardinality(number: Double) {
      // used to handle double rounding errors in assertion
      import org.neo4j.cypher.internal.compiler.planner.logical.CardinalitySupport.Eq

      val (statistics, semanticTable) = testUnit.prepareTestContext

      val (queryGraph, rewrittenSemanticTable) = testUnit.createQueryGraph(semanticTable)
      val cardinalityModel = createQueryGraphCardinalityModel(statistics)
      val input = QueryGraphSolverInput(Map.empty, testUnit.inboundCardinality, testUnit.strictness)
      val result = cardinalityModel(queryGraph, input, rewrittenSemanticTable)
      result should equal(Cardinality(number))
    }

    def shouldHavePlannerQueryCardinality(f: QueryGraphCardinalityModel => Metrics.CardinalityModel)(number: Double) {
      // used to handle double rounding errors in assertion
      import org.neo4j.cypher.internal.compiler.planner.logical.CardinalitySupport.Eq

      val (statistics, semanticTable) = testUnit.prepareTestContext

      val graphCardinalityModel = createQueryGraphCardinalityModel(statistics)
      val cardinalityModelUnderTest = f(graphCardinalityModel)
      val (plannerQuery, _) = producePlannerQueryForPattern(testUnit.query)
      cardinalityModelUnderTest(plannerQuery, QueryGraphSolverInput.empty, semanticTable) should equal(Cardinality(number))
    }
  }

  implicit class RichCardinalityData(cardinalityData: CardinalityData) {
    def forQuery(q: String) = cardinalityData.forQuery(givenPattern(q))
  }

  val DEFAULT_PREDICATE_SELECTIVITY: Double = PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY.factor
  val DEFAULT_EQUALITY_SELECTIVITY: Double = PlannerDefaults.DEFAULT_EQUALITY_SELECTIVITY.factor
  val DEFAULT_RANGE_SELECTIVITY: Double = PlannerDefaults.DEFAULT_RANGE_SELECTIVITY.factor
  val DEFAULT_REL_UNIQUENESS_SELECTIVITY: Double = PlannerDefaults.DEFAULT_REL_UNIQUENESS_SELECTIVITY.factor
  val DEFAULT_RANGE_SEEK_FACTOR: Double = PlannerDefaults.DEFAULT_RANGE_SEEK_FACTOR
  val DEFAULT_LIST_CARDINALITY: Int = PlannerDefaults.DEFAULT_LIST_CARDINALITY.amount.toInt
  val DEFAULT_LIMIT_CARDINALITY: Int = PlannerDefaults.DEFAULT_LIMIT_CARDINALITY.amount.toInt
  val DEFAULT_DISTINCT_SELECTIVITY: Double = PlannerDefaults.DEFAULT_DISTINCT_SELECTIVITY.factor
}
