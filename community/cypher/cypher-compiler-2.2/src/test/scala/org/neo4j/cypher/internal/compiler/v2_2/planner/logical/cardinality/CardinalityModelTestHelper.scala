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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, CardinalitySupport, Metrics}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport, PlannerQuery, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

trait CardinalityModelTestHelper extends CardinalityTestHelper {

  self: CypherFunSuite with LogicalPlanningTestSupport =>

  def createCardinalityModel(stats: GraphStatistics, semanticTable: SemanticTable): QueryGraphCardinalityModel

  def givenPattern(pattern: String) = TestUnit(pattern)
  def givenPredicate(pattern: String) = TestUnit("MATCH " + pattern)

  implicit class RichTestUnit(testUnit: CardinalityTestHelper#TestUnit) {
    def shouldHaveQueryGraphCardinality(number: Double) {
      import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.CardinalitySupport.Eq

      val (statistics, semanticTable) = testUnit.prepareTestContext

      val queryGraph = testUnit.createQueryGraph()
      val cardinalityModel: QueryGraphCardinalityModel = createCardinalityModel(statistics, semanticTable)
      val result = cardinalityModel(queryGraph, QueryGraphCardinalityInput(Map.empty, Cardinality(1)))
      result should equal(Cardinality(number))
    }

    def shouldHavePlannerQueryCardinality(f: QueryGraphCardinalityModel => Metrics.CardinalityModel)(number: Double) {
      import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.CardinalitySupport.Eq

      implicit val (statistics, semanticTable) = testUnit.prepareTestContext

      val graphCardinalityModel = createCardinalityModel(statistics, semanticTable)
      val cardinalityModelUnderTest = f(graphCardinalityModel)
      val plannerQuery: PlannerQuery = producePlannerQueryForPattern(testUnit.query)
      val plan = newMockedLogicalPlanWithSolved(Set.empty, plannerQuery)
      cardinalityModelUnderTest(plan, QueryGraphCardinalityInput.empty) should equal(Cardinality(number))
    }
  }

  implicit class RichCardinalityData(cardinalityData: CardinalityData) {
    def forQuery(q: String) = cardinalityData.forQuery(givenPattern(q))
  }

  val DEFAULT_PREDICATE_SELECTIVITY = GraphStatistics.DEFAULT_PREDICATE_SELECTIVITY.factor
  val DEFAULT_EQUALITY_SELECTIVITY = GraphStatistics.DEFAULT_EQUALITY_SELECTIVITY.factor
  val DEFAULT_RANGE_SELECTIVITY = GraphStatistics.DEFAULT_RANGE_SELECTIVITY.factor
  val DEFAULT_REL_UNIQUENESS_SELECTIVITY = GraphStatistics.DEFAULT_REL_UNIQUENESS_SELECTIVITY.factor
}
