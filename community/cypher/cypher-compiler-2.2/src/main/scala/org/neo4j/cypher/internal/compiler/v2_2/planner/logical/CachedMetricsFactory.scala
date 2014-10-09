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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.helpers.CachedFunction
import org.neo4j.cypher.internal.compiler.v2_2.planner.{SemanticTable, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

case class CachedMetricsFactory(metricsFactory: MetricsFactory) extends MetricsFactory {
  def newCardinalityEstimator(queryGraphCardinalityModel: QueryGraphCardinalityModel) =
    CachedFunction.byIdentity(metricsFactory.newCardinalityEstimator(queryGraphCardinalityModel))

  def newCostModel(cardinality: CardinalityModel) =
    CachedFunction.byIdentity(metricsFactory.newCostModel(cardinality))

  def newQueryGraphCardinalityModel(statistics: GraphStatistics, semanticTable: SemanticTable) = new QueryGraphCardinalityModel {
    private val cache = new java.util.IdentityHashMap[QueryGraph, Cardinality]()
    private val inner = metricsFactory.newQueryGraphCardinalityModel(statistics, semanticTable)

    def apply(input: QueryGraph): Cardinality = cache.get(input) match {
      case null =>
        val newValue = inner(input)
        cache.put(input, newValue)
        newValue

      case value => value
    }
  }

  def newCandidateListCreator(): (Seq[LogicalPlan]) => CandidateList = metricsFactory.newCandidateListCreator()
}
