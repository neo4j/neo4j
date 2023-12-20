/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.SelectivityCombiner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Selectivity

case class QueryGraphCardinalityContext(
  graphStatistics: GraphStatistics,
  selectivityCalculator: SelectivityCalculator,
  combiner: SelectivityCombiner,
  relTypeInfo: RelTypeInfo,
  semanticTable: SemanticTable,
  indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
  cardinalityModel: CardinalityModel,
  allNodesCardinality: Cardinality,
  labelInferenceStrategy: LabelInferenceStrategy
) {

  def predicatesSelectivity(labelInfo: LabelInfo, predicates: Set[Predicate]): Selectivity =
    predicatesSelectivityWithExtraRelTypeInfo(labelInfo, Map.empty, predicates)

  def predicatesSelectivityWithExtraRelTypeInfo(
    labelInfo: LabelInfo,
    extraRelTypeInfo: RelTypeInfo,
    predicates: Set[Predicate]
  ): Selectivity =
    if (predicates.isEmpty)
      Selectivity.ONE
    else
      selectivityCalculator.apply(
        Selections(predicates),
        labelInfo,
        relTypeInfo ++ extraRelTypeInfo,
        semanticTable,
        indexPredicateProviderContext,
        cardinalityModel
      )
}
