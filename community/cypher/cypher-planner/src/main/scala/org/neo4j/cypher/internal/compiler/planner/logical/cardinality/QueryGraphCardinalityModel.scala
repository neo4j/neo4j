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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.AssumeIndependenceQueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.LabelInferenceStrategy
import org.neo4j.cypher.internal.planner.spi.PlanContext

object QueryGraphCardinalityModel {

  def default(
    planContext: PlanContext,
    selectivityCalculator: SelectivityCalculator,
    labelInferenceStrategy: LabelInferenceStrategy
  ): QueryGraphCardinalityModel =
    new AssumeIndependenceQueryGraphCardinalityModel(
      planContext,
      selectivityCalculator,
      IndependenceCombiner,
      labelInferenceStrategy
    )
}
