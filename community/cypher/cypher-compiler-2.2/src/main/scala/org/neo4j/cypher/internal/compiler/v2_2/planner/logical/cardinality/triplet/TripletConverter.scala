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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.triplet

import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, RelTypeId}
import org.neo4j.cypher.internal.compiler.v2_2.ast.{LabelName, RelTypeName}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.{LabelInfo, QueryGraphCardinalityInput}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.TokenSpec
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.TokenSpec.{LabelSpecs, RelTypeSpecs}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, PatternRelationship}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{Selections, SemanticTable, QueryGraph}
import org.neo4j.graphdb.Direction

case class TripletConverter(qg: QueryGraph, input: QueryGraphCardinalityInput, semanticTable: SemanticTable)
  extends (PatternRelationship => Triplet) {

  val labelSpecs = findLabelSpecsForGraph(qg, input)
  val relTypeSpecs = findRelTypeSpecsForGraph(qg)

  def apply(pattern: PatternRelationship) = {
    val name = pattern.name
    val (left, right) = pattern.inOrder

    Triplet(
      name = name,
      left = left,
      leftLabels = labelSpecs(left),
      right = right,
      rightLabels = labelSpecs(right),
      relTypes = relTypeSpecs(name),
      directed = pattern.dir != Direction.BOTH,
      length = pattern.length
    )
  }

  private def findLabelSpecsForGraph(qg: QueryGraph, input: QueryGraphCardinalityInput): LabelSpecs = {
    val selections = qg.selections
    val labelInfo = input.labelInfo
    qg.patternNodes.collect {
      case node => node -> findLabelSpecsForNode(node, selections, labelInfo)
    }.toMap
  }

  private def findLabelSpecsForNode(node: IdName, selections: Selections, labelInfo: LabelInfo): Set[TokenSpec[LabelId]] =
    TokenSpec.mapFrom[LabelName, LabelId](selections.labelsOnNode(node) ++ labelInfo.getOrElse(node, Set.empty))(semanticTable)

  private def findRelTypeSpecsForGraph(qg: QueryGraph): RelTypeSpecs = {
    qg.patternRelationships.collect {
      case pattern: PatternRelationship =>
        pattern.name -> findRelTypeSpecsForTypes(pattern.types.toSet)
    }.toMap
  }

  private def findRelTypeSpecsForTypes(types: Set[RelTypeName]): Set[TokenSpec[RelTypeId]] =
    TokenSpec.mapFrom[RelTypeName, RelTypeId](types)(semanticTable)
}
