/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.triplet

import org.neo4j.cypher.internal.frontend.v2_3.ast.{LabelName, RelTypeName}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.{LabelInfo, QueryGraphSolverInput}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.TokenSpec
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.TokenSpec.{LabelSpecs, RelTypeSpecs}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, PatternRelationship}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{QueryGraph, Selections}
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, SemanticTable, RelTypeId, LabelId}

case class TripletConverter(qg: QueryGraph, input: QueryGraphSolverInput, semanticTable: SemanticTable)
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
      directed = pattern.dir != SemanticDirection.BOTH,
      length = pattern.length
    )
  }

  private def findLabelSpecsForGraph(qg: QueryGraph, input: QueryGraphSolverInput): LabelSpecs = {
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
