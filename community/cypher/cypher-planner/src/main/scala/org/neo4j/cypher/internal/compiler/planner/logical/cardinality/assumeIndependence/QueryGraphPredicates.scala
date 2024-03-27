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

import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.VariableList
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.util.helpers.MapSupport.PowerMap

/**
 * Predicates of a query graph partitioned in order to calculate its cardinality.
 *
 * @param localLabelInfo      node labels explicitly defined in HasLabels predicates inside of the query graph.
 * @param localOnlyLabelInfo  node labels that are in [[QueryGraphPredicates.localLabelInfo]] but are not previously known nodes labels (in previousLabelInfo, passed as an argument to [[QueryGraphPredicates.partitionSelections]])
 *                            localLabelInfo \ previousLabelInfo
 * @param allLabelInfo        previously known nodes labels, passed as an argument to [[QueryGraphPredicates.partitionSelections]], merged with [[localLabelInfo]].
 *                            localLabelInfo U previousLabelInfo
 * @param externalLabelInfo   previously known nodes labels, unless they are also present in [[localLabelInfo]].
 *                            previousLabelInfo \ localLabelInfo
 * @param uniqueRelationships relationships with Unique predicates as introduced by AddUniquenessPredicates.
 * @param otherPredicates     kitchen sink, all the predicates that weren't picked up in the other parameters.
 */
case class QueryGraphPredicates(
  localLabelInfo: LabelInfo,
  localOnlyLabelInfo: LabelInfo,
  allLabelInfo: LabelInfo,
  externalLabelInfo: LabelInfo,
  uniqueRelationships: Set[LogicalVariable],
  otherPredicates: Set[Predicate]
)

object QueryGraphPredicates {

  def partitionSelections(
    previousLabelInfo: LabelInfo,
    localLabelInfo: LabelInfo,
    selections: Selections
  ): QueryGraphPredicates = {
    val (uniqueRelationships, otherPredicates) =
      selections.predicates.foldLeft((Set.empty[LogicalVariable], Set.empty[Predicate])) {
        case ((uniqueRelationships, otherPredicates), Predicate(_, HasLabels(_: Variable, _))) =>
          (uniqueRelationships, otherPredicates)
        case ((uniqueRelationships, otherPredicates), Predicate(_, Unique(VariableList(relationships)))) =>
          (uniqueRelationships ++ relationships, otherPredicates)
        case ((uniqueRelationships, otherPredicates), otherPred) => (uniqueRelationships, otherPredicates + otherPred)
      }

    val externalLabelInfo = previousLabelInfo.fuseLeft(localLabelInfo)(_ -- _)
    val localOnlyLabelInfo = localLabelInfo.fuseLeft(previousLabelInfo)(_ -- _)

    QueryGraphPredicates(
      localLabelInfo = localLabelInfo,
      localOnlyLabelInfo = localOnlyLabelInfo,
      allLabelInfo = localLabelInfo.fuse(previousLabelInfo)(_ ++ _),
      externalLabelInfo = externalLabelInfo,
      uniqueRelationships = uniqueRelationships,
      otherPredicates = otherPredicates
    )
  }
}
