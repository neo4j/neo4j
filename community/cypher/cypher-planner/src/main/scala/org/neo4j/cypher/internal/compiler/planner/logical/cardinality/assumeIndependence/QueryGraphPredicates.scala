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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.Selections

import scala.collection.mutable

/**
 * Predicates of a query graph partitioned in order to calculate its cardinality.
 *
 * @param localLabelInfo node labels explicitly defined in HasLabels predicates inside of the query graph.
 * @param allLabelInfo previously known nodes labels, passed as an argument to [[QueryGraphPredicates.partitionSelections]], merged with [[localLabelInfo]].
 * @param externalLabelInfo previously known nodes labels, unless they are also present in [[localLabelInfo]].
 * @param uniqueRelationships relationships with Unique predicates as introduced by AddUniquenessPredicates.
 * @param otherPredicates kitchen sink, all the predicates that weren't picked up in the other parameters.
 */
case class QueryGraphPredicates(
  localLabelInfo: LabelInfo,
  allLabelInfo: LabelInfo,
  externalLabelInfo: LabelInfo,
  uniqueRelationships: Set[LogicalVariable],
  otherPredicates: Set[Predicate]
)

object QueryGraphPredicates {

  def partitionSelections(labelInfo: LabelInfo, selections: Selections): QueryGraphPredicates = {
    val allLabelInfoBuilder = mutable.Map.empty[LogicalVariable, mutable.Set[LabelName]]
    val uniqueRelationshipsBuilder = Set.newBuilder[LogicalVariable]
    val otherPredicatesBuilder = Set.newBuilder[Predicate]
    selections.predicates.foreach {
      case Predicate(_, HasLabels(v: Variable, labels)) =>
        allLabelInfoBuilder.updateWith(v)(existingLabels =>
          Some(existingLabels.getOrElse(mutable.Set.empty).addAll(labels))
        )
      case Predicate(_, Unique(VariableList(relationships))) =>
        uniqueRelationshipsBuilder.addAll(relationships)
      case otherPredicate =>
        otherPredicatesBuilder.addOne(otherPredicate)
    }
    val localLabelInfo = allLabelInfoBuilder.view.mapValues(_.toSet).toMap
    val externalLabelInfoBuilder = Map.newBuilder[LogicalVariable, Set[LabelName]]
    labelInfo.foreach {
      case (variable, labels) =>
        val localLabels = localLabelInfo.getOrElse(variable, Set.empty)
        allLabelInfoBuilder.updateWith(variable)(existingLabels =>
          Some(existingLabels.getOrElse(mutable.Set.empty).addAll(labels))
        )
        externalLabelInfoBuilder.addOne(variable -> labels.diff(localLabels))
    }
    QueryGraphPredicates(
      localLabelInfo = localLabelInfo,
      allLabelInfo = allLabelInfoBuilder.view.mapValues(_.toSet).toMap,
      externalLabelInfo = externalLabelInfoBuilder.result(),
      uniqueRelationships = uniqueRelationshipsBuilder.result(),
      otherPredicates = otherPredicatesBuilder.result()
    )
  }
}
