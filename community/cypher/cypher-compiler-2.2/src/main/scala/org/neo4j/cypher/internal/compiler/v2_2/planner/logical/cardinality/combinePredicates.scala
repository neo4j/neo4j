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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Selectivity
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.groupPredicates._

object combinePredicates {
  // Multiply all predicates together to get one selectivity
  def assumeIndependence(combinations: Set[EstimatedPredicateCombination]): (Set[Predicate], Selectivity) =
    combinations.map {
      p => p._1.containedPredicates -> p._2
    }.reduceOption[(Set[Predicate], Selectivity)] {
      case ((accPreds, accSel), (preds, selectivity)) => (accPreds ++ preds) -> (accSel * selectivity)
    }.getOrElse(Set.empty[Predicate] -> Selectivity(1))

  // Find the most selective predicate combination and use it
  def assumeDependence(combinations: Set[EstimatedPredicateCombination]): (Set[Predicate], Selectivity) =
    combinations.toSeq.sortBy(_._2).headOption.map {
      case (combination, selectivity) => combination.containedPredicates -> selectivity
    }.getOrElse(Set.empty[Predicate] -> Selectivity(1))
}
