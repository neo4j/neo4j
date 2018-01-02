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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality

import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.triplet.TripletQueryGraphCardinalityModel.NodeCardinalities
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, cardinality}

import scala.annotation.tailrec

case class NodeCardinalityEstimator(estimator: cardinality.SelectivityEstimator, allNodes: Cardinality, inputCardinality: Cardinality)
  extends ((QueryGraph) => (NodeCardinalities, Set[Expression])) {

  def apply(qg: QueryGraph): (NodeCardinalities, Set[Expression]) = {
    val arguments = qg.argumentIds
    val estimatedNodes = qg.patternNodes ++ arguments
    val selections = qg.selections

    @tailrec
    def recurse(nodes: List[IdName], nodeCardinalities: NodeCardinalities, predicates: Set[Expression]): (NodeCardinalities, Set[Expression]) = nodes match {
      case node :: remaining =>
        val nodePredicates = selections.predicatesGivenForRequiredSymbol(arguments + node, node).toSet
        val nodeSelectivity = estimator.and(nodePredicates)
        val baseCardinality = if (arguments.contains(node)) inputCardinality else allNodes
        val cardinality = baseCardinality * nodeSelectivity

        recurse(remaining, nodeCardinalities + (node -> cardinality), predicates ++ nodePredicates)

      case Nil =>
        (nodeCardinalities, predicates)
    }

    recurse(estimatedNodes.toList, Map.empty, Set.empty)
  }
}

