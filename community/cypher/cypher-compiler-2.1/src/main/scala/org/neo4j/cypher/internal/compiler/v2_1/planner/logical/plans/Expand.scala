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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.ast.{LabelName, RelTypeName}

case class Expand(left: LogicalPlan, from: IdName, dir: Direction, types: Seq[RelTypeName], to: IdName, relName: IdName)
                 (implicit val context: LogicalPlanContext) extends LogicalPlan {
  val lhs = Some(left)
  def rhs = None

  val cardinality = {
    val node: Seq[LabelName] = context.queryGraph.knownLabelsOnNode(from)
    val knownLabelsOnNode = node.flatMap(_.id)
    val relTypeIds = types.map(_.id.get)

    val estimatedNoOfRelationshipsPerNode =
      context.estimator.estimateExpandRelationship(knownLabelsOnNode, relTypeIds, dir)

    left.cardinality * estimatedNoOfRelationshipsPerNode
  }

  val cost = left.cost + context.costs.calculateExpandRelationship(cardinality)

  val coveredIds = left.coveredIds + to + relName

  def solvedPredicates = left.solvedPredicates
}
