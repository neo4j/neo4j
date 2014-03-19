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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression

case class Selection(predicates: Seq[Expression], left: LogicalPlan)
                    (implicit val context: LogicalPlanContext) extends LogicalPlan {
  assert(predicates.nonEmpty, "A selection plan should never be created without predicates")

  val lhs = Some(left)

  def rhs = None

  def coveredIds = left.coveredIds

  val cardinality = {
    val selectivity = predicates.map(context.estimator.estimateSelectivity).reduce(_ * _)
    (left.cardinality * selectivity).toInt
  }

  val cost = context.costs.calculateSelection(left.cardinality) + left.cost

  def solvedPredicates: Seq[Expression] = predicates ++ left.solvedPredicates
}
