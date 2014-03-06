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

import org.neo4j.cypher.internal.compiler.v2_1.planner.{CantHandleQueryException, CardinalityEstimator, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, HasLabels}

case class SimpleLogicalPlanner(estimator: CardinalityEstimator) extends LogicalPlanner {

  val projectionPlanner = new ProjectionPlanner

  override def plan(qg: QueryGraph): LogicalPlan = {
    val planTableBuilder = Map.newBuilder[Set[Id], LogicalPlan]
    qg.identifiers.foreach( planTableBuilder += identifierSource(_, qg) )
    val planTable = planTableBuilder.result()

    while (planTable.size > 1) {
      throw new CantHandleQueryException
    }

    val logicalPlan = if (planTable.size == 0) SingleRow() else planTable.values.head

    projectionPlanner.amendPlan(qg, logicalPlan)
  }

  def identifierSource(id: Id, qg: QueryGraph) = {
    val idSet = Set(id)
    val predicates = qg.selections.apply(idSet)
    val source = predicates.collectFirst({
      case HasLabels(Identifier(id.name), label :: Nil) =>
        val labelId = label.id
        LabelNodesScan(id, labelId.toRight(label.name), estimator.estimateLabelScan(labelId))
    }).getOrElse(AllNodesScan(id, estimator.estimateAllNodes()))
    idSet -> source
  }
}
