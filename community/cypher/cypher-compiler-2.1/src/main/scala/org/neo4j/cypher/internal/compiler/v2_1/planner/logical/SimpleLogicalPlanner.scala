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
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SingleRow
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.ast.SignedIntegerLiteral
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.ast.HasLabels

case class SimpleLogicalPlanner(estimator: CardinalityEstimator) extends LogicalPlanner {

  val projectionPlanner = new ProjectionPlanner

  override def plan(qg: QueryGraph): LogicalPlan = {
    val planTableBuilder = Map.newBuilder[Set[IdName], LogicalPlan]
    qg.identifiers.foreach( planTableBuilder += identifierSource(_, qg) )
    val planTable = planTableBuilder.result()

    while (planTable.size > 1) {
      throw new CantHandleQueryException
    }

    val logicalPlan = if (planTable.size == 0) SingleRow() else planTable.values.head

    projectionPlanner.amendPlan(qg, logicalPlan)
  }

  def identifierSource(id: IdName, qg: QueryGraph) = {
    val idSet = Set(id)
    val predicates = qg.selections.apply(idSet)
    val source = predicates.collectFirst({

      // n:Label
      case HasLabels(Identifier(id.name), label :: Nil) =>
        val labelId = label.id
        NodeByLabelScan(id, labelId.toRight(label.name), estimator.estimateNodeByLabelScan(labelId))

      // id(n) = 12
      case Equals(FunctionInvocation(Identifier("id"), _, IndexedSeq(Identifier(identName))), nodeIdExpr) =>
        val idName = IdName(identName)
        NodeByIdScan(idName, nodeIdExpr, estimator.estimateNodeByIdScan())

    }).getOrElse(AllNodesScan(id, estimator.estimateAllNodes()))
    idSet -> source
  }
}
