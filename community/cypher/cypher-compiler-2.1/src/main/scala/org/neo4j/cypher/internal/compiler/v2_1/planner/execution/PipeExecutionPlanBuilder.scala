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
package org.neo4j.cypher.internal.compiler.v2_1.planner.execution

import org.neo4j.cypher.internal.compiler.v2_1.pipes._
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.Monitors
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.PipeInfo


class PipeExecutionPlanBuilder(monitors: Monitors) {
  def build(plan: LogicalPlan): PipeInfo = {
    val updating = false

    def buildPipe(plan: LogicalPlan): Pipe = {
      val left = plan.lhs.map(buildPipe)
      val right = plan.rhs.map(buildPipe)

      implicit val monitor = monitors.newMonitor[PipeMonitor]()
      plan match {
        case Projection(_, expressions) =>
          ProjectionNewPipe(left.get, toLegacyExpressions(expressions))

        case SingleRow() =>
          NullPipe()

        case AllNodesScan(IdName(id)) =>
          AllNodesScanPipe(id)

        case NodeByLabelScan(IdName(id), label) =>
          NodeByLabelScanPipe(id, label)

        case NodeByIdSeek(IdName(id), nodeIdExpr, _) =>
          NodeByIdSeekPipe(id, nodeIdExpr.asCommandExpression)

        case RelationshipByIdSeek(IdName(id), relIdExpr, _) =>
          RelationshipByIdSeekPipe(id, relIdExpr.asCommandExpression)

        case NodeIndexSeek(IdName(id), labelId, propertyKeyId, valueExpr) =>
          NodeIndexSeekPipe(id, Right(labelId), Right(propertyKeyId), valueExpr.asCommandExpression)

        case NodeIndexUniqueSeek(IdName(id), labelId, propertyKeyId, valueExpr) =>
          NodeUniqueIndexSeekPipe(id, Right(labelId), Right(propertyKeyId), valueExpr.asCommandExpression)

        case Selection(predicates, _) =>
          FilterPipe(left.get, predicates.map(_.asCommandPredicate).reduce(_ ++ _))

        case CartesianProduct(_, _) =>
          CartesianProductPipe(left.get, right.get)
      }
    }

    val topLevelPipe = buildPipe(plan)

    PipeInfo(topLevelPipe, updating, None)
  }

  def toLegacyExpressions(expressions: Map[String, Expression]) = expressions.mapValues(_.asCommandExpression)
}
