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

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SimpleLogicalPlanner._

case class idSeekLeafPlanner(predicates: Seq[Expression], isRelationship: Identifier => Boolean) extends LeafPlanner {
  def apply(planTable: LeafPlanTable)(implicit context: LogicalPlanContext): LeafPlanTable =
    predicates.foldLeft(planTable) {
      (planTable, predicate) =>
        predicate match {
          // id(n) = value
          case Equals(FunctionInvocation(Identifier("id"), _, IndexedSeq(id@Identifier(identName))), ConstantExpression(idExpr)) =>
            val idName = IdName(identName)
            val alternative =
              if (isRelationship(id))
                RelationshipByIdSeek(idName, idExpr, context.estimator.estimateRelationshipByIdSeek())
              else
                NodeByIdSeek(idName, idExpr, context.estimator.estimateNodeByIdSeek())

            planTable.updateIfCheaper(idName, LeafPlan(alternative, Seq(predicate)))

          case _ =>
            planTable
        }
    }
}
