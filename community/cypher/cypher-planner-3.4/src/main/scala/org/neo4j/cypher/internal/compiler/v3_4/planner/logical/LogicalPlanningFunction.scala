/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.v3_4.expressions.Expression
import org.neo4j.cypher.internal.ir.v3_4.QueryGraph
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan

trait CandidateGenerator[T] extends ((T, QueryGraph, LogicalPlanningContext, Solveds, Cardinalities) => Seq[LogicalPlan])

trait PlanTransformer[-T] extends ((LogicalPlan, T, LogicalPlanningContext, Solveds, Cardinalities) => LogicalPlan)

trait CandidateSelector extends ProjectingSelector[LogicalPlan]

trait LeafPlanner extends ((QueryGraph, LogicalPlanningContext, Solveds, Cardinalities) => Seq[LogicalPlan])

object LeafPlansForVariable {
  def maybeLeafPlans(id: String, plans: Set[LogicalPlan]): Option[LeafPlansForVariable] =
    if (plans.isEmpty) None else Some(LeafPlansForVariable(id, plans))
}

case class LeafPlansForVariable(id: String, plans: Set[LogicalPlan]) {
  assert(plans.nonEmpty)
}

trait LeafPlanFromExpressions {
  def producePlanFor(predicates: Set[Expression], qg: QueryGraph, context: LogicalPlanningContext): Set[LeafPlansForVariable]
}

trait LeafPlanFromExpression extends LeafPlanFromExpressions {

  def producePlanFor(e: Expression, qg: QueryGraph, context: LogicalPlanningContext): Option[LeafPlansForVariable]


  override def producePlanFor(predicates: Set[Expression], qg: QueryGraph, context: LogicalPlanningContext): Set[LeafPlansForVariable] = {
    predicates.flatMap(p => producePlanFor(p, qg, context))
  }
}
