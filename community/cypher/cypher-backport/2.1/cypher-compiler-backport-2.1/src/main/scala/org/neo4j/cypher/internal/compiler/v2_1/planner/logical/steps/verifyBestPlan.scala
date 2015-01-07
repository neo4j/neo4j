/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.planner.{QueryGraph, PlannerQuery, CantHandleQueryException}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.QueryPlan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{PlanTransformer, LogicalPlanningContext}
import org.neo4j.cypher.internal.compiler.v2_1.ast.PatternExpression

object verifyBestPlan extends PlanTransformer[PlannerQuery] {
  def apply(plan: QueryPlan, expected: PlannerQuery)(implicit context: LogicalPlanningContext, subQueryLookupTable: Map[PatternExpression, QueryGraph]): QueryPlan = {
    val constructed = plan.solved
    if (expected != constructed)
      throw new CantHandleQueryException(s"Expected \n$expected \n\n\nInstead, got: \n$constructed")
    plan
  }
}
