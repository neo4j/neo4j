/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression

object projection {

  import QueryPlanProducer._

  def apply(plan: QueryPlan, projectionsMap: Map[String, Expression])(implicit context: LogicalPlanningContext): QueryPlan = {
    val ids = plan.availableSymbols
    val projectAllCoveredIds = ids.map {
      case IdName(id) => id -> ast.Identifier(id)(null)
    }.toMap

    if (projectionsMap == projectAllCoveredIds)
      planStarProjection(plan, projectionsMap)
    else
      planRegularProjection(plan, projectionsMap)
  }
}
