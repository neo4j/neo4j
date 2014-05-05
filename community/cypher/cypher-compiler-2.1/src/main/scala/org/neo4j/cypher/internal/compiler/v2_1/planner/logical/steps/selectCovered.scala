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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{SelectionPlan, QueryPlan, Selection}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{PlanTransformer, LogicalPlanContext}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{PatternExpression, Expression}

object selectCovered extends PlanTransformer {
  def apply(plan: QueryPlan)(implicit context: LogicalPlanContext): QueryPlan = {
    val qg = context.queryGraph
    val coveredIds = plan.coveredIds

    val predicates: Seq[Expression] = qg.selections.predicatesGiven(coveredIds).filter {
      case predicate => !plan.solved.selections.contains(predicate) && !containsPatternPredicates(predicate)
    }

    if (predicates.isEmpty)
      plan
    else {
      SelectionPlan(predicates, plan)
    }
  }

  private def containsPatternPredicates(e:Expression) = e.exists {
    case _:PatternExpression => true
  }
}
