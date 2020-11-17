/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.topDown

/**
 * Compresses the plan IDs so that they are consecutive numbers starting from 0.
 * Create a copy of the planning attributes with the new IDs.
 *
 * This is helpful for physical planning attributes that do not have to create so large arrays.
 * It also reduces the size of what we need to put into the query cache.
 */
case object CompressPlanIDs extends Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] {

  override def transform(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    val oldAttributes = from.planningAttributes
    val newAttributes = PlanningAttributes.newAttributes

    val newIdGen = new SequentialIdGen()
    val newPlan = from.logicalPlan.endoRewrite(topDown(Rewriter.lift {
      case lp: LogicalPlan =>
        val newLP = lp.copyPlanWithIdGen(newIdGen)
        oldAttributes.solveds.getOption(lp.id).foreach {
          newAttributes.solveds.set(newLP.id, _)
        }
        oldAttributes.cardinalities.getOption(lp.id).foreach {
          newAttributes.cardinalities.set(newLP.id, _)
        }
        oldAttributes.providedOrders.getOption(lp.id).foreach {
          newAttributes.providedOrders.set(newLP.id, _)
        }
        oldAttributes.leveragedOrders.getOption(lp.id).foreach {
          newAttributes.leveragedOrders.set(newLP.id, _)
        }
        newLP
    }))

    from
      .withMaybeLogicalPlan(Some(newPlan))
      .withNewPlanningAttributes(newAttributes)
  }

  override def name: String = "CompressPlanIDs"
}
