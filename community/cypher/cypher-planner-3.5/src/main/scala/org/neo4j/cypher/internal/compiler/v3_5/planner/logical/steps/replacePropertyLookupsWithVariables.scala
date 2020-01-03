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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.v3_5.phases.PlannerContext
import org.neo4j.cypher.internal.v3_5.logical.plans.CachedNodeProperty
import org.neo4j.cypher.internal.v3_5.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_5.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v3_5.expressions.Property
import org.neo4j.cypher.internal.v3_5.frontend.phases.Transformer
import org.neo4j.cypher.internal.v3_5.util.bottomUp
import org.neo4j.cypher.internal.v3_5.util.Rewriter
import org.neo4j.cypher.internal.v3_5.util.topDown

/**
  * A logical plan rewriter that also changes the semantic table (thus a Transformer).
  *
  * It traverses the plan and swaps property lookups for cached node properties where possible.
  */
case object replacePropertyLookupsWithVariables extends Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] {

  /**
    * Rewrites any object to replace property lookups with variables, if they are available.
    * Registers these new variables with the given semantic table and returns a copy
    * of that semantic table where the new variables are known.
    */
  private def rewrite(inputPlan: LogicalPlan, semanticTable: SemanticTable): (LogicalPlan, SemanticTable) = {
    var currentTypes = semanticTable.types

    def rewriteProperties(plan: LogicalPlan): LogicalPlan = {
      // We have to use the incoming available properties. Not the outgoing.
      val availableProperties =
        plan.lhs.fold(Map.empty[Property, CachedNodeProperty])(_.availableCachedNodeProperties) ++
          plan.rhs.fold(Map.empty[Property, CachedNodeProperty])(_.availableCachedNodeProperties)

      val propertyRewriter = topDown(Rewriter.lift {
        case property: Property if availableProperties.contains(property) =>
          val newVar = availableProperties(property)
          // Register the new variables in the semantic table
          currentTypes.get(property) match {
            case None => // I don't like this. We have to make sure we retain the type from semantic analysis
            case Some(currentType) =>
              currentTypes = currentTypes.updated(newVar, currentType)
          }
          newVar
      },
        // Don't rewrite deeper than the next logical plan. It will have different availablePropertyVariables
        stopAtNextLogicalPlan(plan))

      propertyRewriter(plan).asInstanceOf[LogicalPlan]
    }


    val planRewriter = bottomUp(Rewriter.lift {
      case plan: LogicalPlan => rewriteProperties(plan)
    })

    val rewritten = planRewriter(inputPlan)
    val newSemanticTable = if (currentTypes == semanticTable.types) semanticTable else semanticTable.copy(types = currentTypes)
    (rewritten.asInstanceOf[LogicalPlan], newSemanticTable)
  }

  private def stopAtNextLogicalPlan(currentPlan: LogicalPlan)(item: AnyRef): Boolean =
    item != currentPlan && item.isInstanceOf[LogicalPlan]

  override def transform(from: LogicalPlanState, context: PlannerContext): LogicalPlanState = {
    val (plan, table) = rewrite(from.logicalPlan, from.semanticTable())
    from.withMaybeLogicalPlan(Some(plan)).withSemanticTable(table)
  }

  override def name: String = "replacePropertyLookupsWithVariables"
}
