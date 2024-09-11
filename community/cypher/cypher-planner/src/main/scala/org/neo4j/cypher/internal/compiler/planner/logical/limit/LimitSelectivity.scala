/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.limit

import org.neo4j.cypher.internal.compiler.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.compiler.planner.logical.CardinalityCostModel
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.util.Selectivity

import scala.annotation.tailrec

object LimitSelectivity {

  def forAllParts(query: SinglePlannerQuery, context: LogicalPlanningContext): List[Selectivity] = {
    @tailrec
    def recurse(
      query: Option[SinglePlannerQuery],
      parentLimitSelectivity: Selectivity,
      acc: List[Selectivity]
    ): List[Selectivity] = {
      query match {
        case None => acc
        case Some(query) =>
          val lastPartSelectivity = forLastPart(query, context, parentLimitSelectivity)
          recurse(query.withoutLast, lastPartSelectivity, lastPartSelectivity +: acc)
      }
    }

    recurse(Some(query), Selectivity.ONE, List.empty)
  }

  def forLastPart(
    query: SinglePlannerQuery,
    context: LogicalPlanningContext,
    parentLimitSelectivity: Selectivity
  ): Selectivity = {
    if (!query.readOnly) {
      Selectivity.ONE
    } else {
      query.lastQueryHorizon match {
        case _: AggregatingQueryProjection =>
          Selectivity.ONE

        case proj: QueryProjection if proj.queryPagination.limit.isDefined =>
          val queryWithoutLimit = query.updateTailOrSelf(_.updateQueryProjection(_ =>
            proj.withPagination(proj.queryPagination.withLimit(None))
          ))
          val cardinalityModel = context.staticComponents.metrics.cardinality(
            _: PlannerQuery,
            context.plannerState.input.labelInfo,
            context.plannerState.input.relTypeInfo,
            context.semanticTable,
            context.plannerState.indexCompatiblePredicatesProviderContext
          )

          val cardinalityWithoutLimit = cardinalityModel(queryWithoutLimit)
          val cardinalityWithLimit = cardinalityModel(query)

          CardinalityCostModel.limitingPlanSelectivity(
            cardinalityWithoutLimit,
            cardinalityWithLimit,
            parentLimitSelectivity
          )

        case ProcedureCallProjection(ResolvedCall(signature, _, _, _, _, _, _)) if signature.eager => Selectivity.ONE

        case _ => parentLimitSelectivity
      }
    }
  }
}
