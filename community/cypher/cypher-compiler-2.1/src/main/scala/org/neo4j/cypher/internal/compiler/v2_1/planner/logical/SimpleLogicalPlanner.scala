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

import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext

object SimpleLogicalPlanner {

  trait LeafPlanner {
    self: LeafPlanner =>

    def apply(planTable: LeafPlanTable)(implicit context: LogicalPlanContext): LeafPlanTable

    def andThen(other: LeafPlanner): LeafPlanner =
      new LeafPlanner() {
        def apply(planTable: LeafPlanTable)(implicit context: LogicalPlanContext) = other(self.apply(planTable))
      }
  }

  case class LeafPlan(plan: LogicalPlan, solvedPredicates: Seq[Expression])

  case class LeafPlanTable(table: Map[IdName, LeafPlan] = Map.empty) {
    def updateIfCheaper(id: IdName, alternative: LeafPlan): LeafPlanTable = {
      val bestCost = table.get(id).map(_.plan.cardinality).getOrElse(Int.MaxValue)
      val cost = alternative.plan.cardinality

      if (cost < bestCost)
        LeafPlanTable(table.updated(id, alternative))
      else
        this
    }

    def bestLeafPlan: Option[LeafPlan] = {
      if (table.size > 1)
        throw new CantHandleQueryException

      if (table.isEmpty) None else Some(table.values.toSeq.minBy(_.plan.cardinality))
    }
  }
}

case class LogicalPlanContext(planContext: PlanContext, estimator: CardinalityEstimator)

case class SimpleLogicalPlanner(estimator: CardinalityEstimator) extends LogicalPlanner {
  import SimpleLogicalPlanner.LeafPlanTable

  val projectionPlanner = new ProjectionPlanner

  def plan(qg: QueryGraph, semanticTable: SemanticTable)(implicit planContext: PlanContext): LogicalPlan = {
    val predicates: Seq[Expression] = qg.selections.flatPredicates
    val labelPredicateMap = qg.selections.labelPredicates

    implicit val context = LogicalPlanContext(planContext, estimator)

    val leafPlanners =
      idSeekLeafPlanner(predicates, semanticTable.isRelationship) andThen
      indexSeekLeafPlanner(predicates, labelPredicateMap) andThen
      indexScanLeafPlanner(predicates, labelPredicateMap) andThen
      labelScanLeafPlanner(qg, labelPredicateMap) andThen
      allNodesLeafPlanner(qg)

    val bestLeafPlan = leafPlanners(LeafPlanTable()).bestLeafPlan

    val bestPlan = bestLeafPlan match {
      case Some(leafPlan) =>
        // TODO: to be replace with a selection-plan when we support that
        if (!qg.selections.unsolvedPredicates(leafPlan.solvedPredicates).isEmpty)
          throw new CantHandleQueryException
        leafPlan.plan
      case _ =>
        SingleRow()
    }

    projectionPlanner.amendPlan(qg, bestPlan)
  }
}

