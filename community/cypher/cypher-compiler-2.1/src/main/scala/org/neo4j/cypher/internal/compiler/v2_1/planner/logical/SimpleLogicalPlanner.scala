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

import org.neo4j.cypher.internal.compiler.v2_1.planner.{SemanticTable, CantHandleQueryException, CardinalityEstimator, QueryGraph}
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.ast.HasLabels
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import scala.collection.immutable.HashMap

object SimpleLogicalPlanner {
  case class LeafPlan(plan: LogicalPlan, solvedPredicates: Seq[Expression])

  case class LeafPlanTable(table: Map[IdName, LeafPlan] = new HashMap) {
    def updateIfCheaper(id: IdName, alternative: LeafPlan) = {
      val bestCost = table.get(id).map(_.plan.cardinality).getOrElse(Int.MaxValue)
      val cost = alternative.plan.cardinality

      if (cost < bestCost)
        LeafPlanTable(table.updated(id, alternative))
      else
        this
    }

    def bestLeafPlan = {
      if (table.size > 1)
        throw new CantHandleQueryException

      if (table.isEmpty) None else Some(table.values.toSeq.minBy(_.plan.cardinality))
    }
  }
}

// TODO: check if we can seek using unique index
case class SimpleLogicalPlanner(estimator: CardinalityEstimator) extends LogicalPlanner {
  import SimpleLogicalPlanner.{LeafPlan, LeafPlanTable}

  val projectionPlanner = new ProjectionPlanner

  def plan(qg: QueryGraph, semanticTable: SemanticTable)(implicit planContext: PlanContext): LogicalPlan = {
    val predicates = qg.selections.flatPredicates
    val labelPredicateMap = qg.selections.labelPredicates

    val bestLeafPlan =
      LeafPlanTable()
        .introduceIdSeekPlans(predicates, semanticTable.isRelationship)
        .introduceIndexSeekPlans(predicates, labelPredicateMap)
        .introduceIndexScanPlans(predicates, labelPredicateMap)
        .introduceLabelScanPlans(qg, labelPredicateMap)
        .introduceAllNodesScanPlans(qg)
        .bestLeafPlan

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

  private implicit class LeafPlanTableBuilder(planTable: LeafPlanTable)(implicit planContext: PlanContext) {
    def introduceIdSeekPlans(predicates: Seq[Expression], isRelationship: Identifier => Boolean) =
      predicates.foldLeft(planTable) {
        (planTable, expression) =>
          expression match {
            // id(n) = value
            case Equals(FunctionInvocation(Identifier("id"), _, IndexedSeq(id@Identifier(identName))), ConstantExpression(idExpr)) =>

              val idName = IdName(identName)
              val alternative =
                if (isRelationship(id))
                  RelationshipByIdSeek(idName, idExpr, estimator.estimateRelationshipByIdSeek())
                else
                  NodeByIdSeek(idName, idExpr, estimator.estimateNodeByIdSeek())

              planTable.updateIfCheaper(idName, LeafPlan(alternative, Seq(expression)))
            case _ =>
              planTable
          }
      }

    def introduceIndexSeekPlans(predicates: Seq[Expression], labelPredicateMap: Map[IdName, Set[HasLabels]]) =
      predicates.foldLeft(planTable) {
        (planTable, expression) =>
          expression match {
            // n.prop = value
            case Equals(Property(identifier@Identifier(name), propertyKey), ConstantExpression(valueExpr)) if propertyKey.id.isDefined =>
              val idName = IdName(name)
              val propertyKeyId = propertyKey.id.get
              val labelPredicates = labelPredicateMap.getOrElse(idName, Set.empty)
              labelPredicates.foldLeft(planTable) {
                (planTable, hasLabels) =>
                  hasLabels.labels.foldLeft(planTable) {
                    (planTable, labelName) =>
                      labelName.id match {
                        case Some(labelId)
                          if planContext.uniqueIndexesGetForLabel(labelId.id).exists(_.getPropertyKeyId == propertyKeyId.id) =>

                          val alternative = NodeIndexSeek(idName, labelId, propertyKeyId, valueExpr,
                            estimator.estimateNodeIndexScan(labelId, propertyKeyId))
                          planTable.updateIfCheaper(idName, LeafPlan(alternative, Seq(expression, hasLabels)))
                        case _ =>
                          planTable
                      }
                  }
              }
            case _ =>
              planTable
          }
      }

    def introduceIndexScanPlans(predicates: Seq[Expression], labelPredicateMap: Map[IdName, Set[HasLabels]]) =
      predicates.foldLeft(planTable) {
        (planTable, expression) =>
          expression match {
            // n.prop = value
            case Equals(Property(identifier@Identifier(name), propertyKey), ConstantExpression(valueExpr)) if propertyKey.id.isDefined =>
              val idName = IdName(name)
              val propertyKeyId = propertyKey.id.get
              val labelPredicates = labelPredicateMap.getOrElse(idName, Set.empty)
              labelPredicates.foldLeft(planTable) {
                (planTable, hasLabels) =>
                  hasLabels.labels.foldLeft(planTable) {
                    (planTable, labelName) =>
                      labelName.id match {
                        case Some(labelId)
                          if planContext.indexesGetForLabel(labelId.id).exists(_.getPropertyKeyId == propertyKeyId.id) =>

                          val alternative = NodeIndexScan(idName, labelId, propertyKeyId, valueExpr,
                            estimator.estimateNodeIndexScan(labelId, propertyKeyId))
                          planTable.updateIfCheaper(idName, LeafPlan(alternative, Seq(expression, hasLabels)))
                        case _ =>
                          planTable
                      }
                  }
              }
            case _ =>
              planTable
          }
      }

    def introduceAllNodesScanPlans(qg: QueryGraph) =
      qg.identifiers.foldLeft(planTable) {
        (planTable, idName) =>
          val cost = estimator.estimateAllNodes()
          planTable.updateIfCheaper(idName, LeafPlan(AllNodesScan(idName, cost), Seq()))
      }

    def introduceLabelScanPlans(qg: QueryGraph, labelPredicateMap: Map[IdName, Set[HasLabels]]) =
      qg.identifiers.foldLeft(planTable) {
        (planTable, idName) =>
          labelPredicateMap.getOrElse(idName, Set.empty).foldLeft(planTable) {
            (planTable, hasLabels) =>
              hasLabels.labels.foldLeft(planTable) {
                (planTable, labelName) =>
                  val cost = estimator.estimateNodeByLabelScan(labelName.id)
                  val plan = NodeByLabelScan(idName, labelName.toEither(), cost)
                  planTable.updateIfCheaper(idName, LeafPlan(plan, Seq(hasLabels)))
              }
          }
      }
  }
}

