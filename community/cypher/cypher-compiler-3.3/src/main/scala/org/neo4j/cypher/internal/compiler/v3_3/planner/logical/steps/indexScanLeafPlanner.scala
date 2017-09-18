/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.LeafPlansForVariable.maybeLeafPlans
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{LeafPlanFromExpression, LeafPlanner, LeafPlansForVariable, LogicalPlanningContext}
import org.neo4j.cypher.internal.frontend.v3_4.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.notification.IndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.ir.v3_4.{IdName, QueryGraph}
import org.neo4j.cypher.internal.v3_3.logical.plans.{AsDynamicPropertyNonScannable, AsStringRangeNonSeekable, LogicalPlan}

object indexScanLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph)(implicit context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    implicit val semanticTable = context.semanticTable
    val lpp = context.logicalPlanProducer

    e match {
      // MATCH (n:User) WHERE n.prop CONTAINS 'substring' RETURN n
      case predicate@Contains(prop@Property(Variable(name), propertyKey), expr) =>
        val plans = produce(name, propertyKey.name, qg, prop, predicate, lpp.planNodeIndexContainsScan(_, _, _, _, _, expr, _))
        maybeLeafPlans(name, plans)

      // MATCH (n:User) WHERE n.prop ENDS WITH 'substring' RETURN n
      case predicate@EndsWith(prop@Property(Variable(name), propertyKey), expr) =>
        val plans = produce(name, propertyKey.name, qg, prop, predicate, lpp.planNodeIndexEndsWithScan(_, _, _, _, _, expr, _))
        maybeLeafPlans(name, plans)

      // MATCH (n:User) WHERE exists(n.prop) RETURN n
      case predicate@AsPropertyScannable(scannable) =>
        val name = scannable.name
        val propertyKeyName = scannable.propertyKey.name

        val plans = produce(name, propertyKeyName, qg, scannable.property, scannable.expr, lpp.planNodeIndexScan)
        maybeLeafPlans(name, plans)

      case _ =>
        None
    }
  }

  override def apply(qg: QueryGraph)(implicit context: LogicalPlanningContext): Seq[LogicalPlan] = {
    val resultPlans = qg.selections.flatPredicates.flatMap(e => producePlanFor(e, qg).toSeq.flatMap(_.plans))

    if (resultPlans.isEmpty) {
      DynamicPropertyNotifier.process(findNonScannableVariables(qg.selections.flatPredicates), IndexLookupUnfulfillableNotification, qg)
    }

    resultPlans
  }

  private def findNonScannableVariables(predicates: Seq[Expression])(implicit context: LogicalPlanningContext) =
    predicates.flatMap {
      case predicate@AsDynamicPropertyNonScannable(nonScannableId) if context.semanticTable.isNode(nonScannableId) =>
        Some(nonScannableId)
      case predicate@AsStringRangeNonSeekable(nonScannableId) if context.semanticTable.isNode(nonScannableId) =>
        Some(nonScannableId)
      case _ =>
        None
    }.toSet

  type PlanProducer = (IdName, LabelToken, PropertyKeyToken, Seq[Expression], Option[UsingIndexHint], Set[IdName]) => LogicalPlan

  private def produce(variableName: String, propertyKeyName: String, qg: QueryGraph, property: Property,
                      predicate: Expression, planProducer: PlanProducer)
                     (implicit context: LogicalPlanningContext, semanticTable: SemanticTable): Set[LogicalPlan] = {
    val labelPredicates: Map[IdName, Set[HasLabels]] = qg.selections.labelPredicates
    val idName = IdName(variableName)

    for (labelPredicate <- labelPredicates.getOrElse(idName, Set.empty);
         labelName <- labelPredicate.labels;
         indexDescriptor <- findIndexesFor(labelName.name, propertyKeyName);
         labelId <- labelName.id)
      yield {
        val hint = qg.hints.collectFirst {
          case hint@UsingIndexHint(Variable(`variableName`), `labelName`, properties)
            if properties.map(_.name) == Seq(propertyKeyName) => hint
        }
        val keyToken = PropertyKeyToken(property.propertyKey, property.propertyKey.id.head)
        val labelToken = LabelToken(labelName, labelId)
        val predicates = Seq(predicate, labelPredicate)
        planProducer(idName, labelToken, keyToken, predicates, hint, qg.argumentIds)
      }
  }

  private def findIndexesFor(label: String, property: String)(implicit context: LogicalPlanningContext) =
    context.planContext.indexGet(label, Seq(property)) orElse context.planContext.uniqueIndexGet(label, Seq(property))
}
