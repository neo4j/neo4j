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

import org.neo4j.cypher.internal.compiler.IndexLookupUnfulfillableNotification
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlansForVariable.maybeLeafPlans
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.compiler.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.planner.logical.{LeafPlanFromExpression, LeafPlanner, LeafPlansForVariable, LogicalPlanningContext}
import org.neo4j.cypher.internal.ir.{InterestingOrder, ProvidedOrder, QueryGraph}
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.LabelId
import org.neo4j.cypher.internal.v4_0.util.symbols._

object indexScanLeafPlanner extends LeafPlanner with LeafPlanFromExpression {

  override def producePlanFor(e: Expression, qg: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Option[LeafPlansForVariable] = {
    val lpp = context.logicalPlanProducer

    def onlyArgumentDependencies(expr: Expression): Boolean =
      expr.dependencies.map(_.name).forall(qg.argumentIds)

    e match {
      // MATCH (n:User) WHERE n.prop CONTAINS 'substring' RETURN n
      case predicate@Contains(prop@Property(Variable(name), _), expr) if onlyArgumentDependencies(expr) =>
        val plans = produce(name, qg, interestingOrder, prop, CTString, predicate,
                            lpp.planNodeIndexContainsScan(_, _, _, _, _, expr, _, _, interestingOrder, context), context)
        maybeLeafPlans(name, plans)

      // MATCH (n:User) WHERE n.prop ENDS WITH 'substring' RETURN n
      case predicate@EndsWith(prop@Property(Variable(name), _), expr) if onlyArgumentDependencies(expr) =>
        val plans = produce(name, qg, interestingOrder, prop, CTString, predicate,
                            lpp.planNodeIndexEndsWithScan(_, _, _, _, _, expr, _, _, interestingOrder, context), context)
        maybeLeafPlans(name, plans)

      // MATCH (n:User) WHERE exists(n.prop) RETURN n
      case AsPropertyScannable(scannable) =>
        val name = scannable.name

        val plans = produce(name, qg, interestingOrder, scannable.property, CTAny, scannable.expr, lpp.planNodeIndexScan(_, _, _, _, _, _, _, context), context)
        maybeLeafPlans(name, plans)

      // MATCH (n:User) with existence/node key constraint on :User(prop) or aggregation on n.prop
      case predicate@HasLabels(expr@Variable(name), labels) =>
        val label = labels.head
        val labelName = label.name
        val constrainedProps = context.planContext.getPropertiesWithExistenceConstraint(labelName)

        // Can't currently handle aggregation on more than one variable
        val aggregatedProps: Set[String] =
          if (context.aggregatingProperties.forall(prop => prop._1.equals(name)))
            context.aggregatingProperties.map { prop => prop._2 }
          else Set.empty

        // Can't currently handle aggregation on more than one property
        val properties = if(aggregatedProps.size == 1) constrainedProps.union(aggregatedProps) else constrainedProps

        val plans: Set[LogicalPlan] = properties.flatMap(prop => {
          val property = Property(expr, PropertyKeyName(prop)(predicate.position))(predicate.position)
          produceForConstraintOrAggregation(name, qg, interestingOrder, property, CTAny, predicate, lpp.planNodeIndexScan(_, _, _, _, _, _, _, context), context)
        })

        maybeLeafPlans(name, plans)

      case _ =>
        None
    }
  }

  override def apply(qg: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Seq[LogicalPlan] = {
    val resultPlans = qg.selections.flatPredicates.flatMap(e => producePlanFor(e, qg, interestingOrder, context).toSeq.flatMap(_.plans))

    if (resultPlans.isEmpty) {
      DynamicPropertyNotifier.process(findNonScannableVariables(qg.selections.flatPredicates, context), IndexLookupUnfulfillableNotification, qg, context)
    }

    resultPlans
  }

  private def findNonScannableVariables(predicates: Seq[Expression], context: LogicalPlanningContext) =
    predicates.flatMap {
      case AsDynamicPropertyNonScannable(nonScannableId) if context.semanticTable.isNode(nonScannableId) =>
        Some(nonScannableId)
      case AsStringRangeNonSeekable(nonScannableId) if context.semanticTable.isNode(nonScannableId) =>
        Some(nonScannableId)
      case _ =>
        None
    }.toSet

  type PlanProducer = (String, LabelToken, Seq[IndexedProperty], Seq[Expression], Option[UsingIndexHint], Set[String], ProvidedOrder) => LogicalPlan

  private def produce(variableName: String,
                      qg: QueryGraph,
                      interestingOrder: InterestingOrder,
                      property: LogicalProperty,
                      propertyType: CypherType,
                      predicate: Expression,
                      planProducer: PlanProducer,
                      context: LogicalPlanningContext): Set[LogicalPlan] = {
    if (qg.argumentIds.contains(variableName)) Set.empty
    else {
      val semanticTable = context.semanticTable
      val labelPredicates: Map[String, Set[HasLabels]] = qg.selections.labelPredicates

      for (labelPredicate <- labelPredicates.getOrElse(variableName, Set.empty);
           labelName <- labelPredicate.labels;
           labelId <- semanticTable.id(labelName);
           indexDescriptor <- context.planContext.indexGetForLabelAndProperties(labelName.name, Seq(property.propertyKey.name))
           )
        yield {
          produceInner(variableName, qg, interestingOrder, property, propertyType, predicate, planProducer, semanticTable, labelPredicate, labelName, labelId, indexDescriptor)
        }
    }
  }

  private def produceForConstraintOrAggregation(variableName: String,
                                                qg: QueryGraph,
                                                interestingOrder: InterestingOrder,
                                                property: LogicalProperty,
                                                propertyType: CypherType,
                                                predicate: HasLabels,
                                                planProducer: PlanProducer,
                                                context: LogicalPlanningContext): Option[LogicalPlan] = {
    val semanticTable = context.semanticTable
    val labelName = predicate.labels.head
    semanticTable.id(labelName) match {
      case Some(labelId) =>
        val maybePropId = context.semanticTable.id(property.propertyKey)
        val maybeIndexDescriptor = context.planContext.indexGetForLabelAndProperties(labelName.name, Seq(property.propertyKey.name))
        (maybeIndexDescriptor, maybePropId) match {
          case (Some(indexDescriptor), Some(_)) =>
            Some(produceInner(variableName, qg, interestingOrder, property, propertyType, predicate, planProducer, semanticTable, predicate, labelName, labelId, indexDescriptor))
          case _ =>
            None
        }
      case _ => None
    }
  }

  private def produceInner(variableName: String,
                           qg: QueryGraph,
                           interestingOrder: InterestingOrder,
                           property: LogicalProperty,
                           propertyType: CypherType,
                           predicate: Expression,
                           planProducer: PlanProducer,
                           semanticTable: SemanticTable,
                           labelPredicate: HasLabels,
                           labelName: LabelName,
                           labelId: LabelId,
                           indexDescriptor: IndexDescriptor) = {
    val hint = qg.hints.collectFirst {
      case hint@UsingIndexHint(Variable(`variableName`), `labelName`, properties, spec)
        if spec.fulfilledByScan && properties.map(_.name) == Seq(property.propertyKey.name) => hint
    }
    // Index scan is always on just one property
    val getValueBehavior = indexDescriptor.valueCapability(Seq(propertyType)).head
    val indexProperty = plans.IndexedProperty(PropertyKeyToken(property.propertyKey, semanticTable.id(property.propertyKey).head), getValueBehavior)
    val orderProperty = Property(property.map, property.propertyKey)(property.position)
    val providedOrder = ResultOrdering.withIndexOrderCapability(interestingOrder, Seq(orderProperty), Seq(propertyType), indexDescriptor.orderCapability)

    val labelToken = LabelToken(labelName, labelId)
    val predicates = Seq(predicate, labelPredicate)
    planProducer(variableName, labelToken, Seq(indexProperty), predicates, hint, qg.argumentIds, providedOrder)
  }
}
