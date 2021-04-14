package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexMatch
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.QueryExpression

object nodeIndexSeekPlanProvider extends AbstractNodeIndexSeekPlanProvider {

  override def createPlans(indexMatches: Set[IndexMatch], hints: Set[Hint], argumentIds: Set[String], restrictions: LeafPlanRestrictions, context: LogicalPlanningContext): Set[LogicalPlan] = for {
    indexMatch <- indexMatches
    if isAllowedByRestrictions(indexMatch, restrictions)
    plan <- doCreatePlans(indexMatch, hints, argumentIds, context)
  } yield plan

  override protected def constructPlan(
    idName: String,
    label: LabelToken,
    properties: Seq[IndexedProperty],
    isUnique: Boolean,
    valueExpr: QueryExpression[Expression],
    hint: Option[UsingIndexHint],
    argumentIds: Set[String],
    providedOrder: ProvidedOrder,
    indexOrder: IndexOrder,
    context: LogicalPlanningContext,
    solvedPredicates: Seq[Expression],
    predicatesForCardinalityEstimation: Seq[Expression]
  ): LogicalPlan =
    if (isUnique) {
      context.logicalPlanProducer.planNodeUniqueIndexSeek(idName, label, properties, valueExpr, solvedPredicates, predicatesForCardinalityEstimation, hint, argumentIds, providedOrder, indexOrder, context)
    } else {
      context.logicalPlanProducer.planNodeIndexSeek(idName, label, properties, valueExpr, solvedPredicates, predicatesForCardinalityEstimation, hint, argumentIds, providedOrder, indexOrder, context)
    }
}

