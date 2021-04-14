package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexMatch
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

object nodeIndexEndsWithScanPlanProvider extends AbstractNodeIndexScanPlanProvider {

  override def createPlans(indexMatches: Set[IndexMatch], hints: Set[Hint], argumentIds: Set[String], restrictions: LeafPlanRestrictions, context: LogicalPlanningContext): Set[LogicalPlan] = for {
    indexMatch <- indexMatches
    if isAllowedByRestrictions(indexMatch, restrictions) && indexMatch.indexDescriptor.properties.size == 1
    plan <- doCreatePlans(indexMatch, hints, argumentIds, context)
  } yield plan

  def doCreatePlans(indexMatch: IndexMatch, hints: Set[Hint], argumentIds: Set[String], context: LogicalPlanningContext): Set[LogicalPlan] = {
    indexMatch.propertyPredicates.flatMap { indexPredicate =>
      indexPredicate.predicate match {
        case endsWith: EndsWith =>
          val singlePredicateSet = indexMatch.predicateSet(Seq(indexPredicate), exactPredicatesCanGetValue = false)

          val plan = context.logicalPlanProducer.planNodeIndexEndsWithScan(
            idName = indexMatch.variableName,
            label = indexMatch.labelToken,
            properties = singlePredicateSet.indexedProperties(context),
            solvedPredicates = singlePredicateSet.allSolvedPredicates,
            solvedHint = singlePredicateSet.matchingHints(hints).find(_.spec.fulfilledByScan),
            valueExpr = endsWith.rhs,
            argumentIds = argumentIds,
            providedOrder = indexMatch.providedOrder,
            indexOrder = indexMatch.indexOrder,
            context = context,
          )
          Some(plan)

        case _ =>
          None
      }
    }.toSet
  }
}
