package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexMatch
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

/**
 * A component responsible for creating concrete leaf plans from IndexMatch:es
 */
trait NodeIndexPlanProvider {

  def createPlans(
    indexMatches: Set[IndexMatch],
    hints: Set[Hint],
    argumentIds: Set[String],
    restrictions: LeafPlanRestrictions,
    context: LogicalPlanningContext): Set[LogicalPlan]
}
