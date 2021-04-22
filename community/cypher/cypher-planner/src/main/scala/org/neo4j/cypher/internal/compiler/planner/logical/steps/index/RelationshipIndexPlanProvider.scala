package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.ast.Hint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

trait RelationshipIndexPlanProvider {
  def createPlans(indexMatches: Set[RelationshipIndexLeafPlanner.IndexMatch],
                  hints: Set[Hint],
                  argumentIds: Set[String],
                  restrictions: LeafPlanRestrictions,
                  context: LogicalPlanningContext): Set[LogicalPlan]

}
