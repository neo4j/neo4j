package org.neo4j.cypher.internal.compiler.planner.logical.steps.index

import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.NodeIndexPlanner.IndexMatch

abstract class AbstractNodeIndexScanPlanProvider extends NodeIndexPlanProvider {

  def isAllowedByRestrictions(indexMatch: IndexMatch, restrictions: LeafPlanRestrictions): Boolean =
    !restrictions.symbolsThatShouldOnlyUseIndexSeekLeafPlanners.contains(indexMatch.variableName)

}
