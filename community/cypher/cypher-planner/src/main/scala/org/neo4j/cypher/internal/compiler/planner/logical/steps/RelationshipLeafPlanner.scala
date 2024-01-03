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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.equalsPredicate
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

/**
 * Helper functions for relationship leaf planners.
 */
object RelationshipLeafPlanner {

  @FunctionalInterface
  trait RelationshipLeafPlanProvider {

    /**
     * @param patternForLeafPlan the pattern to use for the leaf plan
     * @param originalPattern    the original pattern, as it appears in the query graph
     * @param hiddenSelections   selections that make the leaf plan solve the originalPattern instead
     * @return the leaf plan including the hidden selections
     */
    def getRelationshipLeafPlan(
      patternForLeafPlan: PatternRelationship,
      originalPattern: PatternRelationship,
      hiddenSelections: Seq[Expression]
    ): LogicalPlan
  }

  /**
   * Plan a hidden selection on top of a relationship leaf plan, if needed.
   * This is needed if either the start or end node is already bound, or if the start and end node is the same.
   *
   * @param argumentIds  ids of already bound entities, which are fed into this leaf plan as arguments
   * @param relationship the relationship
   * @param context      the context
   * @param relationshipLeafPlanProvider a RelationshipLeafPlanProvider
   * @return the leaf plan, with the correct hidden selections.
   */
  def planHiddenSelectionAndRelationshipLeafPlan(
    argumentIds: Set[LogicalVariable],
    relationship: PatternRelationship,
    context: LogicalPlanningContext,
    relationshipLeafPlanProvider: RelationshipLeafPlanProvider
  ): LogicalPlan = {
    val startNodeAndEndNodeIsSame = relationship.left == relationship.right
    val startOrEndNodeIsBound = relationship.coveredIds.intersect(argumentIds).nonEmpty
    if (!startOrEndNodeIsBound && !startNodeAndEndNodeIsSame) {
      relationshipLeafPlanProvider.getRelationshipLeafPlan(relationship, relationship, Seq.empty)
    } else if (startOrEndNodeIsBound) {
      // if start/end node variables are already bound, generate new variable names and plan a Selection after the seek
      val newRelationship = generateNewPatternRelationship(relationship, argumentIds, context)
      // For a pattern (a)-[r]-(b), nodePredicates will be something like `a = new_a AND b = new_B`,
      // where `new_a` and `new_b` are the newly generated variable names.
      // This case covers the scenario where (a)-[r]-(a), because the nodePredicate `a = new_a1 AND a = new_a2` implies `new_a1 = new_a2`
      val nodePredicates = buildNodePredicates(relationship.boundaryNodes, newRelationship.boundaryNodes)

      relationshipLeafPlanProvider.getRelationshipLeafPlan(newRelationship, relationship, nodePredicates)
    } else {
      // In the case where `startNodeAndEndNodeIsSame == true` we need to generate 1 new variable name for one side of the relationship
      // and plan a Selection after the seek so that both sides are the same
      val newRightNode = varFor(context.staticComponents.anonymousVariableNameGenerator.nextName)
      val nodePredicate = equalsPredicate(relationship.right, newRightNode)
      val newRelationship = relationship.copy(boundaryNodes = (relationship.left, newRightNode))
      relationshipLeafPlanProvider.getRelationshipLeafPlan(newRelationship, relationship, Seq(nodePredicate))
    }
  }

  /**
   * Generate new variable names for start and end node, but only for those nodes that are arguments.
   * Otherwise, return the same variable name.
   */
  private def generateNewStartEndNodes(
    oldNodes: (LogicalVariable, LogicalVariable),
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): (LogicalVariable, LogicalVariable) = {
    val (left, right) = oldNodes
    val newLeft =
      if (!argumentIds.contains(left)) left
      else varFor(context.staticComponents.anonymousVariableNameGenerator.nextName)
    val newRight =
      if (!argumentIds.contains(right)) right
      else varFor(context.staticComponents.anonymousVariableNameGenerator.nextName)
    (newLeft, newRight)
  }

  /**
   * Generate new variable names for start and end node of a PatternRelationship, but only for those nodes that are arguments.
   * Return a PatternRelationship with the updates nodes.
   */
  private def generateNewPatternRelationship(
    oldRelationship: PatternRelationship,
    argumentIds: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): PatternRelationship = {
    oldRelationship.copy(boundaryNodes = generateNewStartEndNodes(oldRelationship.boundaryNodes, argumentIds, context))
  }

  private def buildNodePredicates(
    oldNodes: (LogicalVariable, LogicalVariable),
    newNodes: (LogicalVariable, LogicalVariable)
  ): Seq[Equals] = {
    def pred(oldName: LogicalVariable, newName: LogicalVariable): Seq[Equals] = {
      if (oldName == newName) {
        Seq.empty
      } else {
        Seq(equalsPredicate(oldName, newName))
      }
    }

    val (oldLeft, oldRight) = oldNodes
    val (newLeft, newRight) = newNodes

    pred(oldLeft, newLeft) ++ pred(oldRight, newRight)
  }
}
