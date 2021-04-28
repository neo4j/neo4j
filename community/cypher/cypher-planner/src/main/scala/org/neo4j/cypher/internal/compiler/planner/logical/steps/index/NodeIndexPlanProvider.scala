/*
 * Copyright (c) "Neo4j"
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

/**
 * This is a temporary hack to allow the Scan provider to check if the Seek provider has created a plan
 */
trait NodeIndexPlanProviderPeek {
  def wouldCreatePlan(
    indexMatch: IndexMatch,
    hints: Set[Hint],
    argumentIds: Set[String],
    restrictions: LeafPlanRestrictions,
    context: LogicalPlanningContext): Boolean
}

object NodeIndexPlanProviderPeek {
  val default: NodeIndexPlanProviderPeek = (_: IndexMatch, _: Set[Hint], _: Set[String], _: LeafPlanRestrictions, _: LogicalPlanningContext) => false
}
