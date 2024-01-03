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

import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.RelationshipLeafPlanner.planHiddenSelectionAndRelationshipLeafPlan
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

case class allRelationshipsScanLeafPlanner(skipIDs: Set[LogicalVariable]) extends LeafPlanner {

  override def apply(
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    def shouldIgnore(pattern: PatternRelationship): Boolean =
      queryGraph.argumentIds.contains(pattern.variable) ||
        skipIDs.contains(pattern.variable) ||
        skipIDs.contains(pattern.left) ||
        skipIDs.contains(pattern.right)
    queryGraph.patternRelationships.flatMap {

      case relationship @ PatternRelationship(rel, (_, _), _, types, SimplePatternLength)
        if !shouldIgnore(relationship) && types.isEmpty =>
        Some(planHiddenSelectionAndRelationshipLeafPlan(
          queryGraph.argumentIds,
          relationship,
          context,
          context.staticComponents.logicalPlanProducer.planAllRelationshipsScan(
            rel,
            _,
            _,
            _,
            queryGraph.argumentIds,
            context
          )
        ))

      case _ => None
    }
  }
}
