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

import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.compiler.planner.logical.steps.RelationshipLeafPlanner.planHiddenSelectionAndRelationshipLeafPlan
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability

case class unionRelationshipTypeScanLeafPlanner(skipIDs: Set[LogicalVariable]) extends LeafPlanner {

  override def apply(
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    def shouldIgnore(pattern: PatternRelationship) =
      queryGraph.argumentIds.contains(pattern.variable) ||
        skipIDs.contains(pattern.variable) ||
        skipIDs.contains(pattern.left) ||
        skipIDs.contains(pattern.right)

    queryGraph.patternRelationships.flatMap {
      case relationship @ PatternRelationship(rel, (_, _), _, types, SimplePatternLength)
        if types.distinct.length > 1 && !shouldIgnore(relationship) =>
        context.staticComponents.planContext.relationshipTokenIndex.flatMap { relTokenIndex =>
          // UnionRelationshipTypeScan relies on ordering, so we can only use this plan if the relTokenIndex is ordered.
          if (relTokenIndex.orderCapability == IndexOrderCapability.BOTH) {
            val plan = planHiddenSelectionAndRelationshipLeafPlan(
              queryGraph.argumentIds,
              relationship,
              context,
              planUnionRelationshipTypeScan(
                rel,
                types,
                _,
                _,
                _,
                queryGraph,
                interestingOrderConfig,
                relTokenIndex.orderCapability,
                context
              )
            )
            Some(plan)
          } else {
            None
          }
        }
      case _ => None
    }
  }

  private def planUnionRelationshipTypeScan(
    variable: LogicalVariable,
    types: Seq[RelTypeName],
    patternForLeafPlan: PatternRelationship,
    originalPattern: PatternRelationship,
    hiddenSelections: Seq[Expression],
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    indexOrderCapability: IndexOrderCapability,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    def providedOrderFor = ResultOrdering.providedOrderForRelationshipTypeScan(
      interestingOrderConfig.orderToSolve,
      _,
      indexOrderCapability,
      context.providedOrderFactory
    )
    context.staticComponents.logicalPlanProducer.planUnionRelationshipByTypeScan(
      variable,
      types,
      patternForLeafPlan,
      originalPattern,
      hiddenSelections,
      hints(queryGraph, originalPattern),
      queryGraph.argumentIds,
      providedOrderFor(variable),
      context
    )
  }

  private def hints(queryGraph: QueryGraph, patternRelationship: PatternRelationship): Seq[UsingScanHint] = {
    queryGraph.hints.toSeq.collect {
      case hint @ UsingScanHint(patternRelationship.variable, LabelOrRelTypeName(relTypeName))
        if patternRelationship.types.map(_.name).contains(relTypeName) => hint
    }
  }

}
