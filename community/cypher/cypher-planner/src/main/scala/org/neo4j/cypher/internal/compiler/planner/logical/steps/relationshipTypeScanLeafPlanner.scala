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

case class relationshipTypeScanLeafPlanner(skipIDs: Set[LogicalVariable]) extends LeafPlanner {

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

      case relationship @ PatternRelationship(rel, (_, _), _, Seq(typ), SimplePatternLength)
        if !shouldIgnore(relationship) =>
        context.staticComponents.planContext.relationshipTokenIndex.map { relationshipTokenIndex =>
          planHiddenSelectionAndRelationshipLeafPlan(
            queryGraph.argumentIds,
            relationship,
            context,
            planRelationshipTypeScan(
              rel,
              typ,
              _,
              _,
              _,
              queryGraph,
              interestingOrderConfig,
              relationshipTokenIndex.orderCapability,
              context
            )
          )
        }
      case _ => None
    }
  }

  private def planRelationshipTypeScan(
    variable: LogicalVariable,
    typ: RelTypeName,
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
    context.staticComponents.logicalPlanProducer.planRelationshipByTypeScan(
      variable,
      typ,
      patternForLeafPlan,
      originalPattern,
      hiddenSelections,
      hint(queryGraph, originalPattern),
      queryGraph.argumentIds,
      providedOrderFor(variable),
      context
    )
  }

  private def hint(queryGraph: QueryGraph, patternRelationship: PatternRelationship): Option[UsingScanHint] = {
    queryGraph.hints.collectFirst {
      case hint @ UsingScanHint(patternRelationship.variable, LabelOrRelTypeName(relTypeName))
        if relTypeName == patternRelationship.types.head.name => hint
    }
  }

}
