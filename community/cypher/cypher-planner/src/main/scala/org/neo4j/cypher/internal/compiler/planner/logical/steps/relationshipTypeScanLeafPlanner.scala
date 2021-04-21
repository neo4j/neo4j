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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.LogicalPlan

case class relationshipTypeScanLeafPlanner(skipIDs: Set[String]) extends LeafPlanner {

  override def apply(queryGraph: QueryGraph, interestingOrderConfig: InterestingOrderConfig, context: LogicalPlanningContext): Seq[LogicalPlan] = {
    def shouldIgnore(pattern: PatternRelationship) =
      !context.planContext.canLookupRelationshipsByType ||
      queryGraph.argumentIds.contains(pattern.name) ||
      queryGraph.argumentIds.contains(pattern.nodes._1) ||
      queryGraph.argumentIds.contains(pattern.nodes._2) ||
      skipIDs.contains(pattern.name) ||
      skipIDs.contains(pattern.nodes._1) ||
      skipIDs.contains(pattern.nodes._2) ||
        queryGraph.hints.exists {
          case UsingScanHint(v, _) => v.name == pattern.nodes._1 || v.name == pattern.nodes._2
          case UsingJoinHint(vs) => vs.exists(v => v.name == pattern.nodes._1 || v.name == pattern.nodes._2)
          case _ => false
        }

    def providedOrderFor = ResultOrdering.providedOrderForRelationshipTypeScan(interestingOrderConfig.orderToSolve, _)

    queryGraph.patternRelationships.flatMap {

      case p@PatternRelationship(name, (_, _), _, Seq(typ), SimplePatternLength) if !shouldIgnore(p) =>
        Some(context.logicalPlanProducer.planRelationshipByTypeScan(name, typ, p, queryGraph.argumentIds, providedOrderFor(name), context))

      case _ => None
    }.toIndexedSeq
  }

}
