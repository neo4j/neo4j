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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/*
A join on given variable is similar to a logical AND - any predicates evaluated on the LHS will in effect
also be applied to the output of the join. This means that evaluating the same predicate on the RHS is redundant.

This rewriter finds predicates on the join variables, and removes any predicates on the RHS that already
exist on the LHS.
 */
case class predicateRemovalThroughJoins(
  solveds: Solveds,
  cardinalities: Cardinalities,
  attributes: Attributes[LogicalPlan]
) extends Rewriter with BottomUpMergeableRewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  override val innerRewriter: Rewriter = Rewriter.lift {
    case n @ NodeHashJoin(nodeIds, lhs, rhs @ Selection(Ands(rhsPredicates), rhsLeaf)) =>
      val lhsPredicates =
        predicatesDependingOnTheJoinIds(solveds.get(lhs.id).asSinglePlannerQuery.lastQueryGraph, nodeIds)
      val newPredicate = rhsPredicates.filterNot(lhsPredicates)

      if (newPredicate.isEmpty) {
        NodeHashJoin(nodeIds, lhs, rhsLeaf)(SameId(n.id))
      } else {
        val newRhsPlannerQuery =
          solveds.get(rhsLeaf.id).asSinglePlannerQuery.amendQueryGraph(_.addPredicates(newPredicate.toArray: _*))
        val newSelection = Selection(Ands(newPredicate)(newPredicate.head.position), rhsLeaf)(attributes.copy(rhs.id))
        solveds.set(newSelection.id, newRhsPlannerQuery)
        // NOTE: This overestimates cardinality of the new Selection, because it has fewer predicates than the original Selection.
        //       To get it right, we should recompute cardinality of newRhsPlannerQuery
        cardinalities.copy(rhsLeaf.id, newSelection.id)
        NodeHashJoin(nodeIds, lhs, newSelection)(SameId(n.id))
      }
  }

  private val instance: Rewriter = bottomUp(innerRewriter)

  private def predicatesDependingOnTheJoinIds(qg: QueryGraph, nodeIds: Set[LogicalVariable]): Set[Expression] =
    qg.selections.predicates.filter(p => (p.dependencies intersect nodeIds) == nodeIds).map(_.expr)
}
