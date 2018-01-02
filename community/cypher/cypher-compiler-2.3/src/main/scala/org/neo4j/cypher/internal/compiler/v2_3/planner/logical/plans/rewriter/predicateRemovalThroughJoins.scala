/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, QueryGraph}
import org.neo4j.cypher.internal.frontend.v2_3.{Rewriter, bottomUp}


/*
A join on given identifier is similar to a logical AND - any predicates evaluated on the LHS will in effect
also be applied to the output of the join. This means that evaluating the same predicate on the RHS is redundant.

This rewriters finds predicates on the join identifiers, and removes any predicates on the RHS that already
exist on the LHS.
 */
case object predicateRemovalThroughJoins extends Rewriter {

  def apply(input: AnyRef) = bottomUp(instance).apply(input)

  private val instance: Rewriter = Rewriter.lift {
    case n@NodeHashJoin(nodeIds, lhs, rhs@Selection(rhsPredicates, rhsLeaf)) =>
      val lhsPredicates = predicatesDependingOnTheJoinIds(lhs.solved.lastQueryGraph, nodeIds)
      val newSelection = rhsPredicates.filterNot(lhsPredicates)

      if (newSelection.isEmpty)
        NodeHashJoin(nodeIds, lhs, rhsLeaf)(n.solved)
      else {
        val newRhsPlannerQuery = rhsLeaf.solved.updateGraph(_.addPredicates(newSelection:_*))
        val newRhsSolved = CardinalityEstimation.lift(newRhsPlannerQuery, rhsLeaf.solved.estimatedCardinality)
        NodeHashJoin(nodeIds, lhs, Selection(newSelection, rhsLeaf)(newRhsSolved))(n.solved)
      }
  }

  private def predicatesDependingOnTheJoinIds(qg: QueryGraph, nodeIds: Set[IdName]): Set[Expression] =
    qg.selections.predicates.filter(p => (p.dependencies intersect nodeIds) == nodeIds).map(_.expr)
}
