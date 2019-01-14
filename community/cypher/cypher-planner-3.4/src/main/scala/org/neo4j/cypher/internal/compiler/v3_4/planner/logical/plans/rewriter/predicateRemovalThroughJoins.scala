/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.util.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.v3_4.expressions.Expression
import org.neo4j.cypher.internal.ir.v3_4.QueryGraph
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.util.v3_4.attribution.{Attributes, SameId}
import org.neo4j.cypher.internal.v3_4.logical.plans.{NodeHashJoin, Selection}

/*
A join on given variable is similar to a logical AND - any predicates evaluated on the LHS will in effect
also be applied to the output of the join. This means that evaluating the same predicate on the RHS is redundant.

This rewriters finds predicates on the join variables, and removes any predicates on the RHS that already
exist on the LHS.
 */
case class predicateRemovalThroughJoins(solveds: Solveds, cardinalities: Cardinalities, attributes: Attributes) extends Rewriter {

  override def apply(input: AnyRef) = instance.apply(input)

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case n@NodeHashJoin(nodeIds, lhs, rhs@Selection(rhsPredicates, rhsLeaf)) =>
      val lhsPredicates = predicatesDependingOnTheJoinIds(solveds.get(lhs.id).lastQueryGraph, nodeIds)
      val newPredicate = rhsPredicates.filterNot(lhsPredicates)

      if (newPredicate.isEmpty) {
        NodeHashJoin(nodeIds, lhs, rhsLeaf)(SameId(n.id))
      } else {
        val newRhsPlannerQuery = solveds.get(rhsLeaf.id).amendQueryGraph(_.addPredicates(newPredicate: _*))
        val newSelection = Selection(newPredicate, rhsLeaf)(attributes.copy(rhs.id))
        solveds.set(newSelection.id, newRhsPlannerQuery)
        cardinalities.copy(rhsLeaf.id, newSelection.id)
        NodeHashJoin(nodeIds, lhs, newSelection)(SameId(n.id))
      }
  })

  private def predicatesDependingOnTheJoinIds(qg: QueryGraph, nodeIds: Set[String]): Set[Expression] =
    qg.selections.predicates.filter(p => (p.dependencies intersect nodeIds) == nodeIds).map(_.expr)
}
