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

import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * The input to PartialSort is already sorted by a prefix.
 * If there is a SKIP as well, we can skip over whole chunks before we need to start sorting.
 */
case object skipInPartialSort extends Rewriter with BottomUpMergeableRewriter {

  override val innerRewriter: Rewriter = Rewriter.lift {
    case s @ Skip(ps @ PartialSort(_, _, _, None), skip) =>
      s.copy(source = ps.copy(skipSortingPrefixLength = Some(skip))(SameId(ps.id)))(SameId(s.id))

    case s @ Skip(ptop @ PartialTop(_, _, _, _, None), skip) =>
      s.copy(source = ptop.copy(skipSortingPrefixLength = Some(skip))(SameId(ptop.id)))(SameId(s.id))
  }

  private val instance: Rewriter = bottomUp(innerRewriter)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
