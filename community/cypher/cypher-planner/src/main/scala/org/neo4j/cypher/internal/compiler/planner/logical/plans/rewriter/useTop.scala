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

import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * When doing ORDER BY c1,c2,...,cn LIMIT e, we don't have to sort the full result in one go
 */
case object useTop extends Rewriter with BottomUpMergeableRewriter {

  override val innerRewriter: Rewriter = Rewriter.lift {
    case o @ Limit(Sort(src, sortDescriptions), limit) =>
      Top(src, sortDescriptions, limit)(SameId(o.id))
    // NOTE: it is only safe to rewrite ExhaustiveLimit + Sort not ExhaustiveLimit + PartialSort
    //      since we can't guarantee that src will be exhausted in that case
    case o @ ExhaustiveLimit(Sort(src, sortDescriptions), limit) =>
      Top(src, sortDescriptions, limit)(SameId(o.id))
    case o @ Limit(PartialSort(src, alreadySortedPrefix, stillToSortSuffix, skipSortingPrefixLength), limit) =>
      PartialTop(src, alreadySortedPrefix, stillToSortSuffix, limit, skipSortingPrefixLength)(SameId(o.id))
  }

  private val instance: Rewriter = bottomUp(innerRewriter)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
