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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPCache.Results

// Read-only interface to IDPTable
trait IDPCache[Result] {
  def size: Int

  /**
   * Returns a tuple of a result and a sorted result, if they are in the table.
   */
  def apply(goal: Goal): Results[Result]

  def contains(goal: Goal, sorted: Boolean): Boolean

  /**
   * All plans of size k, which do not have the SORTED_BIT set
   */
  def unsortedPlansOfSize(k: Int): Iterator[(Goal, Result)]

  def plans: Iterator[((Goal, Boolean), Result)]
}

object IDPCache {

  case class Results[Result](result: Option[Result], sortedResult: Option[Result]) {

    /**
     * Returns iterator over all unique results
     */
    def iterator: Iterator[Result] = (result.toSet ++ sortedResult).iterator
  }
}
