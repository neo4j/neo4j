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

import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Fix ID references in a plan, after some rewriters have changed plan IDs.
 */
class FixIdReferences(cancellationChecker: CancellationChecker) {

  /**
   * A map from old plan ID to new plan ID.
   * This map is kept up to date while executing the rewriters.
   */
  private val oldToNewIds: mutable.Map[Id, Id] = mutable.Map()

  /**
   * Get the new ID for the given old ID.
   * If the new ID is also present in the map, continue getting the mapping of the new ID until
   * the newest ID is not present in the mapping any more.
   * Return old ID if it is not present in the mapping.
   */
  @tailrec
  private def recursiveNewIdFor(oldId: Id): Id = {
    oldToNewIds.get(oldId) match {
      case Some(newId) => recursiveNewIdFor(newId)
      case None        => oldId
    }
  }

  /**
   * Get the new ID for the given old ID.
   * Fail no mapping is present.
   */
  private def newIdFor(oldId: Id): Id = oldToNewIds(oldId)

  /**
   * Obtain a rewriter that fixes all ID references.
   * @param recursiveIdLookup Depending on the nature of the mapping from old to new IDs, new IDs need to be resolved
   *                          differently.
   *                          <p>
   *                          Scenario 1: New ID space.
   *                          If all plans get new IDs in a completely new ID space, then we must use a single lookup
   *                          in the map.
   *                          <p>
   *                          Scenario 2: Same ID space, multiple remappings.
   *                          If plans can get replaced by other plans with other IDs - multiple times - then we must
   *                          use a recursive lookup in the map.
   * @return
   */
  def apply(recursiveIdLookup: Boolean): Rewriter = {
    val newId: Id => Id = if (recursiveIdLookup) recursiveNewIdFor else newIdFor
    topDown(
      Rewriter.lift {
        case EagernessReason.Conflict(first, second) =>
          EagernessReason.Conflict(newId(first), newId(second))
      },
      cancellation = cancellationChecker
    )
  }

  /**
   * Register a mapping from the old ID to the new ID.
   */
  def registerMapping(oldId: Id, newId: Id): Unit = {
    oldToNewIds.addOne(oldId -> newId)
  }

  /**
   * For each ID in `others` register a mapping of that ID to the ID in `keep`.
   * Return the same ID as in `keep`.
   */
  def combineIds(keep: Id, others: Id*): IdGen = {
    others.foreach(registerMapping(_, keep))
    SameId(keep)
  }

  /**
   * Obtain the currently present ID mappings.
   */
  def getCurrentMappings(): Map[Id, Id] = oldToNewIds.toMap
}
