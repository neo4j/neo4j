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
package org.neo4j.cypher.internal.planning.notification

import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.notifications.EagerLoadCsvNotification

object checkForEagerLoadCsv extends NotificationChecker {

  def apply(plan: LogicalPlan): Seq[InternalNotification] = {
    sealed trait SearchState
    case object NoEagerFound extends SearchState
    case object EagerFound extends SearchState
    case object EagerWithLoadCsvFound extends SearchState

    // Walk over the pipe tree and check if an Eager is to be executed after a LoadCsv
    val resultState = plan.folder.treeFold[SearchState](NoEagerFound) {
      case _: LoadCSV => {
        case EagerFound => SkipChildren(EagerWithLoadCsvFound)
        case e          => SkipChildren(e)
      }
      case _: Eager =>
        _ =>
          TraverseChildren(EagerFound)
    }

    resultState match {
      case EagerWithLoadCsvFound => Seq(EagerLoadCsvNotification)
      case _                     => Seq.empty
    }
  }
}
