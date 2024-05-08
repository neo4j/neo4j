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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Foldable.FoldableAny

/**
 * Checks that [[EagernessReason.Conflict]]s reference only valid plan IDs.
 */
case object ConflictsReferenceValidIds extends ValidatingCondition {

  override def apply(a: Any)(cancellationChecker: CancellationChecker): Seq[String] = {
    // Using flatten instead of treeCollect to explicitly exclude IDs in nested plan expressions and such.
    val ids = (a match {
      case lp: LogicalPlan => lp.flatten(cancellationChecker).map(_.id)
      case _               => throw new IllegalStateException(s"Did not pass a LogicalPlan to $name")
    }).toSet

    val invalidConflicts = a.folder(cancellationChecker).treeCollect {
      case c @ EagernessReason.Conflict(first, second) if !Set(first, second).subsetOf(ids) => c
    }

    invalidConflicts.map(c => s"$c references a non-existing plan ID.")
  }

  override def name: String = productPrefix
}
