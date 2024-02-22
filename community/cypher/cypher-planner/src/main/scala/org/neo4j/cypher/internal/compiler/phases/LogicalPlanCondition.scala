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
package org.neo4j.cypher.internal.compiler.phases

import org.neo4j.cypher.internal.rewriting.ValidatingCondition
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.StepSequencer

case class LogicalPlanCondition(inner: ValidatingCondition) extends ValidatingCondition {

  override def apply(state: Any)(cancellationChecker: CancellationChecker): Seq[String] = state match {
    case s: LogicalPlanState => inner(s.logicalPlan)(cancellationChecker: CancellationChecker)
    case x                   => throw new IllegalStateException(s"Unknown state: $x")
  }

  override def name: String = productPrefix
}

object LogicalPlanCondition {

  /**
   * Conditions that during Rewriting check the LogicalPlan need to be checked on the LogicalPlan only.
   * When checking these same conditions during higher-level phases, we need to wrap ValidatingCondition in LogicalPlanCondition.
   */
  def wrap(condition: StepSequencer.Condition): StepSequencer.Condition = {
    condition match {
      case vc: ValidatingCondition => LogicalPlanCondition(vc)
      case _                       => condition
    }
  }
}
