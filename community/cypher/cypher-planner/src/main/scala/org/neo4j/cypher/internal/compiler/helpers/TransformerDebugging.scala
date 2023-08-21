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
package org.neo4j.cypher.internal.compiler.helpers

import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.logical.plans.LogicalPlanToPlanBuilderString
import org.neo4j.cypher.internal.util.StepSequencer

object TransformerDebugging {

  /**
   * Transformer that can be inserted when debugging, to help detect
   * what part of the compilation introduces a plan issue.
   */
  def printPlan(tag: String): Transformer[BaseContext, BaseState, BaseState] =
    new Transformer[BaseContext, BaseState, BaseState] {

      override def transform(from: BaseState, context: BaseContext): BaseState = {
        from match {
          case s: LogicalPlanState if s.maybeLogicalPlan.isDefined =>
            println("     |||||||| PRINT Plan: " + tag)
            println(LogicalPlanToPlanBuilderString(s.logicalPlan))
          case _ =>
        }
        from
      }

      override def postConditions: Set[StepSequencer.Condition] = Set.empty

      override def name: String = "print plan"
    }
}
