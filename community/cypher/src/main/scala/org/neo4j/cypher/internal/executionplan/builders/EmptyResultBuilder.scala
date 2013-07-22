/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.executionplan.{PlanBuilder, ExecutionPlanInProgress, LegacyPlanBuilder}
import org.neo4j.cypher.internal.pipes.EmptyResultPipe

class EmptyResultBuilder extends LegacyPlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val resultPipe = new EmptyResultPipe(plan.pipe)

    plan.copy(pipe = resultPipe)
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val q = plan.query

    val notYetExtracted = q.extracted
    val noSortingLeftToDo = !q.sort.exists(_.unsolved)
    val noSkipOrLimitLeftToDo = !q.slice.exists(_.unsolved)
    val notYetHandledReturnItems = q.returns.exists(_.unsolved)
    val nothingToReturnButPipeNotEmpty = q.returns.size == 0 && plan.pipe.symbols.size > 0
    val isLastPipe = q.tail.isEmpty

    notYetExtracted &&
      noSortingLeftToDo &&
      noSkipOrLimitLeftToDo &&
      isLastPipe &&
      (notYetHandledReturnItems || nothingToReturnButPipeNotEmpty)
  }

  def priority = PlanBuilder.ColumnFilter
}