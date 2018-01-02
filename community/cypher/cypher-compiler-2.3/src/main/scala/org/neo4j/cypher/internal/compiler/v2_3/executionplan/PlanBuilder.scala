/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.pipes.PipeMonitor

/*
PlanBuilders are basically partial functions that can execute for some input, and can answer if an
input is something it can work on.

The idea is that they take an ExecutionPlanInProgress, and moves it a little bit towards a more solved plan, which is
represented by the stack of Pipes.

A PlanBuilder should only concern itself with the first PartiallySolvedQuery part - the tail query parts will be seen
later.

It's important that PlanBuilders never return the input unchanged.
*/

trait PlanBuilder {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): Boolean

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor): ExecutionPlanInProgress

  def missingDependencies(plan: ExecutionPlanInProgress): Seq[String] = Seq()

  implicit class SeqWithReplace[A](inSeq: Seq[A]) {
    def replace(remove: A, replaceWith: A) = inSeq.filterNot(_ == remove) :+ replaceWith
  }
}
