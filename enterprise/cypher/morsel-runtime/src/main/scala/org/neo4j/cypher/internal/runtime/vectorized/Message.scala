/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.neo4j.cypher.internal.runtime.slotted.SlottedExecutionContext
import org.neo4j.cypher.internal.util.v3_4.InternalException

sealed trait Message {
  def iterationState: Iteration
}

/* This is called per start of iteration. Pipelines run once per incoming row will have this calles once per row.
* Once per query, the InitIteration message will come with the initQuery flag set. Until this first run has finished,
* not other runs will be scheduled, giving the pipeline a chance to initialise global state.
* */
case class StartLeafLoop(iterationState: Iteration) extends Message
case class StartLoopWithSingleMorsel(data: Morsel, iterationState: Iteration) extends Message
case class StartLoopWithEagerData(data: Array[Morsel], iterationState: Iteration) extends Message

case class ContinueLoopWith(continuation: Continuation) extends Message {
  override def iterationState: Iteration = continuation.iteration
}

// Used to signal all workers that they should pack up and go home
object ShutdownWorkers extends Message {
  override def iterationState: Iteration = throw new InternalException("Not expected to run in an iteration")
}

/* Used to signal how to continue with an iteration at a later point, or that an iteration has finished */
sealed trait Continuation {
  val iteration: Iteration
}

trait Continue extends Continuation {
  val needsSameThread: Boolean
}

case class ContinueWithData(data: Morsel, index: Int, iteration: Iteration) extends Continue {
  override val needsSameThread = false
}

case class ContinueWithSource[T](source: T, iteration: Iteration, needsSameThread: Boolean) extends Continue

case class ContinueWithDataAndSource[T](data: Morsel, index: Int, source: T, iteration: Iteration)
  extends Continue {
  override val needsSameThread = true
}

case class NoOp(iteration: Iteration) extends Continuation

/* Response used to signal that all input has been consumed - no more output from this iteration */
case class EndOfLoop(iteration: Iteration) extends Continuation

class Iteration(argument: Option[SlottedExecutionContext])
