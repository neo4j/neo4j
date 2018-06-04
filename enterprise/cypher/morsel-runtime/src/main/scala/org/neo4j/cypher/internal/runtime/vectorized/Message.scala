/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.runtime.vectorized

import org.opencypher.v9_0.util.InternalException

sealed trait Message {
  def iterationState: Iteration
}

case class StartLeafLoop(iterationState: Iteration) extends Message
case class StartLoopWithSingleMorsel(data: MorselExecutionContext, iterationState: Iteration) extends Message
case class StartLoopWithEagerData(data: Array[MorselExecutionContext], iterationState: Iteration) extends Message

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

trait Continue extends Continuation

case class ContinueWithData(data: MorselExecutionContext, iteration: Iteration) extends Continue

case class ContinueWithSource[T](source: T, iteration: Iteration) extends Continue

case class ContinueWithDataAndSource[T](data: MorselExecutionContext, source: T, iteration: Iteration) extends Continue

case class NoOp(iteration: Iteration) extends Continuation

/* Response used to signal that all input has been consumed - no more output from this iteration */
case class EndOfLoop(iteration: Iteration) extends Continuation

case class Iteration(argument: Option[MorselExecutionContext]) {
  // the number of longs/refs in the argument can be fewer than the whole row, thus they need to be provided here
  def copyArgumentStateTo(row: MorselExecutionContext, nLongs: Int, nRefs: Int): Unit = {
    argument.foreach(row.copyFrom(_, nLongs, nRefs))
  }
}