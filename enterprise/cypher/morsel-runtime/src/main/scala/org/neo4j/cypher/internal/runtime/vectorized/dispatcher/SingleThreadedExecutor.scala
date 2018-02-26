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
package org.neo4j.cypher.internal.runtime.vectorized.dispatcher

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.util.v3_4.{InternalException, TaskCloser}
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue

import scala.collection.mutable

class SingleThreadedExecutor(morselSize: Int = 100000) extends Dispatcher {

  override def execute[E <: Exception](operators: Pipeline,
                                       queryContext: QueryContext,
                                       params: MapValue,
                                       taskCloser: TaskCloser)
                                      (visitor: QueryResultVisitor[E]): Unit = {
    var leafOp = operators
    while (leafOp.dependency != NoDependencies) {
      leafOp = leafOp.dependency.pipeline
    }

    val jobStack: mutable.Stack[(Message, Pipeline)] = new mutable.Stack[(Message, Pipeline)]()
    val iteration = new Iteration(None)
    jobStack.push((StartLeafLoop(iteration), leafOp))
    val state = QueryState(params, visitor)
    val eagerAcc = new mutable.ArrayBuffer[Morsel]()
    var eagerRecipient: Pipeline = null
    do {
      if(eagerAcc.nonEmpty) {
        jobStack.push((StartLoopWithEagerData(eagerAcc.toArray, iteration), eagerRecipient))
        eagerAcc.clear()
        eagerRecipient = null
      }

      while (jobStack.nonEmpty) {
        val (message, pipeline) = jobStack.pop()
        val data = Morsel.create(pipeline.slots, morselSize)
        val continuation = pipeline.operate(message, data, queryContext, state)
        if (continuation != EndOfLoop(iteration)) {
          jobStack.push((ContinueLoopWith(continuation), pipeline))
        }

        pipeline.parent match {
          case Some(mother) if mother.dependency.isInstanceOf[Eager] =>
            if(eagerRecipient != null && mother != eagerRecipient) {
              taskCloser.close(success = false)
              throw new InternalException("oh noes")
            }
            eagerRecipient = mother
            eagerAcc.append(data)

          case Some(mother) if mother.dependency.isInstanceOf[Lazy] =>
            jobStack.push((StartLoopWithSingleMorsel(data, iteration), pipeline.parent.get))

          case _ =>
        }
      }
    }
    while (eagerAcc.nonEmpty)

    taskCloser.close(success = true)
  }
}
