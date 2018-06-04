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
package org.neo4j.cypher.internal.runtime.vectorized.dispatcher

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.opencypher.v9_0.util.{InternalException, TaskCloser}
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
    val iteration = Iteration(None)
    jobStack.push((StartLeafLoop(iteration), leafOp))
    val state = QueryState(params, visitor)
    // TODO use PipeLineWithEagerDependency(eagerData) instead
    val eagerAcc = new mutable.ArrayBuffer[MorselExecutionContext]()
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
          case Some(mother@PipeLineWithEagerDependency(_)) =>
            if(eagerRecipient != null && mother != eagerRecipient) {
              taskCloser.close(success = false)
              throw new InternalException("oh noes")
            }
            eagerRecipient = mother
            eagerAcc.append(MorselExecutionContext(data, pipeline))

          case Some(Pipeline(_,_,_, Lazy(_))) =>
            jobStack.push((StartLoopWithSingleMorsel(MorselExecutionContext(data, pipeline), iteration), pipeline.parent.get))

          case _ =>
        }
      }
    }
    while (eagerAcc.nonEmpty)

    taskCloser.close(success = true)
  }
}
