/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
