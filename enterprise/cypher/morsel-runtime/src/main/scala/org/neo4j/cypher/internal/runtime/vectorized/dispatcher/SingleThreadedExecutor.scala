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
import org.neo4j.cypher.internal.runtime.parallel.Task
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.result.QueryResult.QueryResultVisitor
import org.neo4j.values.virtual.MapValue
import org.opencypher.v9_0.util.TaskCloser

import scala.collection.mutable

class SingleThreadedExecutor(morselSize: Int = 100000) extends Dispatcher {

  override def execute[E <: Exception](operators: Pipeline,
                                       queryContext: QueryContext,
                                       params: MapValue,
                                       taskCloser: TaskCloser)
                                      (visitor: QueryResultVisitor[E]): Unit = {
    var leafOp = operators
    while (leafOp.upstream.nonEmpty) {
      leafOp = leafOp.upstream.get
    }

    val state = QueryState(params, visitor, morselSize, true)
    val initialTask = leafOp.asInstanceOf[StreamingPipeline].init(MorselExecutionContext.EMPTY, queryContext, state)

    val jobStack: mutable.Stack[Task] = new mutable.Stack()
    jobStack.push(initialTask)

    try {
      while (jobStack.nonEmpty) {
        val nextTask = jobStack.pop()

        val downstreamTasks = nextTask.executeWorkUnit()
        for (newTask <- downstreamTasks)
          jobStack.push(newTask)

        if (nextTask.canContinue)
          jobStack.push(nextTask)
      }

      taskCloser.close(success = true)
    } catch {
      case t: Throwable =>
        taskCloser.close(success = false)
        throw t
    }
  }
}
