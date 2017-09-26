/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.concurrent.ArrayBlockingQueue

import org.neo4j.cypher.internal.runtime.vectorized.ShutdownWorkers
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.util.v3_4.CypherException

class Minion() extends Runnable {
  val input = new ArrayBlockingQueue[Task](100)
  val output = new ArrayBlockingQueue[ResultObject](100)
  private var myQueryContext: QueryContext = _

  override def run(): Unit = {
    // Take a morsel, work a morsel, return a morsel. 'Tis the life of a Worker, no more, no less.
    while (true) {
      try {
        val task: Task = input.take()

        if(task.message == ShutdownWorkers) {
          return
        }

        if (task.query.alive) try {
          if (myQueryContext == null) {
            myQueryContext = task.query.context.createNewQueryContext()
          }

          val morsel = task.morsel
          val state = task.query.queryState
          val next = task.pipeline.operate(task.message, morsel, myQueryContext, state)

          output.put(ResultObject(task.pipeline, task.query, next, morsel))
        } catch {
          case e: CypherException =>
            // If a task for a query dies, we need to kill the whole query
            e.printStackTrace(System.err)
            task.query.finished()
        }
      } catch {
        // someone seems to want us to shut down and go home.
        case _: InterruptedException =>

        // Uh-oh... An uncaught exception is not good. Let's kill everything.
        case e: Exception =>
          e.printStackTrace()
          throw e
      }
    }
  }
}
