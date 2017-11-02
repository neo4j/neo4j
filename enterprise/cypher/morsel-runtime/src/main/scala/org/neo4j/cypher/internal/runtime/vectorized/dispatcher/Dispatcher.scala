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

import java.util.concurrent.ConcurrentLinkedQueue

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.PipelineInformation
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.vectorized._
import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.graphdb.Result
import org.neo4j.values.virtual.MapValue


class Dispatcher {

  private val MORSEL_SIZE = 1000
  private val WORKER_THREADS = {
    val r = Runtime.getRuntime.availableProcessors() * 2
    println(s"starting $r minions")
    r
  }

  def run[E <: Exception](pipeline: Pipeline,
                          visitor: Result.ResultVisitor[E],
                          context: QueryContext,
                          resultPipe: PipelineInformation,
                          params: MapValue): Unit = {
    val query = Query(pipeline, context, QueryState(params, visitor), resultPipe)

    queryQueue.offer(query)

    while (query.alive) {
      try {
        Thread.sleep(100) // Wait for stuff to be available to us
      } catch {
        case _: InterruptedException =>
      }
    }
  }

  private val queryQueue: ConcurrentLinkedQueue[Query] = new ConcurrentLinkedQueue[Query]()

  def start(): Unit = {
    if (workers != null)
      throw new InternalException("Dispatcher already started")

    val minions = (0 to WORKER_THREADS) map { i =>
      val minion = new Minion()
      val t = new Thread(minion, s"Query Worker $i")
      t.setDaemon(true)
      t.start()
      minion
    }
    workers = minions.toArray
    conductor = new Conductor(workers, MORSEL_SIZE, queryQueue)

    val t = new Thread(conductor, "Query Conductor")
    t.setDaemon(true)
    t.start()
  }

  def shutdown(): Unit = {
    if (conductor != null) {
      conductor.shutdown()
      conductor = null
    } else
      throw new RuntimeException("Not running")
  }

  private var workers: Array[Minion] = _
  private var conductor: Conductor = _
}

object Dispatcher {
  lazy val instance: Dispatcher = createAndStartDispatcher()

  private def createAndStartDispatcher(): Dispatcher = {
    val dispatcher = new Dispatcher
    dispatcher.start()
    dispatcher
  }
}
