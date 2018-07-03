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
package org.neo4j.cypher.internal.runtime.parallel

import java.util
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors}
import java.util.concurrent.atomic.AtomicLong

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SimpleSchedulerTest extends CypherFunSuite {

  test("execute simple task") {

    val s = new SimpleScheduler( Executors.newFixedThreadPool( 1 ) )

    val testThread = Thread.currentThread().getId
    val taskThreadId = new AtomicLong(testThread)

    val sb = new StringBuilder
    val queryExecution = s.execute(NoopTask(() => {
      sb ++= "great success"
      taskThreadId.set(Thread.currentThread().getId)
    }))

    queryExecution.await()

    sb.result() should equal("great success")
    taskThreadId.get() should not equal testThread
  }

  test("execute 1000 simple tasks, spread over 4 threads") {
    val concurrency = 4
    val s = new SimpleScheduler( Executors.newFixedThreadPool( concurrency ) )

    val map = new ConcurrentHashMap[Int, Long]()
    val futures =
      for ( i <- 0 until 1000 ) yield
        s.execute(NoopTask(() => {
          map.put(i, Thread.currentThread().getId)
        }))

    futures.foreach(f => f.await())

    val countsPerThread = map.toSeq.groupBy(kv => kv._2).mapValues(_.size)
    for ((threadId, count) <- countsPerThread) {
      count should be > 1
    }
  }

  test("execute downstream tasks") {

    val s = new SimpleScheduler( Executors.newFixedThreadPool( 2 ) )

    val result: mutable.Set[String] =
      java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap[String, java.lang.Boolean])

    val queryExecution = s.execute(
      SubTasker(List(
        NoopTask(() => result += "once"),
        NoopTask(() => result += "upon"),
        NoopTask(() => result += "a"),
        NoopTask(() => result += "time")
      )))

    queryExecution.await()
    result should equal(Set("once", "upon", "a", "time"))
  }

  test("execute reduce-like task tree") {

    val s = new SimpleScheduler( Executors.newFixedThreadPool( 64 ) )

    var output = new ArrayBuffer[Int]
    val aggregator = Aggregator(buffer => {
                                  for (x <- buffer)
                                    output += x
                                })

    val tasks = SubTasker(List(
      PushToEager(List(1,2,3), aggregator),
      PushToEager(List(4,5,6), aggregator)))

    val queryExecution = s.execute(tasks)

    queryExecution.await()
    output.toSet should equal(Set(1, 2, 3, 4, 5, 6))
  }

  // HELPER TASKS

  case class Aggregator(method: util.Collection[Int] => Unit) extends Task {

    val buffer = new ConcurrentLinkedQueue[Int]

    override def executeWorkUnit(): Seq[Task] = {
      method(buffer)
      buffer.clear()
      Nil
    }

    override def canContinue: Boolean = buffer.nonEmpty
  }

  case class PushToEager(subResults: Seq[Int], eager: Aggregator) extends Task {

    private val resultSequence = subResults.iterator

    override def executeWorkUnit(): Seq[Task] = {
      if (resultSequence.hasNext)
        eager.buffer.add(resultSequence.next())

      if (canContinue) Nil
      else List(eager)
    }

    override def canContinue: Boolean = resultSequence.hasNext
  }

  case class SubTasker(subtasks: Seq[Task]) extends Task {

    private val taskSequence = subtasks.iterator

    override def executeWorkUnit(): Seq[Task] =
      if (taskSequence.hasNext) List(taskSequence.next())
      else Nil

    override def canContinue: Boolean = taskSequence.nonEmpty
  }

  case class NoopTask(f: () => Any) extends Task {
    override def executeWorkUnit(): Seq[Task] = {
      f()
      Nil
    }

    override def canContinue: Boolean = false
  }
}
