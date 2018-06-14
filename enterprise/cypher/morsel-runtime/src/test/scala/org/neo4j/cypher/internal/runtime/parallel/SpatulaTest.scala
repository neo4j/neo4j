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

import java.util.concurrent.{ConcurrentHashMap, Executors}
import java.util.concurrent.atomic.AtomicLong

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SpatulaTest extends CypherFunSuite {

  test("execute a bunch of things") {

    val s = new ASpatula( Executors.newFixedThreadPool( 1 ) )

    val testThread = Thread.currentThread().getId
    val taskThreadId = new AtomicLong(testThread)

    val sb = new StringBuilder
    val queryExecution = s.execute(NoopTask(() => {
      sb ++= "great success"
      taskThreadId.set(Thread.currentThread().getId)
    }))

    queryExecution.await()

    sb.result() should equal("great success")
    taskThreadId.get() should not equal(testThread)
  }

  test("execute more things") {
    val concurrency = 4
    val s = new ASpatula( Executors.newFixedThreadPool( concurrency ) )

    val map = new ConcurrentHashMap[Int, Long]()
    val futures =
      for ( i <- 0 until 1000 ) yield
        s.execute(NoopTask(() => {
          map.put(i, Thread.currentThread().getId)
        }))

    futures.foreach(f => f.await())

    val countsPerThread = map.toSeq.groupBy(kv => kv._2).mapValues(_.size)
    for ((threadId, count) <- countsPerThread) {
      count should be > 180
    }
  }

  test("execute a subtask thing") {

    val s = new ASpatula( Executors.newFixedThreadPool( 2 ) )

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

  test("execute more relish") {

    val s = new ASpatula( Executors.newFixedThreadPool( 64 ) )

    var output: List[Int] = List()
    val method :ArrayBuffer[Int] => Unit = (arrayBuffer) => output ++= arrayBuffer

    val aggregator = Aggregator(method)
    val tasks = SubTasker(List(
      PushToEager(List(1,2,3), aggregator),
      PushToEager(List(4,5,6), aggregator)))

    val queryExecution = s.execute(tasks)

    queryExecution.await()
    output should equal(List(1,2,3))
  }

  case class Aggregator(method: ArrayBuffer[Int] => Unit) extends Task {

    val buffer = new ArrayBuffer[Int]

    override def executeWorkUnit(): Seq[Task] ={
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
        eager.buffer.append(resultSequence.next())

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

  case class Row(nodeId:Long)

  /*

    SemiApply
  AS      Exp
          Arg

  L(IN) R(OUT)
  1 T   1
  1 T   1
  2 F

  Arg
  argumentId L
  0          1
  1          1
  2          2

  post-Exp

  L R N2 ArgumentId
  1 1 3  0
  1 2 3  0
  1 3 4  0
  1 1 3  1
  1 2 3  1
  1 3 4  1
  2 4 4  2
  2 5 5  2



    SemiApply
          Selection
          OP1
   ..   Op2 Op3
       Op4  Op5
           Op6  Op7

   */

  case class Argument(argument: Array[Row]) extends Task {
    override def executeWorkUnit(): Seq[Task] = ???

    override def canContinue: Boolean = ???
  }


  case class OneRowApply(inputMorsel: Array[Row], arg: Argument) extends Task {

    val rows = inputMorsel.iterator

    override def executeWorkUnit(): Seq[Task] = {
      if (rows.hasNext) List(Argument(Array(rows.next)))
      else Nil
    }

    override def canContinue: Boolean = ???
  }




  case class NoopTask(f: () => Any) extends Task {
    override def executeWorkUnit(): Seq[Task] = {
      f()
      Nil
    }

    override def canContinue: Boolean = false
  }

  case class SingleThreadedAllNodeScan(start:Int, end:Int) extends Task {
    override def executeWorkUnit(): Seq[Task] = ???

    override def canContinue: Boolean = ???
  }

  case class ParallelAllNodeScan() extends Task {
    override def executeWorkUnit(): Seq[Task] = {
      for (range <- List((0, 1000), (1001, 2000))) yield {
        SingleThreadedAllNodeScan(range._1, range._2)
      }
    }

    override def canContinue: Boolean = ???
  }
}
