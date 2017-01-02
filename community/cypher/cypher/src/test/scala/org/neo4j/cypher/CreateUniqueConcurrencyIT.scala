/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import java.util.Random
import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.kernel.DeadlockDetectedException

class CreateUniqueConcurrencyIT extends ExecutionEngineFunSuite {

  test("create unique should not fail due to insufficient locking") {
    var counter = 0
    (5 until 8).foreach { nbrOfNodes =>
      (0 until nbrOfNodes).foreach { i =>
        execute(s"CREATE (:A {id: $i})")
      }
      (20 until 50).foreach { nbrOfThreads =>
        execute("MATCH ()-[r]->() DELETE r")
        val errorsFound = runConcurrentlyToFindDeadlocks(nbrOfNodes, nbrOfThreads)
        if (errorsFound > 0)
          println(s"found $errorsFound errors")
        counter = counter + errorsFound
      }
      execute("MATCH (n) DETACH DELETE n")
    }
    counter shouldBe 0
  }

  def runConcurrentlyToFindDeadlocks(nbrOfNodes: Int, nbrOfThreads: Int): Int = {
    val random = new Random()
    val counter = new AtomicInteger(0)
    val deadlockCounter = new AtomicInteger(0)

    val threads = (0 until nbrOfThreads).map { _ =>
      new Thread(new Runnable {
        override def run(): Unit = {
          val startId = random.nextInt(nbrOfNodes)
          var endId = random.nextInt(nbrOfNodes)
          while (endId == startId)
            endId = random.nextInt(nbrOfNodes)
          try {
            execute(s"MATCH (s:A {id: $startId}), (e:A {id: $endId}) CREATE UNIQUE (s)-[:REL]->(e)")
          } catch {
            case deadlock: DeadlockDetectedException =>
              deadlockCounter.incrementAndGet()
            case e: Exception =>
              counter.incrementAndGet()
          }
        }
      })
    }

    threads.foreach(_.start())
    threads.foreach(_.join)
    if (deadlockCounter.get() > 0)
      println(s"Deadlocks found: ${deadlockCounter.get()}")
    counter.get()
  }

}
