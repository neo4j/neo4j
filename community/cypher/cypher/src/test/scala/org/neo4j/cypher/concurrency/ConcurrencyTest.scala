/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.concurrency

import java.util.concurrent.atomic.AtomicBoolean

import org.neo4j.cypher.internal.compiler.v2_3.spi.Locker
import org.neo4j.graphdb._
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.exceptions.EntityNotFoundException
import org.neo4j.test.TestGraphDatabaseFactory

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Random

abstract class ConcurrencyTest {
  type Update = (GraphDatabaseAPI => Unit)
  var maxId = 0
  val r = new Random()

  // Runs a random updater
  class UpdateRunner(updaters: Seq[Update], graphDb: GraphDatabaseAPI) extends Runnable {

    private val continue = new AtomicBoolean(true)

    def stop(): Unit = {
      println("stop updater")
      continue.set(false)
    }

    override def run() {
      while (continue.get()) {
        try {
          val update = updaters(r.nextInt(updaters.size))
          update(graphDb)
        } catch {
          case _: OutOfRandomNodesException => println("random node fail")
          case _: EntityNotFoundException =>
          case _: NotFoundException =>
        }
      }
    }
  }


  def updaters: Seq[Update]

  def init(graph: GraphDatabaseAPI)

  def read(graph: GraphDatabaseAPI)

  def start() = {
    val graph: GraphDatabaseAPI = new TestGraphDatabaseFactory().newImpermanentDatabase().asInstanceOf[GraphDatabaseAPI]
    try {
      init(graph)
      val updaterObjects = (1 to 10).map(_ => new UpdateRunner(updaters, graph))
      val threads = updaterObjects.map(new Thread(_))
      threads.foreach(_.start())

      try {
        runForSeconds(10) {
          read(graph)
        }
      } finally {
        updaterObjects.foreach(_.stop())
        threads.foreach(_.join())
      }
    } finally graph.shutdown()
  }

  def runForSeconds(seconds: Int)(f: => Unit): Unit = {
    val start = System.currentTimeMillis()
    while ((System.currentTimeMillis() - start) < seconds * 1000) {
      f
    }
  }

  def randomNode(graph: GraphDatabaseAPI): Node = {

    @tailrec
    def get(i: Int): Node = {
      val x = r.nextInt(maxId)
      try {
        graph.getNodeById(x)
      } catch {
        case e: NotFoundException =>
          if (i < 1000)
            get(i + 1)
          else
            throw new OutOfRandomNodesException
      }
    }

    get(0)
  }

}

class ReadLocker(tx: Transaction) extends Locker {
  private val locks = new mutable.ListBuffer[Lock]

  def releaseAllLocks() {
    locks.foreach(_.release())
  }

  def acquireLock(p: PropertyContainer) {
    locks += tx.acquireReadLock(p)
  }
}

class OutOfRandomNodesException extends Exception
