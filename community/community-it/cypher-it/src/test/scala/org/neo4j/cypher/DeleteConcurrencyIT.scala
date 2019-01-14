/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.io.{PrintWriter, StringWriter}

import org.neo4j.graphdb.{NotFoundException, TransactionFailureException}
import org.neo4j.internal.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.exceptions.Status

class DeleteConcurrencyIT extends ExecutionEngineFunSuite {

  test("should not fail if a node has been already deleted by another transaction") {
    graph.inTx {
      execute("CREATE (n:person)")
    }

    val threadNum = 2
    val threads: List[MyThread] = (0 until threadNum).map { ignored =>
      new MyThread(1, () => {
        execute(s"MATCH (root) WHERE ID(root) = 0 DELETE root").toList
      })
    }.toList

    threads.foreach(_.start())
    threads.foreach(_.join())

    val errors = threads.collect {
      case t if t.exception != null => t.exception
    }

    withClue(prettyPrintErrors(errors)) {
      errors shouldBe empty
    }
  }

  test("should not fail if a relationship has been already deleted by another transaction") {
    graph.inTx {
      execute("CREATE (:person)-[:FRIEND]->(:person)")
    }

    val threadNum = 2
    val threads: List[MyThread] = (0 until threadNum).map { ignored =>
      new MyThread(1, () => {
        execute(s"MATCH ()-[r:FRIEND]->() WHERE ID(r) = 0 DELETE r").toList
      })
    }.toList

    threads.foreach(_.start())
    threads.foreach(_.join())

    val errors = threads.collect {
      case t if t.exception != null => t.exception
    }

    withClue(prettyPrintErrors(errors)) {
      errors shouldBe empty
    }
  }

  test("relationship delete deadlock") {
    graph.inTx {
      execute("CREATE (p1:person) CREATE (p2:person) CREATE (p1)<-[:T]-(p2)")
    }
    val concurrency = 30
    val threads: List[MyThread] = (0 until concurrency).map { ignored =>
      new MyThread(1, () => {
        execute(s"MATCH ()-[r]->() WITH r DELETE r").toList
      })
    }.toList ++ (0 until concurrency).map { ignored =>
      new MyThread(1, () => {
        execute(s"MATCH (p1), (p2) WHERE id(p1) < id(p2) CREATE (p2)-[:T]->(p1)").toList
      })
    }.toList

    threads.foreach(_.start())
    threads.foreach(_.join())

    val errors = threads.collect {
      case t if t.exception != null => t.exception
    }

    withClue(prettyPrintErrors(errors)) {
      errors shouldBe empty
    }
  }

  test("should not fail when trying to detach delete a node from 2 different transactions") {
    val nodes = 10
    val ids = graph.inTx {
      (0 until nodes).map(ignored => execute("CREATE (n:person) RETURN ID(n) as id").columnAs[Long]("id").next()).toList
    }

    graph.inTx {
      ids.foreach { id =>
        execute(s"MATCH (a) WHERE ID(a) = $id MERGE (b:person_name {val:'Bob Smith'}) CREATE (a)-[r:name]->(b)").toList
      }
    }

    val threads: List[MyThread] = ids.map { id =>
      new MyThread(1, () => {
        execute(s"MATCH (root)-[:name]->(b) WHERE ID(root) = $id DETACH DELETE b").toList
      })
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    val errors = threads.collect {
      case t if t.exception != null => t.exception
    }

    withClue(prettyPrintErrors(errors)) {
      errors shouldBe empty
    }
  }

  test("detach delete should be atomic") {
    val NUM_NODES = 13
    val NUM_EXECUTIONS = 10

    val ids = {
      val ids = graph.inTx {
        (0 until NUM_NODES)
          .map(ignored => execute("CREATE (n:Person) RETURN ID(n) as id")
            .columnAs[Long]("id").next()
          ).toList
      }
      new scala.util.Random(41).shuffle(ids)
    }

    graph.inTx {
      ids.foreach { id =>
        execute(s"MATCH (a) WHERE ID(a) = $id MERGE (b:Name {val:'Bob Smith'}) CREATE (a)-[r:NAMED]->(b)").toList
      }
    }

    val threads: List[MyThread] = {
      val detachThreads =
        ids.map { id =>
          new MyThread(NUM_EXECUTIONS, () => {
            execute(s"MATCH (root)-[:NAMED]->(b) WHERE ID(root) = $id DETACH DELETE b").toList
          })
        }
      val createThreads =
        ids.map { id =>
          new MyThread(NUM_EXECUTIONS, () => {
            execute(s"MATCH (root), (b:Name) WHERE ID(root) = $id CREATE (root)-[:NAMED]->(b)").toList
          }, ignoreRollbackAndNodeNotFound)
        }
      new scala.util.Random(41).shuffle(detachThreads ++ createThreads)
    }

    threads.foreach(_.start())
    threads.foreach(_.join())

    val errors = threads.collect {
      case t if t.exception != null => t.exception
    }

    withClue(prettyPrintErrors(errors)) {
      errors shouldBe empty
    }
  }

  val ignoreRollbackAndNodeNotFound: Throwable => Boolean = {
      // let's ignore the commit failures if they are caused by the above exceptions
      case ex: TransactionFailureException =>
        ex.getCause match {
          case kex: KernelException if kex.status() == Status.Transaction.TransactionMarkedAsFailed => true
          case _ => false
        }
      case ex: CypherExecutionException =>
        ex.status == Status.Statement.EntityNotFound
      case ex: NotFoundException => true
      case _ => false
    }

  private def prettyPrintErrors(errors: Seq[Throwable]): String = {
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    errors.foreach { e => e.printStackTrace(writer); writer.println() }
    stringWriter.toString
  }

  private class MyThread(numExecutions:Int, f: () => Unit, ignoreException: (Throwable) => Boolean = _ => false) extends Thread {
    private var ex: Throwable = _

    def exception: Throwable = ex

    override def run() {
      for ( i <- 0 until numExecutions ) {
        try {
          graph.inTx { f() }
        } catch {
          case ex: Throwable =>
            if (!ignoreException(ex))
              this.ex = ex
        }
      }
    }
  }
}
