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

import java.io.{PrintWriter, StringWriter}

class DeleteConcurrencyIT extends ExecutionEngineFunSuite {

  test("should not fail if a node has been already deleted by another transaction") {
    graph.inTx {
      execute("CREATE (n:person)")
    }

    val threads: List[MyThread] = (0 until 2).map { ignored =>
      new MyThread(() => {
        execute(s"MATCH (root) WHERE ID(root) = 0 DELETE root").toList
      })
    }.toList

    threads.foreach(_.start())
    threads.foreach(_.join())

    import scala.language.reflectiveCalls
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

    val threads: List[MyThread] = (0 until 2).map { ignored =>
      new MyThread(() => {
        execute(s"MATCH ()-[r:FRIEND]->() WHERE ID(r) = 0 DELETE r").toList
      })
    }.toList

    threads.foreach(_.start())
    threads.foreach(_.join())

    import scala.language.reflectiveCalls
    val errors = threads.collect {
      case t if t.exception != null => t.exception
    }

    withClue(prettyPrintErrors(errors)) {
      errors shouldBe empty
    }
  }

  private def prettyPrintErrors(errors: Seq[Throwable]): String = {
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)
    errors.foreach { e => e.printStackTrace(writer); writer.println() }
    stringWriter.toString
  }

  private class MyThread(f: () => Unit) extends Thread {
    private var ex: Throwable = null

    def exception: Throwable = ex

    override def run() {
      try {
        graph.inTx { f() }
      } catch {
        case ex: Throwable => this.ex = ex
      }
    }
  }
}
