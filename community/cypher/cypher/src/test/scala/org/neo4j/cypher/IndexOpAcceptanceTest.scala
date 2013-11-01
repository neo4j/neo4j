/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.scalatest.Assertions
import org.junit.Test
import org.neo4j.kernel.api.exceptions.schema.{NoSuchIndexException, DropIndexFailureException}
import org.neo4j.kernel.EmbeddedGraphDatabase
import java.util.concurrent.TimeUnit
import java.io.{FileOutputStream, File}

class IndexOpAcceptanceTest extends ExecutionEngineHelper with StatisticsChecker with Assertions {
  @Test def createIndex() {
    // WHEN
    execute("CREATE INDEX ON :Person(name)")

    // THEN
    assertInTx(List(List("name")) === graph.indexPropsForLabel("Person"))
  }

  @Test def createIndexShouldBeIdempotent() {
    // GIVEN
    execute("CREATE INDEX ON :Person(name)")

    // WHEN
    execute("CREATE INDEX ON :Person(name)")

    // THEN no exception is thrown
  }

  @Test def secondIndexCreationShouldFailIfIndexesHasFailed() {
    // GIVEN
    try {
      createDbWithFailedIndex()

      // WHEN THEN
      intercept[FailedIndexException](execute("CREATE INDEX ON :Person(name)"))
    } finally {
      graph.shutdown()
      new File("target/test-data/impermanent-db").deleteAll()
    }
  }

  private def createDbWithFailedIndex() {
    graph = new EmbeddedGraphDatabase("target/test-data/impermanent-db") with Snitch
    engine = new ExecutionEngine(graph)
    execute("CREATE INDEX ON :Person(name)")
    execute("create (:Person {name:42})")
    graph.inTx {
      graph.schema().awaitIndexesOnline(3, TimeUnit.SECONDS)
    }
    graph.shutdown()

    val stream = new FileOutputStream("target/test-data/impermanent-db/schema/index/lucene/1/failure-message")
    stream.write(65)
    stream.close()

    graph = new EmbeddedGraphDatabase("target/test-data/impermanent-db") with Snitch
    engine = new ExecutionEngine(graph)
  }

  @Test def dropIndex() {
    // GIVEN
    execute("CREATE INDEX ON :Person(name)")

    // WHEN
    execute("DROP INDEX ON :Person(name)")

    // THEN
    assertInTx(List.empty[List[String]] === graph.indexPropsForLabel("Person"))
  }

  @Test def drop_index_that_does_not_exist() {
    // WHEN
    val e = intercept[CypherExecutionException](execute("DROP INDEX ON :Person(name)"))
    assert(e.getCause.isInstanceOf[DropIndexFailureException])
    assert(e.getCause.getCause.isInstanceOf[NoSuchIndexException])
  }

  implicit class FileHelper(file: File) {
    def deleteAll(): Unit = {
      def deleteFile(dfile: File): Unit = {
        if (dfile.isDirectory)
          dfile.listFiles.foreach {
            f => deleteFile(f)
          }
        dfile.delete
      }
      deleteFile(file)
    }
  }

}


