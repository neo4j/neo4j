/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.api.exceptions.schema.{NoSuchIndexException, DropIndexFailureException}
import java.util.concurrent.TimeUnit
import java.io.{FileOutputStream, File}
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.test.TestGraphDatabaseFactory

class IndexOpAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {

  test("createIndex") {
    // WHEN
    execute("CREATE INDEX ON :Person(name)")

    // THEN
    graph.inTx{
      graph.indexPropsForLabel("Person") should equal(List(List("name")))
    }
  }

  test("createIndexShouldBeIdempotent") {
    // GIVEN
    execute("CREATE INDEX ON :Person(name)")

    // WHEN
    execute("CREATE INDEX ON :Person(name)")

    // THEN no exception is thrown
  }

  test("secondIndexCreationShouldFailIfIndexesHasFailed") {
    // GIVEN
    val graph = createDbWithFailedIndex
    try {
      // WHEN THEN
      intercept[FailedIndexException](execute("CREATE INDEX ON :Person(name)"))
    } finally {
      graph.shutdown()
      new File("target/test-data/impermanent-db").deleteAll()
    }
  }

  test("dropIndex") {
    // GIVEN
    execute("CREATE INDEX ON :Person(name)")

    // WHEN
    execute("DROP INDEX ON :Person(name)")

    // THEN
    graph.inTx{
      graph.indexPropsForLabel("Person") shouldBe empty
    }
  }

  test("drop_index_that_does_not_exist") {
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

  private def createDbWithFailedIndex: GraphDatabaseService = {
    new File("target/test-data/impermanent-db").deleteAll()
    var graph = new TestGraphDatabaseFactory().newEmbeddedDatabase("target/test-data/impermanent-db")
    eengine = new ExecutionEngine(graph)
    execute("CREATE INDEX ON :Person(name)")
    execute("create (:Person {name:42})")
    val tx = graph.beginTx()
    try {
      graph.schema().awaitIndexesOnline(3, TimeUnit.SECONDS)
      tx.success()
    } finally {
      tx.close()
    }
    graph.shutdown()

    val stream = new FileOutputStream("target/test-data/impermanent-db/schema/index/lucene/1/failure-message")
    stream.write(65)
    stream.close()

    graph = new TestGraphDatabaseFactory().newEmbeddedDatabase("target/test-data/impermanent-db")
    eengine = new ExecutionEngine(graph)
    graph
  }
}
