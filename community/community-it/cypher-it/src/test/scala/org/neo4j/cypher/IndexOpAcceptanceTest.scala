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

import java.io.File
import java.util.concurrent.TimeUnit

import org.neo4j.cypher.ExecutionEngineHelper.createEngine
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.exceptions.schema.{DropIndexFailureException, NoSuchIndexException}
import org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory
import org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory.FailureType.POPULATION
import org.neo4j.test.TestGraphDatabaseFactory
import org.neo4j.test.rule.TestDirectory

class IndexOpAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {

  test("createIndex") {
    // WHEN
    execute("CREATE INDEX ON :Person(name)")

    // THEN
    graph.inTx {
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
      val e = intercept[FailedIndexException](execute("CREATE INDEX ON :Person(name)"))
      e.getMessage should include (org.neo4j.kernel.impl.index.schema.FailingGenericNativeIndexProviderFactory.POPULATION_FAILURE_MESSAGE)
    } finally {
      graph.shutdown()
      new File("target/test-data/test-impermanent-db").deleteAll()
    }
  }

  test("dropIndex") {
    // GIVEN
    execute("CREATE INDEX ON :Person(name)")

    // WHEN
    execute("DROP INDEX ON :Person(name)")

    // THEN
    graph.inTx {
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
    val testDirectory = TestDirectory.testDirectory()
    testDirectory.prepareDirectory(getClass, "createDbWithFailedIndex")
    val storeDir = testDirectory.databaseDir()
    graph.shutdown()
    val dbFactory = new TestGraphDatabaseFactory()
    // Build a properly failing index provider which is a wrapper around the default provider, but which throws exception
    // in its populator when trying to add updates to it
    val providerFactory = new FailingGenericNativeIndexProviderFactory(POPULATION)
    dbFactory.removeKernelExtensions(TestGraphDatabaseFactory.INDEX_PROVIDERS_FILTER)
    dbFactory.addKernelExtension(providerFactory)
    graph = new GraphDatabaseCypherService(dbFactory.newEmbeddedDatabase(storeDir))
    eengine = createEngine(graph)
    execute("create (:Person {name:42})")
    execute("CREATE INDEX ON :Person(name)")
    val tx = graph.getGraphDatabaseService.beginTx()
    try {
      graph.schema().awaitIndexesOnline(3, TimeUnit.SECONDS)
      tx.success()
    } catch {
      case e:IllegalStateException => assert(e.getMessage.contains("FAILED"), "Was expecting FAILED state")
    } finally {
      tx.close()
    }
    graph.getGraphDatabaseService
  }
}
