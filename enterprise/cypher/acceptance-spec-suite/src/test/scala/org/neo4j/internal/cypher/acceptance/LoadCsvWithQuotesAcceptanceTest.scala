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
package org.neo4j.internal.cypher.acceptance

import java.io.PrintWriter

import org.neo4j.csv.reader.MissingEndQuoteException
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.{CreateTempFileTestSupport, InternalExecutionResult}
import org.neo4j.cypher.{ExecutionEngineFunSuite, ExecutionEngineHelper, RunWithConfigTestSupport}
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.values.virtual.VirtualValues

class LoadCsvWithQuotesAcceptanceTest extends ExecutionEngineFunSuite with RunWithConfigTestSupport with CreateTempFileTestSupport {
  def csvUrls(f: PrintWriter => Unit) = Seq(
    createCSVTempFileURL(f),
    createGzipCSVTempFileURL(f),
    createZipCSVTempFileURL(f)
  )

  override protected def initTest(): Unit = ()

  override protected def stopTest(): Unit = ()

  test("import rows with messy quotes using legacy mode as default") {
    runWithConfig() { db =>
      val urls = csvUrls({
        writer =>
          writer.println("name,x")
          writer.println("'Quotes 0',\"\"")
          writer.println("'Quotes 1',\"\\\"\"")
          writer.println("'Quotes 2',\"\"\"\"")
          writer.println("'Quotes 3',\"\\\"\\\"\"")
          writer.println("'Quotes 4',\"\"\"\"\"\"")
      })
      for (url <- urls) {
        val result = executeWithCustomDb(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
        assert(result.toList === List(
          Map("line.x" -> ""),
          Map("line.x" -> "\""),
          Map("line.x" -> "\""),
          Map("line.x" -> "\"\""),
          Map("line.x" -> "\"\"")
        ))
      }
    }
  }

  test("import rows with messy quotes using legacy mode") {
    runWithConfig(GraphDatabaseSettings.csv_legacy_quote_escaping -> "true") { db =>
      val urls = csvUrls({
        writer =>
          writer.println("name,x")
          writer.println("'Quotes 0',\"\"")
          writer.println("'Quotes 1',\"\\\"\"")
          writer.println("'Quotes 2',\"\"\"\"")
          writer.println("'Quotes 3',\"\\\"\\\"\"")
          writer.println("'Quotes 4',\"\"\"\"\"\"")
      })
      for (url <- urls) {
        val result = executeWithCustomDb(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
        assert(result.toList === List(
          Map("line.x" -> ""),
          Map("line.x" -> "\""),
          Map("line.x" -> "\""),
          Map("line.x" -> "\"\""),
          Map("line.x" -> "\"\"")
        ))
      }
    }
  }

  test("import rows with messy quotes using rfc4180 mode") {
    runWithConfig(GraphDatabaseSettings.csv_legacy_quote_escaping -> "false") { db =>
      val urls = csvUrls({
        writer =>
          writer.println("name,x")
          writer.println("'Quotes 0',\"\"")
          writer.println("'Quotes 2',\"\"\"\"")
          writer.println("'Quotes 4',\"\"\"\"\"\"")
          writer.println("'Quotes 5',\"\\\"\"\"")
      })
      for (url <- urls) {
        val result = executeWithCustomDb(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
        assert(result.toList === List(
          Map("line.x" -> ""),
          Map("line.x" -> "\""),
          Map("line.x" -> "\"\""),
          Map("line.x" -> "\\\"")
        ))
      }
    }
  }

  test("fail to import rows with java quotes when in rfc4180 mode") {
    runWithConfig(GraphDatabaseSettings.csv_legacy_quote_escaping -> "false") { db =>
      val urls = csvUrls({
        writer =>
          writer.println("name,x")
          writer.println("'Quotes 0',\"\"")
          writer.println("'Quotes 1',\"\\\"\"")
          writer.println("'Quotes 2',\"\"\"\"")
      })
      for (url <- urls) {
        intercept[MissingEndQuoteException] {
          executeWithCustomDb(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
        }.getMessage should include("which started on line 2")
      }
    }
  }

  def executeWithCustomDb(db: GraphDatabaseCypherService, query: String): InternalExecutionResult = {
    val engine = ExecutionEngineHelper.createEngine(db)
    RewindableExecutionResult(engine.execute(query,
                                             VirtualValues.emptyMap(),
                                             engine.queryService.transactionalContext(query = query -> Map())))
  }

}
