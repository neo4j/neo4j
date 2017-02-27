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
package org.neo4j.internal.cypher.acceptance

import java.io.PrintWriter

import org.neo4j.csv.reader.MissingEndQuoteException
import org.neo4j.cypher.internal.{ExecutionEngine, RewindableExecutionResult}
import org.neo4j.cypher.internal.compatibility.ExecutionResultWrapperFor3_0
import org.neo4j.cypher.internal.compiler.v3_0.test_helpers.CreateTempFileTestSupport
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, RunWithConfigTestSupport}
import org.neo4j.graphdb.factory.GraphDatabaseSettings

class LoadCsvWithQuotesAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport with RunWithConfigTestSupport with CreateTempFileTestSupport {
  def csvUrls(f: PrintWriter => Unit) = Seq(
    createCSVTempFileURL(f),
    createGzipCSVTempFileURL(f),
    createZipCSVTempFileURL(f)
  )

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
        val result = executeUsingCostPlannerOnly(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
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
        val result = executeUsingCostPlannerOnly(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
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
        val result = executeUsingCostPlannerOnly(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
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
          executeUsingCostPlannerOnly(db, s"LOAD CSV WITH HEADERS FROM '$url' AS line RETURN line.x")
        }.getMessage should include("which started on line 2")
      }
    }
  }

  def executeUsingCostPlannerOnly(db: GraphDatabaseCypherService, query: String) =
    new ExecutionEngine(db).execute(s"CYPHER planner=COST $query", Map.empty[String, Any], db.session()) match {
      case e: ExecutionResultWrapperFor3_0 => RewindableExecutionResult(e)
    }

}
