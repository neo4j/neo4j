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
package org.neo4j.internal.cypher.acceptance

import java.io.PrintWriter

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CreateTempFileTestSupport
import org.neo4j.cypher.internal.helpers.TxCounts
import org.neo4j.cypher.internal.frontend.v2_3.helpers.StringHelper.RichString
import org.neo4j.graphdb.Node

class PeriodicCommitAcceptanceTest extends ExecutionEngineFunSuite
  with TxCountsTrackingTestSupport with QueryStatisticsTestSupport
  with CreateTempFileTestSupport {

  def unwrapLoadCSVStatus[T](f: => T) = {
    try {
      f
    }
    catch {
      case e: LoadCsvStatusWrapCypherException => throw e.getCause
    }
  }

  def createTempCSVFile(numberOfLines: Int): String =
    createTempFileURL("file", ".csv") { writer: PrintWriter =>
      1.to(numberOfLines).foreach { n: Int => writer.println(n.toString) }
    }

  private def createFile(f: PrintWriter => Unit) = createTempFileURL("cypher", ".csv")(f).cypherEscape

  test("should reject periodic commit when not followed by LOAD CSV") {
    evaluating {
      executeScalar("USING PERIODIC COMMIT 200 MATCH (n) RETURN count(n)")
    } should produce[SyntaxException]
  }

  test("should produce data from periodic commit") {
    val url = createTempFileURL("foo", ".csv") { writer: PrintWriter =>
      writer.println("42")
    }
    val result = execute(s"USING PERIODIC COMMIT 200 LOAD CSV FROM '$url' AS line CREATE (n {id: line[0]}) RETURN n.id")

    result.toList should equal(List(Map("n.id" -> "42")))
    result.columns should equal(List("n.id"))
  }

  test("should support simple periodic commit") {
    // given
    val url = createTempCSVFile(5)
    val queryText =
      "USING PERIODIC COMMIT 2 " +
      s"LOAD CSV FROM '$url' AS line " +
      "CREATE ()"

    // when
    val (result, txCounts) = executeAndTrackTxCounts(queryText)

    // then
    assertStats(result, nodesCreated = 5)

    // and then
    txCounts should equal(TxCounts(commits = 3))
  }

  test("should support simple periodic commit with unaligned batch size") {
    // given
    val url = createTempCSVFile(4)
    val queryText =
      "USING PERIODIC COMMIT 3 " +
      s"LOAD CSV FROM '$url' AS line " +
      "CREATE ()"

    // when
    val (result, txCounts) = executeAndTrackTxCounts(queryText)

    // then
    assertStats(result, nodesCreated = 4)

    // and then
    txCounts should equal(TxCounts(commits = 2))
  }

  test("should abort first tx when failing on first batch during periodic commit") {
    // given
    val url = createTempCSVFile(20)
    val queryText = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line CREATE ({x: (toInt(line[0]) - 8)/0})"

    // when
    val (_, txCounts) = prepareAndTrackTxCounts(intercept[ArithmeticException](
      unwrapLoadCSVStatus(executeScalar[Number](queryText))
    ))

    // then
    txCounts should equal(TxCounts(rollbacks = 1))
  }

  test("should not mistakenly use closed statements") {
    // given
    val url = createTempCSVFile(20)
    val queryText = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line MERGE (:Label);"

    // when
    val (_, txCounts) = executeAndTrackTxCounts(queryText)

    // then
    txCounts should equal(TxCounts(commits = 2, rollbacks = 0))
  }

  test("should commit first tx and abort second tx when failing on second batch during periodic commit") {
    // given
    val url = createTempCSVFile(20)
    val queryText = s"USING PERIODIC COMMIT 10 LOAD CSV FROM '$url' AS line CREATE ({x: 1 / (toInt(line[0]) - 16)})"

    // when
    val (_, txCounts) = prepareAndTrackTxCounts(intercept[ArithmeticException](
      unwrapLoadCSVStatus(executeScalar[Number](queryText))
    ))

    // then
    txCounts should equal(TxCounts(commits = 1, rollbacks = 1))
  }

  test("should support periodic commit hint without explicit size") {
    val url = createTempCSVFile(1)
    executeScalar[Node](s"USING PERIODIC COMMIT LOAD CSV FROM '$url' AS line CREATE (n) RETURN n")
  }

  test("should support periodic commit hint with explicit size") {
    val url = createTempCSVFile(1)
    executeScalar[Node](s"USING PERIODIC COMMIT 400 LOAD CSV FROM '$url' AS line CREATE (n) RETURN n")
  }

  test("should reject periodic commit hint with negative size") {
    val url = createTempCSVFile(1)
    evaluating {
      executeScalar[Node](s"USING PERIODIC COMMIT -1 LOAD CSV FROM '$url' AS line CREATE (n) RETURN n")
    } should produce[SyntaxException]
  }

  test("should fail if periodic commit is executed in an open transaction") {
    // given
    evaluating {
      val url = createTempCSVFile(3)
      graph.inTx {
        execute(s"USING PERIODIC COMMIT LOAD CSV FROM '$url' AS line CREATE ()")
      }
    } should produce[PeriodicCommitInOpenTransactionException]
  }

  test("should tell line number information when failing using periodic commit and load csv") {
    // given
    val url = createFile(writer => {
      writer.println("1")
      writer.println("2")
      writer.println("0")
      writer.println("3")
    })

    val queryText =
      s"USING PERIODIC COMMIT 1 LOAD CSV FROM '$url' AS line " +
        s"CREATE ({name: 1/toInt(line[0])})"

    // when executing 5 updates
    val e = intercept[CypherException](execute(queryText))

    // then
    e.getMessage should include("on line 3. Possibly the last row committed during import is line 2. Note that this information might not be accurate.")
  }
}
