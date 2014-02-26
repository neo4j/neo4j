/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.cypher.internal.commons.CreateTempFileTestSupport
import org.neo4j.cypher.internal.helpers.TxCounts
import java.io.PrintWriter
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.StringHelper.RichString

class PeriodicCommitLoadCsvAcceptanceTest
  extends ExecutionEngineFunSuite with CreateTempFileTestSupport with TxCountsTrackingTestSupport {

  test("should commit on row boundaries only") {
    // given
    val url = createFile(writer => {
      writer.println("1,2")
      writer.println("3,4")
      writer.println("4,5")
    })

    val queryText =
      s"USING PERIODIC COMMIT 3 LOAD CSV FROM '${url}' AS line CREATE ({name: line[0]}) CREATE ({name: line[1]})"

    // when
    val txCounts = executeAndTrackTxCounts(queryText)

    // then
    txCounts should equal(TxCounts(commits = 3))
  }

  test("should commit if the updates per row are twice the periodic commit size") {
    // given
    val url = createFile(writer => {
      writer.println("1,2,3,4,5")
    })

    val queryText =
      s"USING PERIODIC COMMIT 4 LOAD CSV FROM '${url}' AS line" +
        " CREATE ({name: line[0]})" +
        " CREATE ({name: line[1]})" +
        " CREATE ({name: line[2]})" +
        " CREATE ({name: line[3]})" +
        " CREATE ({name: line[4]})"

    // when
    val txCounts = executeAndTrackTxCounts(queryText)

    // then
    txCounts should equal(TxCounts(commits = 2))
  }

  test("in case of multiple load csv clauses the more external load csv will delay the commits") {
    // given
    val url = createFile(writer => {
      writer.println("1,2")
      writer.println("2,3")
    })

    val queryText =
       "USING PERIODIC COMMIT 8" +
        s" LOAD CSV FROM '${url}' AS l1" +
        s" CREATE ({name: l1[0]})" +
        s" CREATE ({name: l1[1]})" +
        s" WITH *" +
        s" LOAD CSV FROM '${url}' AS l2" +
        s" CREATE ({name: l2[1]})" +
        s" CREATE ({name: l2[0]})"

    // when
    val txCounts = executeAndTrackTxCounts(queryText)

    // then
    txCounts should equal(TxCounts(commits = 2))
  }

  test("should give up on row boundary alignment in case of union") {
    // given
    val url = createFile(writer => {
      writer.println("1")
      writer.println("2")
    })

    val queryText =
      s"USING PERIODIC COMMIT 1 LOAD CSV FROM '${url}' AS line" +
        " CREATE ({name: line[0]})" +
        "UNION CREATE({name: 123})"

    // when
    val txCounts = executeAndTrackTxCounts(queryText)

    // then
    txCounts should equal(TxCounts(commits = 7))
  }

  test("should give up on row boundary alignment in case of aggregation") {
    // given
    val url = createFile(writer => {
      writer.println("1")
      writer.println("2")
    })

    val queryText =
      s"USING PERIODIC COMMIT 1 LOAD CSV FROM '${url}' AS line" +
        " CREATE ({name: line[0]})" +
        "RETURN count(*)"

    // when
    val txCounts = executeAndTrackTxCounts(queryText)

    // then
    txCounts should equal(TxCounts(commits = 5))
  }

  test("should commit on row boundary even when some part of the query fails") {
    // given
    val url = createFile(writer => {
      writer.println("1")
      writer.println("2")
      writer.println("0")
      writer.println("3")
    })

    val queryText =
      s"USING PERIODIC COMMIT 1 LOAD CSV FROM '${url}' AS line " +
        s"CREATE ({name: 1/toInt(line[0])})"

    // when
    val (_, txCounts) = prepareAndTrackTxCounts(intercept[CypherException](execute(queryText)))

    // then
    txCounts should equal(TxCounts(commits = 2, rollbacks = 1))
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
      s"USING PERIODIC COMMIT 1 LOAD CSV FROM '${url}' AS line " +
      s"CREATE ({name: 1/toInt(line[0])})"

    // when
    val e = intercept[CypherException](execute(queryText))

    // then
    e.getMessage.contains("on line 3. Possibly the last row committed during import is line 2. Note that this information might not be accurate.") should equal(true)
  }

  private def createFile(f: PrintWriter => Unit) = createTempFileURL("cypher", ".csv", f).cypherEscape
}
