/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.runtime.spec.tests

import java.io.PrintWriter

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ir.HasHeaders
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.plans.Prober.Probe
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RowCount
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.immutable

abstract class LoadCsvTestBase[CONTEXT <: RuntimeContext](
                                                         edition: Edition[CONTEXT],
                                                         runtime: CypherRuntime[CONTEXT],
                                                         sizeHint: Int
                                                       ) extends RuntimeTestSuite[CONTEXT](edition, runtime)
  with CreateTempFileTestSupport {

  protected val testRange: immutable.Seq[Int] = 0 until sizeHint/2
  protected val testValueOffset = 10000

  private def wrapInQuotations(c: String): String = "\"" + c + "\""

  def singleColumnCsvFile(withHeaders: Boolean = false): String = {
    val url = createCSVTempFileURL({ writer: PrintWriter =>
      if (withHeaders)
        writer.println("a")
      testRange.foreach { i =>
        writer.println(s"${testValueOffset + i}")
      }
    }).cypherEscape
    wrapInQuotations(url)
  }

  def multipleColumnCsvFile(withHeaders: Boolean = false): String = {
    val url = createCSVTempFileURL({ writer: PrintWriter =>
      if (withHeaders)
        writer.println("a,b,c")
      testRange.foreach { i =>
        writer.println(s"${testValueOffset + i},${testValueOffset*2 + i},${testValueOffset*3 + i}")
      }
    }).cypherEscape
    wrapInQuotations(url)
  }

  test("should load csv file") {
    // given
    val url = singleColumnCsvFile()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("line[0] as x")
      .loadCSV(url, variableName = "line", NoHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = testRange.map { i => Array(s"${testValueOffset + i}") }.toArray
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should load multiple column csv file") {
    // given
    val url = multipleColumnCsvFile()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .projection("line[0] as x", "line[1] as y", "line[2] as z")
      .loadCSV(url, variableName = "line", NoHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = testRange.map { i => Array(s"${testValueOffset + i}", s"${testValueOffset*2 + i}", s"${testValueOffset*3 + i}") }.toArray
    runtimeResult should beColumns("x", "y", "z").withRows(expected)
  }

  test("should load csv file with headers") {
    // given
    val url = singleColumnCsvFile(withHeaders = true)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("line.a as x")
      .loadCSV(url, variableName = "line", HasHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = testRange.map { i => Array(s"${testValueOffset + i}") }.toArray
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should load multiple column csv file with headers") {
    // given
    val url = multipleColumnCsvFile(withHeaders = true)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .projection("line.a as x", "line.b as y", "line.c as z")
      .loadCSV(url, variableName = "line", HasHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = testRange.map { i => Array(s"${testValueOffset + i}", s"${testValueOffset*2 + i}", s"${testValueOffset*3 + i}") }.toArray
    runtimeResult should beColumns("x", "y", "z").withRows(expected)
  }

  test("should load csv file with aggregation") {
    // given
    val url = singleColumnCsvFile()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(x) AS count"))
      .projection("line[0] as x")
      .loadCSV(url, variableName = "line", NoHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("count").withSingleRow(testRange.size.toLong)
  }

  test("should load csv file on RHS of apply") {
    // given
    val url = singleColumnCsvFile()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.projection("line[0] as x")
      .|.loadCSV(url, variableName = "line", NoHeaders)
      .|.argument()
      .unwind("[1, 2, 3] as i")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected1 = testRange.map { i => Array(s"${testValueOffset + i}") }.toArray
    val expected = Array.fill(3)(expected1).flatten
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should load csv file on RHS of apply with aggregation") {
    // given
    val url = singleColumnCsvFile()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("count")
      .apply()
      .|.aggregation(Seq.empty, Seq("count(x) AS count"))
      .|.projection("line[0] as x")
      .|.loadCSV(url, variableName = "line", NoHeaders)
      .|.argument()
      .unwind("[1, 2, 3] as i")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array.fill(3)(Array(testRange.size.toLong))
    runtimeResult should beColumns("count").withRows(expected)
  }

  test("should load csv file on RHS of apply with aggregation on top") {
    // given
    val url = singleColumnCsvFile()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(x) AS count"))
      .apply()
      .|.projection("line[0] as x")
      .|.loadCSV(url, variableName = "line", NoHeaders)
      .|.argument()
      .unwind("[1, 2, 3] as i")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("count").withSingleRow(testRange.size.toLong * 3)
  }

  test("should load csv file with value hash join") {
    // given
    val url1 = multipleColumnCsvFile(withHeaders = true)
    val url2 = multipleColumnCsvFile()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "a", "b")
      .valueHashJoin("x = y")
      .|.projection("rline[0] as y",  "rline[2] as b")
      .|.loadCSV(url2, variableName = "rline", NoHeaders)
      .|.argument()
      .projection("lline.a as x", "lline.b as a")
      .loadCSV(url1, variableName = "lline", HasHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = testRange.map { i =>
      Array(s"${testValueOffset + i}", s"${testValueOffset + i}", s"${testValueOffset*2 + i}", s"${testValueOffset*3 + i}") }.toArray
    runtimeResult should beColumns("x", "y", "a", "b").withRows(expected)
  }

  test("should load csv create node with properties") {
    // given an empty data base
    val url = multipleColumnCsvFile(withHeaders = true)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .create(createNodeWithProperties("n", Seq("A"), "{p1: line.a, p2: line.b, p3: line.c}"))
      .loadCSV(url, variableName = "line", HasHeaders)
      .argument()
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val nodes = tx.getAllNodes.asScala.toArray
    val expected = nodes.map(Array(_))
    runtimeResult should beColumns("n")
      .withRows(expected)
      .withStatistics(nodesCreated = testRange.size, labelsAdded = testRange.size, propertiesSet = testRange.size * 3)

    nodes.map { _.getAllProperties.asScala } should contain theSameElementsAs
      testRange.map { i => Map("p1" -> s"${testValueOffset + i}", "p2" -> s"${testValueOffset*2 + i}", "p3" -> s"${testValueOffset*3 + i}") }
  }

  test("should load csv file with linenumber") {
    // given
    val url = singleColumnCsvFile()

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("line[0] as x", "linenumber() as y")
      .loadCSV(url, variableName = "line", NoHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = testRange.map { i => Array(s"${testValueOffset + i}", i.toLong + 1) }.toArray
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should load csv file with headers and linenumber") {
    // given
    val url = singleColumnCsvFile(withHeaders = true)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("line.a as x", "linenumber() as y")
      .loadCSV(url, variableName = "line", HasHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = testRange.map { i => Array(s"${testValueOffset + i}", i.toLong + 2) }.toArray
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should load csv file with linenumber and file") {
    // given
    val url = singleColumnCsvFile()
    val filename = url.replace("file:", "").replace("\"", "")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .projection("line[0] as x", "linenumber() as y", "file() as z")
      .loadCSV(url, variableName = "line", NoHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = testRange.map { i => Array(s"${testValueOffset + i}", i.toLong + 1, filename) }.toArray
    runtimeResult should beColumns("x", "y", "z").withRows(expected)
  }

  test("should load csv file with headers and linenumber and file") {
    // given
    val url = singleColumnCsvFile(withHeaders = true)
    val filename = url.replace("file:", "").replace("\"", "")

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .projection("line.a as x", "linenumber() as y", "file() as z")
      .loadCSV(url, variableName = "line", HasHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = testRange.map { i => Array(s"${testValueOffset + i}", i.toLong + 2, filename) }.toArray
    runtimeResult should beColumns("x", "y", "z").withRows(expected)
  }

  test("should load csv create node with properties with periodic commit") {
    // given an empty data base
    val url = multipleColumnCsvFile(withHeaders = true)

    // when
    val logicalQuery = new LogicalQueryBuilder(this,
                                               hasLoadCsv = true,
                                               periodicCommitBatchSize = Some(testRange.size / 5))
      .produceResults("n")
      .create(createNodeWithProperties("n", Seq("A"), "{p1: line.a, p2: line.b, p3: line.c}"))
      .loadCSV(url, variableName = "line", HasHeaders)
      .argument()
      .build(readOnly = false)

    val executablePlan = buildPlan(logicalQuery, runtime)

    val runtimeResult = execute(executablePlan, readOnly = false, periodicCommit = true)
    consume(runtimeResult)

    // then
    runtimeResult should beColumns("n")
      .withRows(RowCount(testRange.size))
      .withStatistics(nodesCreated = testRange.size, labelsAdded = testRange.size, propertiesSet = testRange.size * 3)
  }

  test("should load csv create node with properties with periodic commit and eager aggregation") {
    // given an empty data base
    val url = multipleColumnCsvFile(withHeaders = true)

    // when
    val logicalQuery = new LogicalQueryBuilder(this,
                                               hasLoadCsv = true,
                                               periodicCommitBatchSize = Some(testRange.size / 5))
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(n) AS count"))
      .create(createNodeWithProperties("n", Seq("A"), "{p1: line.a, p2: line.b, p3: line.c}"))
      .loadCSV(url, variableName = "line", HasHeaders)
      .argument()
      .build(readOnly = false)

    val executablePlan = buildPlan(logicalQuery, runtime)

    val runtimeResult = execute(executablePlan, readOnly = false, periodicCommit = true)
    consume(runtimeResult)

    // then
    runtimeResult should beColumns("count")
      .withSingleRow(testRange.size.toLong)
      .withStatistics(nodesCreated = testRange.size, labelsAdded = testRange.size, propertiesSet = testRange.size * 3)
  }

  Seq(1, 2, 3, 4, testRange.size/5, testRange.size).foreach { explicitBatchSize =>
    test(s"should load csv create node with properties with periodic commit with batch size $explicitBatchSize") {
      // given an empty data base
      val url = multipleColumnCsvFile(withHeaders = true)
      val periodicCommitBatchSize = explicitBatchSize

      // when
      var beforeTxId: Long = Long.MinValue
      val numberOfNewTokens = 4 // Creating new tokens will result in implicit transactions being committed

      val beforeProbe: Probe = new Probe() {
        override def onRow(row: AnyRef): Unit = {
          beforeTxId = runtimeTestSupport.getLastClosedTransactionId
        }
      }

      val txIdAssertingProbe: Probe = new Probe() {
        var seenRowsCount = 0

        override def onRow(row: AnyRef): Unit = {
          val lastTxId = runtimeTestSupport.getLastClosedTransactionId
          val expectedTxId = beforeTxId + numberOfNewTokens + (seenRowsCount / periodicCommitBatchSize)
          seenRowsCount += 1
          if (lastTxId != expectedTxId && beforeTxId != Long.MinValue) {
            // Beware that for some values on explicitBatchSize this may not match exactly and could be a false positive
            fail(s"Expected last closed transaction #$expectedTxId but was #$lastTxId at seen row $seenRowsCount with batch size $periodicCommitBatchSize")
          }
        }
      }

      val logicalQuery = new LogicalQueryBuilder(this,
                                                 hasLoadCsv = true,
                                                 periodicCommitBatchSize = Some(periodicCommitBatchSize))
        .produceResults("n")
        .prober(txIdAssertingProbe)
        .create(createNodeWithProperties("n", Seq("A"), "{p1: line.a, p2: line.b, p3: line.c}"))
        .loadCSV(url, variableName = "line", HasHeaders)
        .prober(beforeProbe)
        .argument()
        .build(readOnly = false)

      val executablePlan = buildPlan(logicalQuery, runtime)

      val runtimeResult = execute(executablePlan, readOnly = false, periodicCommit = true)
      consume(runtimeResult)

      // then
      runtimeResult should beColumns("n")
        .withRows(RowCount(testRange.size))
        .withStatistics(nodesCreated = testRange.size, labelsAdded = testRange.size, propertiesSet = testRange.size * 3)
    }
  }
}
