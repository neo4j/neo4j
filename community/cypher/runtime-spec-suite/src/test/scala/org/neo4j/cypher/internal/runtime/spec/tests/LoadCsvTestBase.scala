/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.ir.HasHeaders
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RowCount
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString

import java.io.PrintWriter

import scala.collection.immutable
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

abstract class LoadCsvTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime)
    with CreateTempFileTestSupport {

  protected val testRange: immutable.Seq[Int] = 0 until sizeHint / 2
  protected val testValueOffset = 10000

  def wrapInQuotations(c: String): String = "\"" + c + "\""

  def singleColumnCsvFile(withHeaders: Boolean = false): String = {
    val url = createCSVTempFileURL({ writer: PrintWriter =>
      if (withHeaders) {
        writer.println("a")
      }
      testRange.foreach { i =>
        writer.println(s"${testValueOffset + i}")
      }
    }).cypherEscape
    wrapInQuotations(url)
  }

  def multipleColumnCsvFile(withHeaders: Boolean = false): String = {
    val url = createCSVTempFileURL({ writer: PrintWriter =>
      if (withHeaders) {
        writer.println("a,b,c")
      }
      testRange.foreach { i =>
        writer.println(s"${testValueOffset + i},${testValueOffset * 2 + i},${testValueOffset * 3 + i}")
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
    val expected = testRange.map { i =>
      Array(s"${testValueOffset + i}", s"${testValueOffset * 2 + i}", s"${testValueOffset * 3 + i}")
    }.toArray
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
    val expected = testRange.map { i =>
      Array(s"${testValueOffset + i}", s"${testValueOffset * 2 + i}", s"${testValueOffset * 3 + i}")
    }.toArray
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
      .|.projection("rline[0] as y", "rline[2] as b")
      .|.loadCSV(url2, variableName = "rline", NoHeaders)
      .|.argument()
      .projection("lline.a as x", "lline.b as a")
      .loadCSV(url1, variableName = "lline", HasHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = testRange.map { i =>
      Array(
        s"${testValueOffset + i}",
        s"${testValueOffset + i}",
        s"${testValueOffset * 2 + i}",
        s"${testValueOffset * 3 + i}"
      )
    }.toArray
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

    val allNodes = tx.getAllNodes
    try {
      // then
      val nodes = allNodes.asScala.toArray
      val expected = nodes.map(Array(_))
      runtimeResult should beColumns("n")
        .withRows(expected)
        .withStatistics(nodesCreated = testRange.size, labelsAdded = testRange.size, propertiesSet = testRange.size * 3)

      nodes.map { _.getAllProperties.asScala } should contain theSameElementsAs
        testRange.map { i =>
          Map(
            "p1" -> s"${testValueOffset + i}",
            "p2" -> s"${testValueOffset * 2 + i}",
            "p3" -> s"${testValueOffset * 3 + i}"
          )
        }
    } finally {
      allNodes.close()
    }
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
    val expected = testRange.map { i => Array[Any](s"${testValueOffset + i}", i.toLong + 1) }.toArray
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
    val expected = testRange.map { i => Array[Any](s"${testValueOffset + i}", i.toLong + 2) }.toArray
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
    val expected = testRange.map { i => Array[Any](s"${testValueOffset + i}", i.toLong + 1, filename) }.toArray
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
    val expected = testRange.map { i => Array[Any](s"${testValueOffset + i}", i.toLong + 2, filename) }.toArray
    runtimeResult should beColumns("x", "y", "z").withRows(expected)
  }
}

// Currently not supported in pipelined
trait LoadCsvWithCallInTransactions[CONTEXT <: RuntimeContext] {
  self: LoadCsvTestBase[CONTEXT] =>

  test("should load csv create node with properties with call in tx") {
    // given an empty data base
    val url = multipleColumnCsvFile(withHeaders = true)

    // when
    val logicalQuery = new LogicalQueryBuilder(this, hasLoadCsv = true)
      .produceResults("n")
      .transactionApply(testRange.size / 5)
      .|.create(createNodeWithProperties("n", Seq("A"), "{p1: line.a, p2: line.b, p3: line.c}"))
      .|.argument("line")
      .loadCSV(url, variableName = "line", HasHeaders)
      .argument()
      .build(readOnly = false)

    val executablePlan = buildPlan(logicalQuery, runtime)

    val runtimeResult = execute(executablePlan, readOnly = false, implicitTx = true)

    // then
    runtimeResult should beColumns("n")
      .withRows(RowCount(testRange.size))
      .withStatistics(
        nodesCreated = testRange.size,
        labelsAdded = testRange.size,
        propertiesSet = testRange.size * 3,
        transactionsStarted = 6,
        transactionsCommitted = 6
      )
  }

  test("should load csv create node with properties with call in tx and eager aggregation") {
    // given an empty data base
    val url = multipleColumnCsvFile(withHeaders = true)

    // when
    val logicalQuery = new LogicalQueryBuilder(this, hasLoadCsv = true)
      .produceResults("count")
      .aggregation(Seq.empty, Seq("count(n) AS count"))
      .transactionApply(testRange.size / 5)
      .|.create(createNodeWithProperties("n", Seq("A"), "{p1: line.a, p2: line.b, p3: line.c}"))
      .|.argument("line")
      .loadCSV(url, variableName = "line", HasHeaders)
      .argument()
      .build(readOnly = false)

    val executablePlan = buildPlan(logicalQuery, runtime)

    val runtimeResult = execute(executablePlan, readOnly = false, implicitTx = true)

    // then
    runtimeResult should beColumns("count")
      .withSingleRow(testRange.size.toLong)
      .withStatistics(
        nodesCreated = testRange.size,
        labelsAdded = testRange.size,
        propertiesSet = testRange.size * 3,
        transactionsStarted = 6,
        transactionsCommitted = 6
      )
  }

  test("should load csv file with one empty header") {
    // given
    val fileString = createCSVTempFileURL { writer: PrintWriter =>
      writer.println("a,b,")
      Range(0, 10).foreach(i => writer.println(s"$i,${2 * i},${3 * i}"))
    }
    val url = wrapInQuotations(fileString.cypherEscape)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("line[''] as x")
      .loadCSV(url, variableName = "line", HasHeaders)
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Range(0, 10).map(i => Array((i * 3L).toString))
    runtimeResult should beColumns("x").withRows(expected)
  }
}

//Merge is supported in fused only so we need to break this one out
trait LoadCsvWithCallInTransactionsAndMerge[CONTEXT <: RuntimeContext] {
  self: LoadCsvTestBase[CONTEXT] =>

  test("should close open cursors on call in tx - scenario") {
    val url = multipleColumnCsvFile(withHeaders = true)

    given {
      nodeIndex("L", "prop")
    }

    val setupQuery = new LogicalQueryBuilder(this, hasLoadCsv = true)
      .produceResults("a", "b")
      .apply()
      .|.merge(Seq(createNodeWithProperties("b", Seq("L"), "{prop: row.b}")), Seq(), Seq(), Seq(), Set())
      .|.nodeIndexOperator(
        "b:L(prop = ???)",
        paramExpr = Some(prop("row", "b")),
        argumentIds = Set("row"),
        getValue = Map("prop" -> DoNotGetValue)
      )
      .eager()
      .apply()
      .|.merge(Seq(createNodeWithProperties("a", Seq("L"), "{prop: row.a}")), Seq(), Seq(), Seq(), Set())
      .|.nodeIndexOperator(
        "a:L(prop = ???)",
        paramExpr = Some(prop("row", "a")),
        argumentIds = Set("row"),
        getValue = Map("prop" -> DoNotGetValue)
      )
      .loadCSV(url, "row", HasHeaders, None)
      .argument()
      .build(readOnly = false)

    val setupResult = given {
      val setupPlan = buildPlan(setupQuery, runtime)
      val setupResult = execute(setupPlan, readOnly = false)
      consume(setupResult)
      setupResult
    }

    setupResult should beColumns("a", "b")
      .withRows(RowCount(testRange.size))
      .withStatistics(
        nodesCreated = testRange.size * 2,
        labelsAdded = testRange.size * 2,
        propertiesSet = testRange.size * 2
      )

    // when
    val logicalQuery = new LogicalQueryBuilder(this, hasLoadCsv = true)
      .produceResults("r")
      .transactionApply(2)
      .|.apply()
      .|.|.merge(Seq(), Seq(createRelationship("r", "a", "T", "b", OUTGOING)), Seq(), Seq(), Set("a", "b"))
      .|.|.filter("a = aa", "b = bb")
      .|.|.relationshipTypeScan("(aa)-[r:T]->(bb)", IndexOrderNone, "a", "b")
      .|.apply()
      .|.|.cartesianProduct()
      .|.|.|.nodeIndexOperator(
        "b:L(prop = ???)",
        paramExpr = Some(prop("row", "b")),
        argumentIds = Set("row"),
        getValue = Map("prop" -> DoNotGetValue)
      )
      .|.|.nodeIndexOperator(
        "a:L(prop = ???)",
        paramExpr = Some(prop("row", "a")),
        argumentIds = Set("row"),
        getValue = Map("prop" -> DoNotGetValue)
      )
      .|.argument("row")
      .loadCSV(url, "row", HasHeaders)
      .argument()
      .build(readOnly = false)

    val executablePlan = buildPlan(logicalQuery, runtime)

    val runtimeResult = execute(executablePlan, readOnly = false, implicitTx = true)

    // then
    runtimeResult should beColumns("r")
      .withRows(RowCount(testRange.size))
      .withStatistics(
        relationshipsCreated = testRange.size,
        transactionsStarted = testRange.size / 2 + 1,
        transactionsCommitted = testRange.size / 2 + 1
      )
  }

}
