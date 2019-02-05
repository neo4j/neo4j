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
package org.neo4j.cypher.internal.runtime.spec

import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.runtime.{InputCursor, InputDataStream, NoInput}
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.{CypherRuntime, LogicalQuery, RuntimeContext}
import org.neo4j.cypher.result.{QueryResult, RuntimeResult}
import org.neo4j.graphdb.{GraphDatabaseService, Label, Node}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.{AnyValue, AnyValues}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{BeforeAndAfterEach, Tag}

import scala.collection.mutable.ArrayBuffer

object RuntimeTestSuite {
  val ANY_VALUE_ORDERING: Ordering[AnyValue] = Ordering.comparatorToOrdering(AnyValues.COMPARATOR)
}

/**
  * Contains helpers, matchers and graph handling to support runtime acceptance test,
  * meaning tests where the query is
  *
  *  - specified as a logical plan
  *  - executed on a real database
  *  - evaluated by it's results
  */
abstract class RuntimeTestSuite[CONTEXT <: RuntimeContext](edition: Edition[CONTEXT],
                                                           val runtime: CypherRuntime[CONTEXT])
  extends CypherFunSuite
  with AstConstructionTestSupport
    with BeforeAndAfterEach
{

  var graphDb: GraphDatabaseService = _
  var runtimeTestSupport: RuntimeTestSupport[CONTEXT] = _
  val ANY_VALUE_ORDERING = Ordering.comparatorToOrdering(AnyValues.COMPARATOR)

  override def beforeEach(): Unit = {
    graphDb = edition.graphDatabaseFactory.newImpermanentDatabase()
    runtimeTestSupport = new RuntimeTestSupport[CONTEXT](graphDb, edition)
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    graphDb.shutdown()
  }

  override def test(testName: String, testTags: Tag*)(testFun: => Unit): Unit = {
    super.test(s"[${runtime.name}] $testName", testTags:_*)(testFun)
  }

  // EXECUTE

  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              input: InputValues
             ): RuntimeResult =
    runtimeTestSupport.run(logicalQuery, runtime, input.stream(), (_, result) => result)

  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT]
             ): RuntimeResult =
    runtimeTestSupport.run(logicalQuery, runtime, NoInput, (_, result) => result)

  def executeAndContext(logicalQuery: LogicalQuery,
                        runtime: CypherRuntime[CONTEXT],
                        input: InputValues
                       ): (RuntimeResult, CONTEXT) =
    runtimeTestSupport.run(logicalQuery, runtime, input.stream(), (context, result) => (result, context))

  def executeUntil(logicalQuery: LogicalQuery,
                   input: InputValues,
                   condition: ContextCondition[CONTEXT]): RuntimeResult = {
    val nAttempts = 100
    for (i <- 0 until nAttempts) {
      val (result, context) = executeAndContext(logicalQuery, runtime, input)
      if (condition.test(context)) {
        return result
      }
    }
    fail(s"${condition.errorMsg} in $nAttempts attempts!")
  }

  // INPUT

  val NO_INPUT = new InputValues

  def inputValues(rows: Array[Any]*): InputValues =
    new InputValues().and(rows:_*)

  def inputSingleColumn(nBatches: Int, batchSize: Int, valueFunction: Int => Any): InputValues = {
    val input = new InputValues()
    for (batch <- 0 until nBatches) {
      val rows =
        for (row <- 0 until batchSize)
          yield Array(valueFunction(batch * batchSize + row))
      input.and(rows:_*)

    }
    input
  }

  class InputValues() {
    val batches = new ArrayBuffer[IndexedSeq[Array[Any]]]
    def and(rows: Array[Any]*): InputValues = {
      batches += rows.toIndexedSeq
      this
    }
    def flatten: IndexedSeq[Array[Any]] =
      batches.flatten

    def stream(): InputDataStream = new BufferInputStream(batches.map(_.map(row => row.map(ValueUtils.of))))
  }

  class BufferInputStream(data: ArrayBuffer[IndexedSeq[Array[AnyValue]]]) extends InputDataStream {
    private var batchIndex = new AtomicInteger(0)
    override def nextInputBatch(): InputCursor = {
      val i = batchIndex.getAndIncrement()
      if (i < data.size)
        new BufferInputCursor(data(i))
      else
        null
    }
  }

  class BufferInputCursor(data: IndexedSeq[Array[AnyValue]]) extends InputCursor {
    private var i = -1

    override def next(): Boolean = {
      i += 1
      i < data.size
    }

    override def value(offset: Int): AnyValue =
      data(i)(offset)

    override def close(): Unit = {}
  }

  // GRAPHS

  def nodeGraph(nNodes: Int, labels: String*): Seq[Node] = {
    val tx = graphDb.beginTx()
    try {
      tx.success()
      for (_ <- 0 until nNodes) yield {
        graphDb.createNode(labels.map(Label.label):_*)
      }
    } finally tx.close()
  }

  def nodePropertyGraph(nNodes: Int, properties: PartialFunction[Int, Map[String, Any]], labels: String*): Seq[Node] = {
    val tx = graphDb.beginTx()
    try {
      tx.success()
      for (i <- 0 until nNodes) yield {
        val node = graphDb.createNode(labels.map(Label.label):_*)
        properties.runWith(_.foreach(kv => node.setProperty(kv._1, kv._2)))(i)
        node
      }
    } finally tx.close()
  }

  // MATCHERS

  protected def beColumns(columns: String*): RuntimeResultMatcher =
    new RuntimeResultMatcher(columns)

  class RuntimeResultMatcher(expectedColumns: Seq[String]) extends Matcher[RuntimeResult] {

    val expectedRows = new ArrayBuffer[Array[AnyValue]]
    var maybeResultMatcher: Option[Matcher[Seq[Array[AnyValue]]]] = None

    def withRow(values: Any*): RuntimeResultMatcher = {
      rowModifier()
      expectedRows += values.toArray.map(ValueUtils.of)
      this
    }

    def withRows[T](rows: Seq[Array[T]]): RuntimeResultMatcher = {
      rowModifier()
      expectedRows ++= rows.map(row => row.map(ValueUtils.of))
      this
    }

    def withSingleValueRows(values: Seq[Any]): RuntimeResultMatcher = {
      rowModifier()
      expectedRows ++= values.map(x => Array(ValueUtils.of(x)))
      this
    }

    def withNoRows(): RuntimeResultMatcher = {
      rowModifier()
      this
    }

    private def rowModifier(): Unit = {
      if (maybeResultMatcher.isDefined) {
        throw new IllegalArgumentException("Cannot use both `withRow` and `withResultMatching`")
      }
    }

    def withResultMatching(func: PartialFunction[Any, _]): RuntimeResultMatcher = {
      if (expectedRows.nonEmpty) {
        throw new IllegalArgumentException("Cannot use both `withRow` and `withResultMatching`")
      }
      maybeResultMatcher = Some(matchPattern(func))
      this
    }

    override def apply(left: RuntimeResult): MatchResult = {
      val columns = left.fieldNames().toSeq
      if (columns != expectedColumns)
        MatchResult(matches = false, s"Expected result columns $expectedColumns, got $columns", "")
      else {
        val rows = new ArrayBuffer[Array[AnyValue]]
        left.accept(new QueryResult.QueryResultVisitor[Exception] {
          override def visit(row: QueryResult.Record): Boolean = {
            val valueArray = row.fields()
            rows += util.Arrays.copyOf(valueArray, valueArray.length)
            true
          }
        })
        maybeResultMatcher match {
          case Some(matcher) => matcher(rows)
          case None =>
            MatchResult(
              equalWithoutOrder(expectedRows, rows),
              s"""Expected rows:
                 |
                 |${pretty(expectedRows)}
                 |
                 |but got
                 |
                 |${pretty(rows)}""".stripMargin,
              ""
            )
        }
      }
    }

    private def pretty(a: ArrayBuffer[Array[AnyValue]]): String = {
      val sb = new StringBuilder
      for (row <- a)
        sb ++= row.map(value => value.toString).mkString("", ", ", "\n")
      sb.result()
    }

    private def equalWithoutOrder(a: ArrayBuffer[Array[AnyValue]], b: ArrayBuffer[Array[AnyValue]]): Boolean = {

      if (a.size != b.size)
        return false

      val sortedA = a.map(row => VirtualValues.list(row:_*)).sorted(ANY_VALUE_ORDERING)
      val sortedB = b.map(row => VirtualValues.list(row:_*)).sorted(ANY_VALUE_ORDERING)

      sortedA == sortedB
    }

    private def equalInOrder(a: ArrayBuffer[Array[AnyValue]], b: ArrayBuffer[Array[AnyValue]]): Boolean = {

      if (a.size != b.size)
        return false

      val sortedA = a.map(row => VirtualValues.list(row:_*))
      val sortedB = b.map(row => VirtualValues.list(row:_*))

      sortedA == sortedB
    }
  }
}

case class ContextCondition[CONTEXT <: RuntimeContext](test: CONTEXT => Boolean, errorMsg: String)
