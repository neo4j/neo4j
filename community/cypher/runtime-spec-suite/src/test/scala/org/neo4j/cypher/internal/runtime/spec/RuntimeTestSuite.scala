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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.runtime.{InputCursor, InputDataStream, NoInput, QueryStatistics}
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.{CypherRuntime, ExecutionPlan, LogicalQuery, RuntimeContext}
import org.neo4j.cypher.result.{QueryResult, RuntimeResult}
import org.neo4j.dbms.database.DatabaseManagementService
import org.neo4j.graphdb._
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.{AnyValue, AnyValues}
import org.scalactic.source.Position
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{BeforeAndAfterEach, Tag}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

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
  with TokenResolver {

  var managementService: DatabaseManagementService = _
  var graphDb: GraphDatabaseService = _
  var runtimeTestSupport: RuntimeTestSupport[CONTEXT] = _
  val ANY_VALUE_ORDERING: Ordering[AnyValue] = Ordering.comparatorToOrdering(AnyValues.COMPARATOR)

  final override def beforeEach(): Unit = {
    managementService = edition.newGraphManagementService()
    graphDb = managementService.database(DEFAULT_DATABASE_NAME)
    runtimeTestSupport = new RuntimeTestSupport[CONTEXT](managementService, graphDb, edition)
    super.beforeEach()
  }

  final override def afterEach(): Unit = {
    managementService.shutdown()
    afterShutdown()
  }

  def afterShutdown(): Unit = {}

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    super.test(testName, Tag(runtime.name) +: testTags: _*)(testFun)
  }

  // HELPERS

  override def labelId(label: String): Int = {
    val tx = graphDb.beginTx()
    try {
      tx.success()
      tx.asInstanceOf[InternalTransaction].kernelTransaction().tokenRead().nodeLabel(label)
    } finally tx.close()
  }

  override def propertyKeyId(prop: String): Int =  {
    val tx = graphDb.beginTx()
    try {
      tx.success()
      tx.asInstanceOf[InternalTransaction].kernelTransaction().tokenRead().propertyKey(prop)
    } finally tx.close()
  }

  def select[X](things: Seq[X],
                selectivity: Double = 1.0,
                duplicateProbability: Double = 0.0,
                nullProbability: Double = 0.0): Seq[X] = {
    val rng = new Random(42)
    for {thing<- things if rng.nextDouble() < selectivity
         dup <- if (rng.nextDouble() < duplicateProbability) Seq(thing, thing) else Seq(thing)
         nullifiedDup = if (rng.nextDouble() < nullProbability) null.asInstanceOf[X] else dup
    } yield nullifiedDup
  }

  // EXECUTE

  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              input: InputValues
             ): RuntimeResult =
    runtimeTestSupport.run(logicalQuery, runtime, input.stream(), (_, result) => result)

  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              inputStream: InputDataStream
             ): RuntimeResult =
    runtimeTestSupport.run(logicalQuery, runtime, inputStream, (_, result) => result)

  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT]
             ): RuntimeResult =
    runtimeTestSupport.run(logicalQuery, runtime, NoInput, (_, result) => result)

  def execute(executablePlan: ExecutionPlan): RuntimeResult =
    runtimeTestSupport.run(executablePlan, NoInput, (_, result) => result)

  def buildPlan(logicalQuery: LogicalQuery,
                runtime: CypherRuntime[CONTEXT]): ExecutionPlan =
    runtimeTestSupport.compile(logicalQuery, runtime)

  def executeAndContext(logicalQuery: LogicalQuery,
                        runtime: CypherRuntime[CONTEXT],
                        input: InputValues
                       ): (RuntimeResult, CONTEXT) =
    runtimeTestSupport.run(logicalQuery, runtime, input.stream(), (context, result) => (result, context))

  def executeAndAssertCondition(logicalQuery: LogicalQuery,
                                input: InputValues,
                                condition: ContextCondition[CONTEXT]): Unit = {
    val nAttempts = 100
    for (_ <- 0 until nAttempts) {
      val (result, context) = executeAndContext(logicalQuery, runtime, input)
      result.accept((_: QueryResult.Record) => true)
      if (condition.test(context))
        return
    }
    fail(s"${condition.errorMsg} in $nAttempts attempts!")
  }

  // INPUT

  val NO_INPUT = new InputValues

  def inputValues(rows: Array[Any]*): InputValues =
    new InputValues().and(rows: _*)

  def batchedInputValues(batchSize: Int, rows: Array[Any]*): InputValues = {
    val input = new InputValues()
    rows.grouped(batchSize).foreach(batch => input.and(batch: _*))
    input
  }

  //noinspection ScalaUnnecessaryParentheses
  def inputColumns(nBatches: Int, batchSize: Int, valueFunctions: (Int => Any)*): InputValues = {
    val input = new InputValues()
    for (batch <- 0 until nBatches) {
      val rows = for (row <- 0 until batchSize) yield valueFunctions.map(_(batch * batchSize + row)).toArray
      input.and(rows: _*)
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

    def stream(): BufferInputStream = new BufferInputStream(batches.map(_.map(row => row.map(ValueUtils.of))))
  }

  class BufferInputStream(data: ArrayBuffer[IndexedSeq[Array[AnyValue]]]) extends InputDataStream {
    private val batchIndex = new AtomicInteger(0)

    override def nextInputBatch(): InputCursor = {
      val i = batchIndex.getAndIncrement()
      if (i < data.size)
        new BufferInputCursor(data(i))
      else
        null
    }

    def hasMore: Boolean = batchIndex.get() < data.size
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

  def bipartiteGraph(nNodes: Int, aLabel: String, bLabel: String, relType: String): (Seq[Node], Seq[Node]) = {
    val aNodes = nodeGraph(nNodes, aLabel)
    val bNodes = nodeGraph(nNodes, bLabel)
    inTx {
      val relationshipType = RelationshipType.withName(relType)
      for {a <- aNodes; b <- bNodes} {
        a.createRelationshipTo(b, relationshipType)
      }
    }
    (aNodes, bNodes)
  }

  def nodeGraph(nNodes: Int, labels: String*): Seq[Node] = {
    inTx {
      for (_ <- 0 until nNodes) yield {
        graphDb.createNode(labels.map(Label.label): _*)
      }
    }
  }

  def circleGraph(nNodes: Int, labels: String*): (Seq[Node], Seq[Relationship]) = {
    val nodes = inTx {
      for (_ <- 0 until nNodes) yield {
        graphDb.createNode(labels.map(Label.label): _*)
      }
    }

    val rels = new ArrayBuffer[Relationship]
    inTx {
      val rType = RelationshipType.withName("R")
      for (i <- 0 until nNodes) {
        val a = nodes(i)
        val b = nodes((i + 1) % nNodes)
        rels += a.createRelationshipTo(b, rType)
      }
    }
    (nodes, rels)
  }

  def inTx[T](f: => T): T = {
    val tx = graphDb.beginTx()
    try {
      tx.success()
      f
    } finally tx.close()
  }

  def nodePropertyGraph(nNodes: Int, properties: PartialFunction[Int, Map[String, Any]], labels: String*): Seq[Node] = {
    val tx = graphDb.beginTx()
    try {
      tx.success()
      for (i <- 0 until nNodes) yield {
        val node = graphDb.createNode(labels.map(Label.label): _*)
        properties.runWith(_.foreach(kv => node.setProperty(kv._1, kv._2)))(i)
        node
      }
    } finally tx.close()
  }

  def connect(nodes: Seq[Node], rels: Seq[(Int, Int, String)]): Seq[Relationship] = {
    val tx = graphDb.beginTx()
    try {
      tx.success()
      rels.map {
        case (from, to, typ) =>
          nodes(from).createRelationshipTo(nodes(to), RelationshipType.withName(typ))
      }
    } finally tx.close()
  }

  // INDEXES

  def index(label: String, properties: String*): Unit = {
    var tx = graphDb.beginTx()
    try {
      tx.success()
      var creator = graphDb.schema().indexFor(Label.label(label))
      properties.foreach(p => creator = creator.on(p))
      creator.create()
    } finally tx.close()

    tx = graphDb.beginTx()
    try {
      tx.success()
      graphDb.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
    } finally tx.close()
  }

  def uniqueIndex(label: String, property: String): Unit = {
    var tx = graphDb.beginTx()
    try {
      tx.success()
      val creator = graphDb.schema().constraintFor(Label.label(label)).assertPropertyIsUnique(property)
      creator.create()
    } finally tx.close()

    tx = graphDb.beginTx()
    try {
      tx.success()
      graphDb.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
    } finally tx.close()
  }

  // MATCHERS

  private val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.0001)

  def tolerantEquals(expected: Double, x: Number): Boolean =
    doubleEquality.areEqual(expected, x.doubleValue())

  protected def beColumns(columns: String*): RuntimeResultMatcher =
    new RuntimeResultMatcher(columns)

  class RuntimeResultMatcher(expectedColumns: Seq[String]) extends Matcher[RuntimeResult] {

    private var rowsMatcher: RowsMatcher = AnyRowsMatcher
    private var maybeStatisticts: Option[QueryStatistics] = None

    def withStatistics(stats: QueryStatistics): RuntimeResultMatcher = {
      maybeStatisticts = Some(stats)
      this
    }

    def withSingleRow(values: Any*): RuntimeResultMatcher = withRows(singleRow(values: _*))

    def withRows(rows: Iterable[Array[_]]): RuntimeResultMatcher = withRows(inAnyOrder(rows))
    def withNoRows(): RuntimeResultMatcher = withRows(NoRowsMatcher)

    def withRows(rowsMatcher: RowsMatcher): RuntimeResultMatcher = {
      if (this.rowsMatcher != AnyRowsMatcher)
        throw new IllegalArgumentException("RowsMatcher already set")
      this.rowsMatcher = rowsMatcher
      this
    }

    override def apply(left: RuntimeResult): MatchResult = {
      val columns = left.fieldNames().toIndexedSeq
      if (columns != expectedColumns) {
        MatchResult(matches = false, s"Expected result columns $expectedColumns, got $columns", "")
      } else if (maybeStatisticts.exists(_ != left.queryStatistics())) {
        MatchResult(matches = false, s"Expected statistics ${left.queryStatistics()}, got ${maybeStatisticts.get}", "")
      } else {
        val rows = new ArrayBuffer[Array[AnyValue]]
        left.accept(new QueryResult.QueryResultVisitor[Exception] {
          override def visit(row: QueryResult.Record): Boolean = {
            val valueArray = row.fields()
            rows += util.Arrays.copyOf(valueArray, valueArray.length)
            true
          }
        })
        MatchResult(
          rowsMatcher.matches(columns, rows),
          s"""Expected:
             |
             |$rowsMatcher
             |
             |but got
             |
             |${Rows.pretty(rows)}""".stripMargin,
          ""
        )
      }
    }
  }

  def inOrder(rows: Iterable[Array[_]]): RowsMatcher = {
    val anyValues = rows.map(row => row.map(ValueUtils.of)).toIndexedSeq
    EqualInOrder(anyValues)
  }

  def inAnyOrder(rows: Iterable[Array[_]]): RowsMatcher = {
    val anyValues = rows.map(row => row.map(ValueUtils.of)).toIndexedSeq
    EqualInAnyOrder(anyValues)
  }

  def singleColumn(values: Iterable[Any]): RowsMatcher = {
    val anyValues = values.map(x => Array(ValueUtils.of(x))).toIndexedSeq
    EqualInAnyOrder(anyValues)
  }

  def singleColumnInOrder(values: Iterable[Any]): RowsMatcher = {
    val anyValues = values.map(x => Array(ValueUtils.of(x))).toIndexedSeq
    EqualInOrder(anyValues)
  }

  def singleRow(values: Any*): RowsMatcher = {
    val anyValues = Array(values.toArray.map(ValueUtils.of))
    EqualInAnyOrder(anyValues)
  }

  def rowCount(value: Int): RowsMatcher = {
    RowCount(value)
  }

  def matching(func: PartialFunction[Any, _]): RowsMatcher = {
    CustomRowsMatcher(matchPattern(func))
  }

  def groupedBy(columns: String*): RowOrderMatcher = new GroupBy(None, None, columns: _*)

  def groupedBy(nGroups: Int, groupSize: Int, columns: String*): RowOrderMatcher = new GroupBy(Some(nGroups), Some(groupSize), columns: _*)

  def sortedAsc(column: String): RowOrderMatcher = new Ascending(column)

  def sortedDesc(column: String): RowOrderMatcher = new Descending(column)

  case class DiffItem(missingRow: ListValue, fromA: Boolean)

}

case class ContextCondition[CONTEXT <: RuntimeContext](test: CONTEXT => Boolean, errorMsg: String)
