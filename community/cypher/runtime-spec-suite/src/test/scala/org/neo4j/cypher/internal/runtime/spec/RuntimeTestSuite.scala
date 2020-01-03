/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.concurrent.TimeUnit

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.logical.builder.Resolver
import org.neo4j.cypher.internal.logical.plans.{ProcedureSignature, QualifiedName, UserFunctionSignature}
import org.neo4j.cypher.internal.runtime.debug.DebugLog
import org.neo4j.cypher.internal.runtime.{InputDataStream, InputDataStreamTestSupport, NoInput, QueryStatistics}
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.util.{Rewriter, topDown}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.{CypherRuntime, ExecutionPlan, LogicalQuery, RuntimeContext}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb._
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.impl.query.{QuerySubscriber, RecordingQuerySubscriber}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.logging.{AssertableLogProvider, LogProvider}
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.{AnyValue, AnyValues}
import org.scalactic.source.Position
import org.scalactic.{Equality, TolerantNumerics}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{BeforeAndAfterEach, Tag}

import scala.collection.JavaConverters._
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
                                                           val runtime: CypherRuntime[CONTEXT],
                                                           workloadMode: Boolean = false)
  extends CypherFunSuite
  with AstConstructionTestSupport
  with InputDataStreamTestSupport
  with BeforeAndAfterEach
  with Resolver {

  var managementService: DatabaseManagementService = _
  var graphDb: GraphDatabaseService = _
  var runtimeTestSupport: RuntimeTestSupport[CONTEXT] = _
  val ANY_VALUE_ORDERING: Ordering[AnyValue] = Ordering.comparatorToOrdering(AnyValues.COMPARATOR)
  val logProvider: AssertableLogProvider = new AssertableLogProvider()

  override protected def beforeEach(): Unit = {
    DebugLog.beginTime()
    managementService = edition.newGraphManagementService()
    graphDb = managementService.database(DEFAULT_DATABASE_NAME)
    logProvider.clear()
    runtimeTestSupport = createRuntimeTestSupport(graphDb, edition, workloadMode, logProvider)
    runtimeTestSupport.start()
    runtimeTestSupport.startTx()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    runtimeTestSupport.stopTx()
    DebugLog.log("")
    shutdownDatabase()
    afterTest()
  }

  protected def createRuntimeTestSupport(graphDb: GraphDatabaseService,
                                         edition: Edition[CONTEXT],
                                         workloadMode: Boolean,
                                         logProvider: LogProvider): RuntimeTestSupport[CONTEXT] = {
    new RuntimeTestSupport[CONTEXT](graphDb, edition, workloadMode, logProvider)
  }

  protected def shutdownDatabase(): Unit = {
    if (managementService != null) {
      runtimeTestSupport.stop()
      managementService.shutdown()
      managementService = null
    }
  }

  def afterTest(): Unit = {}

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    super.test(testName, Tag(runtime.name) +: testTags: _*)(testFun)
  }

  /**
   * This method should be invoked with the complete graph setup given as a block.
   * It creates a new transaction and converts the result to entities that are valid in the new transaction.
   * It could be overridden to simply call `f` to test the case where the data is created in the same transaction
   *
   * There is no need to call this method if the setup does not create any graph entities, e.g. if you use input.
   *
   * @param f the graph creation
   * @return the graph, with entities that are valid in the new transaction
   */
  def given[T <: AnyRef](f: => T): T = {
    val result = f
    restartTx()
    reattachEntitiesToNewTransaction(result).asInstanceOf[T]
  }

  /**
   * This method should be invoked with the complete graph setup given as a block, if the graph is not needed later on (e.g. for assertions).
   * It creates a new transaction.
   * It could be overridden to simply call `f` to test the case where the data is created in the same transaction
   *
   * There is no need to call this method if the setup does not create any graph entities, e.g. if you use input.
   *
   * @param f the graph creation
   */
  def given(f: => Unit): Unit = {
    f
    restartTx()
  }

  private val reattachEntitiesToNewTransaction: Rewriter = topDown {
    Rewriter.lift {
      case n: Node => tx.getNodeById(n.getId)
      case r: Relationship => tx.getRelationshipById(r.getId)
    }
  }

  // HELPERS

  override def getLabelId(label: String): Int = {
    tx.kernelTransaction().tokenRead().nodeLabel(label)
  }

  override def getPropertyKeyId(prop: String): Int = {
    tx.kernelTransaction().tokenRead().propertyKey(prop)
  }

  override def procedureSignature(name: QualifiedName): ProcedureSignature = {
    val ktx = tx.kernelTransaction()
    TransactionBoundPlanContext.procedureSignature(ktx, name)
  }

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
    val ktx = tx.kernelTransaction()
    TransactionBoundPlanContext.functionSignature(ktx, name)
  }

  def select[X](things: Seq[X],
                selectivity: Double = 1.0,
                duplicateProbability: Double = 0.0,
                nullProbability: Double = 0.0): Seq[X] = {
    val rng = new Random(42)
    for {thing <- things if rng.nextDouble() < selectivity
         dup <- if (rng.nextDouble() < duplicateProbability) Seq(thing, thing) else Seq(thing)
         nullifiedDup = if (rng.nextDouble() < nullProbability) null.asInstanceOf[X] else dup
    } yield nullifiedDup
  }

  // EXECUTE
  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              input: InputValues): RecordingRuntimeResult = {
    val subscriber = new RecordingQuerySubscriber
    val result = runtimeTestSupport.run(logicalQuery, runtime, input.stream(), (_, result) => result, subscriber, profile = false)
    RecordingRuntimeResult(result, subscriber)
  }

  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              input: InputDataStream,
              subscriber: QuerySubscriber): RuntimeResult =
    runtimeTestSupport.run(logicalQuery, runtime, input, (_, result) => result, subscriber, profile = false)

  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              inputStream: InputDataStream): RecordingRuntimeResult = {
    val subscriber = new RecordingQuerySubscriber
    val result = runtimeTestSupport.run(logicalQuery, runtime, inputStream, (_, result) => result,subscriber, profile = false)
    RecordingRuntimeResult(result, subscriber)
  }

  def profile(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              input: InputValues): RecordingRuntimeResult = {
    val subscriber = new RecordingQuerySubscriber
    val result = runtimeTestSupport.run(logicalQuery, runtime, input.stream(), (_, result) => result, subscriber, profile = true)
    RecordingRuntimeResult(result, subscriber)
  }

  def execute(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT]
             ): RecordingRuntimeResult = {
    val subscriber = new RecordingQuerySubscriber
    val result = runtimeTestSupport.run(logicalQuery, runtime, NoInput, (_, result) => result, subscriber, profile = false)

    RecordingRuntimeResult(result, subscriber)
  }

  def executeAndConsumeTransactionally(logicalQuery: LogicalQuery,
                                       runtime: CypherRuntime[CONTEXT]
             ): IndexedSeq[Array[AnyValue]] = {
    val subscriber = new RecordingQuerySubscriber
    runtimeTestSupport.runTransactionally(logicalQuery, runtime, NoInput, (_, result) => {
      val recordingRuntimeResult = RecordingRuntimeResult(result, subscriber)
      val seq = recordingRuntimeResult.awaitAll()
      recordingRuntimeResult.runtimeResult.close()
      seq
    }, subscriber, profile = false)
  }

  def execute(logicalQuery: LogicalQuery, runtime: CypherRuntime[CONTEXT],  subscriber: QuerySubscriber): RuntimeResult =
    runtimeTestSupport.run(logicalQuery, runtime, NoInput, (_, result) => result, subscriber, profile = false)

  def execute(executablePlan: ExecutionPlan): RecordingRuntimeResult = {
    val subscriber = new RecordingQuerySubscriber
    val result = runtimeTestSupport.run(executablePlan, NoInput, (_, result) => result, subscriber, profile = false)
    RecordingRuntimeResult(result, subscriber)
  }

  def buildPlan(logicalQuery: LogicalQuery,
                runtime: CypherRuntime[CONTEXT]): ExecutionPlan =
    runtimeTestSupport.compile(logicalQuery, runtime)

  def profile(logicalQuery: LogicalQuery,
              runtime: CypherRuntime[CONTEXT],
              inputDataStream: InputDataStream = NoInput): RecordingRuntimeResult = {
    val subscriber = new RecordingQuerySubscriber
    val result = runtimeTestSupport.run(logicalQuery, runtime, inputDataStream, (_, result) => result, subscriber, profile = true)
    RecordingRuntimeResult(result, subscriber)
  }

  def executeAndContext(logicalQuery: LogicalQuery,
                        runtime: CypherRuntime[CONTEXT],
                        input: InputValues,
                        profile: Boolean = false
                       ): (RecordingRuntimeResult, CONTEXT) = {
    val subscriber = new RecordingQuerySubscriber
    val (result, context) = runtimeTestSupport.run(logicalQuery, runtime, input.stream(), (context, result) => (result, context), subscriber, profile)
    (RecordingRuntimeResult(result, subscriber), context)
  }

  def executeAndAssertCondition(logicalQuery: LogicalQuery,
                                input: InputValues,
                                condition: ContextCondition[CONTEXT]): Unit = {
    val nAttempts = 100
    for (_ <- 0 until nAttempts) {
      val (result, context) = executeAndContext(logicalQuery, runtime, input)
      //TODO here we should not materialize the result
      result.awaitAll()
      if (condition.test(context))
        return
    }
    fail(s"${condition.errorMsg} in $nAttempts attempts!")
  }

  // GRAPHS

  def bipartiteGraph(nNodes: Int, aLabel: String, bLabel: String, relType: String): (Seq[Node], Seq[Node]) = {
    val aNodes = nodeGraph(nNodes, aLabel)
    val bNodes = nodeGraph(nNodes, bLabel)
    val relationshipType = RelationshipType.withName(relType)
    for {a <- aNodes; b <- bNodes} {
      a.createRelationshipTo(b, relationshipType)
    }
    (aNodes, bNodes)
  }

  def bidirectionalBipartiteGraph(nNodes: Int, aLabel: String, bLabel: String, relTypeAB: String, relTypeBA: String): (Seq[Node], Seq[Node], Seq[Relationship], Seq[Relationship]) = {
    val aNodes = nodeGraph(nNodes, aLabel)
    val bNodes = nodeGraph(nNodes, bLabel)
    val relationshipTypeAB = RelationshipType.withName(relTypeAB)
    val relationshipTypeBA = RelationshipType.withName(relTypeBA)
    val (aRels, bRels) =
      (for {a <- aNodes; b <- bNodes} yield {
        val aRel = a.createRelationshipTo(b, relationshipTypeAB)
        val bRel = b.createRelationshipTo(a, relationshipTypeBA)
        (aRel, bRel)
      }).unzip
    (aNodes, bNodes, aRels, bRels)
  }

  def nodeGraph(nNodes: Int, labels: String*): Seq[Node] = {
      for (_ <- 0 until nNodes) yield {
        tx.createNode(labels.map(Label.label): _*)
      }
  }

  /**
    * Create n disjoint chain graphs, where one is a chain of nodes connected
    * by relationships of the given types. The initial node will have the label
    * :START, and the last node the label :END. Note that relationships with a type
    * starting with `FRO` will be created in reverse direction, allowing convenient
    * creation of chains with varying relationship direction.
    */
  def chainGraphs(nChains: Int, relTypeNames: String*): IndexedSeq[TestPath] = {
      val relTypes = relTypeNames.map(RelationshipType.withName)
      val startLabel = Label.label("START")
      val endLabel = Label.label("END")
      for (_ <- 0 until nChains) yield {
        val head = tx.createNode(startLabel)
        var previous: Node = head
        val relationships =
          for (relType <- relTypes) yield {
            val n =
              if (relType == relTypes.last)
                tx.createNode(endLabel)
              else
                tx.createNode()

            val r =
              if (relType.name().startsWith("FRO")) {
                n.createRelationshipTo(previous, relType)
              } else {
                previous.createRelationshipTo(n, relType)
              }
            previous = n
            r
          }
        TestPath(head, relationships)
      }
  }

  /**
    * Create a lollipop graph:
    *
    *             -[r1:R]->
    *   (n1:START)         (n2)-[r3:R]->(n3)
    *             -[r2:R]->
    */
  def lollipopGraph(): (Seq[Node], Seq[Relationship]) = {
      val n1 = tx.createNode(Label.label("START"))
      val n2 = tx.createNode()
      val n3 = tx.createNode()
      val relType = RelationshipType.withName("R")
      val r1 = n1.createRelationshipTo(n2, relType)
      val r2 = n1.createRelationshipTo(n2, relType)
      val r3 = n2.createRelationshipTo(n3, relType)
      (Seq(n1, n2, n3), Seq(r1, r2, r3))
  }

  /**
    * Create a sine graph:
    *
    *       <- sc1 <- sc2 <- sc3 <-
    *       +>    sb1 +> sb2     +>
    *       ->        sa1        ->
    * start ----------------------> middle <---------------------- end
    *                                      ->        sa1        ->
    *                                      +>    sb1 +> sb2     +>
    *                                      -> sc1 -> sc2 -> sc3 ->
    *
    * where
    *   start has label :START
    *   middle has label :MIDDLE
    *   end has label :END
    *   -> has type :A
    *   +> has type :B
    */
  def sineGraph(): SineGraph = {
      val start = tx.createNode(Label.label("START"))
      val middle = tx.createNode(Label.label("MIDDLE"))
      val end = tx.createNode(Label.label("END"))

      val A = RelationshipType.withName("A")
      val B = RelationshipType.withName("B")

      def chain(relType: RelationshipType, nodes: Node*): Unit = {
        for (i <- 0 until nodes.length-1) {
          nodes(i).createRelationshipTo(nodes(i+1), relType)
        }
      }

      val startMiddle = start.createRelationshipTo(middle, A)
      val endMiddle = end.createRelationshipTo(middle, A)

      val sa1 = tx.createNode()
      val sb1 = tx.createNode()
      val sb2 = tx.createNode()
      val sc1 = tx.createNode()
      val sc2 = tx.createNode()
      val sc3 = tx.createNode()

      chain(A, start, sa1, middle)
      chain(B, start, sb1, sb2, middle)
      chain(A, middle, sc3, sc2, sc1, start)

      val ea1 = tx.createNode()
      val eb1 = tx.createNode()
      val eb2 = tx.createNode()
      val ec1 = tx.createNode()
      val ec2 = tx.createNode()
      val ec3 = tx.createNode()

      chain(A, middle, ea1, end)
      chain(B, middle, eb1, eb2, end)
      chain(A, middle, ec1, ec2, ec3, end)

      SineGraph(start, middle, end, sa1, sb1, sb2, sc1, sc2, sc3, ea1, eb1, eb2, ec1, ec2, ec3, startMiddle, endMiddle)
  }

  def circleGraph(nNodes: Int, labels: String*): (Seq[Node], Seq[Relationship]) = {
    val nodes =
      for (_ <- 0 until nNodes) yield {
        tx.createNode(labels.map(Label.label): _*)
      }

    val rels = new ArrayBuffer[Relationship]
      val rType = RelationshipType.withName("R")
      for (i <- 0 until nNodes) {
        val a = nodes(i)
        val b = nodes((i + 1) % nNodes)
        rels += a.createRelationshipTo(b, rType)
      }
    (nodes, rels)
  }

  def starGraph(ringSize: Int, labelCenter: String, labelRing: String): (Seq[Node], Seq[Relationship]) = {
    val ring =
      for (_ <- 0 until ringSize) yield {
        tx.createNode(Label.label(labelRing))
      }
    val center = tx.createNode(Label.label(labelCenter))

    val rels = new ArrayBuffer[Relationship]
      val rType = RelationshipType.withName("R")
      for (i <- 0 until ringSize) {
        val a = ring(i)
        rels += a.createRelationshipTo(center, rType)
      }
    (ring :+ center, rels)
  }

  case class Connectivity(atLeast: Int, atMost: Int, relType: String)

  /**
    * All outgoing relationships of a node
    * @param from the start node
    * @param connections the end nodes rels, grouped by rel type
    */
  case class NodeConnections(from: Node, connections: Map[String, Seq[Node]])

  /**
    * Randomly connect nodes.
    * @param nodes all nodes to connect.
    * @param connectivities a definition of how many rels of which rel type to create for each node.
    * @return all actually created connections, grouped by start node.
    */
  def randomlyConnect(nodes: Seq[Node], connectivities: Connectivity*): Seq[NodeConnections] = {
    val random = new Random(12345)
      for (from <- nodes) yield {
        val source = tx.getNodeById(from.getId)
        val relationshipsByType =
          for {
            c <- connectivities
            numConnections = random.nextInt(c.atMost - c.atLeast) + c.atLeast
            if numConnections > 0
          } yield {
            val relType = RelationshipType.withName(c.relType)

            val endNodes =
              for (_ <- 0 until numConnections) yield {
                val to = tx.getNodeById(nodes(random.nextInt(nodes.length)).getId)
                source.createRelationshipTo(to, relType)
                to
              }
            (c.relType, endNodes)
          }

        NodeConnections(source, relationshipsByType.toMap)
      }
  }

  def nodePropertyGraph(nNodes: Int, properties: PartialFunction[Int, Map[String, Any]], labels: String*): Seq[Node] = {
    val labelArray = labels.map(Label.label)
    for (i <- 0 until nNodes) yield {
      val node = tx.createNode(labelArray: _*)
      properties.runWith(_.foreach(kv => node.setProperty(kv._1, kv._2)))(i)
      node
    }
  }

  def connect(nodes: Seq[Node], rels: Seq[(Int, Int, String)]): Seq[Relationship] = {
    rels.map {
      case (from, to, typ) =>
        nodes(from).createRelationshipTo(nodes(to), RelationshipType.withName(typ))
    }
  }

  def connectWithProperties(nodes: Seq[Node], rels: Seq[(Int, Int, String,Map[String, Any])]): Seq[Relationship] = {
    rels.map {
      case (from, to, typ, props) =>
        val r = nodes(from).createRelationshipTo(nodes(to), RelationshipType.withName(typ))
        props.foreach((r.setProperty _).tupled)
        r
    }
  }

  // TX

  def tx: InternalTransaction = runtimeTestSupport.tx

  /**
   * Call this to ensure that everything is commited and a new TX is opened. Be sure to not
   * use data from the previous tx afterwards. If you need to, get them again from the new
   * tx by id.
   */
  def restartTx(): Unit = runtimeTestSupport.restartTx()

  // INDEXES

  /**
   * Creates an index and restarts the transaction. This should be called before any data creation operation.
   */
  def index(label: String, properties: String*): Unit = {
    try {
      var creator = tx.schema().indexFor(Label.label(label))
      properties.foreach(p => creator = creator.on(p))
      creator.create()
    } finally {
      runtimeTestSupport.restartTx()
    }
    tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
  }

  /**
   * Creates a unique index and restarts the transaction. This should be called before any data creation operation.
   */
  def uniqueIndex(label: String, property: String): Unit = {
    try {
      val creator = tx.schema().constraintFor(Label.label(label)).assertPropertyIsUnique(property)
      creator.create()
    } finally {
      runtimeTestSupport.restartTx()
    }
    tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
  }

  // MATCHERS

  private val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.0001)

  def tolerantEquals(expected: Double, x: Number): Boolean =
    doubleEquality.areEqual(expected, x.doubleValue())

  protected def beColumns(columns: String*): RuntimeResultMatcher =
    new RuntimeResultMatcher(columns)

  class RuntimeResultMatcher(expectedColumns: Seq[String]) extends Matcher[RecordingRuntimeResult] {

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

    override def apply(left: RecordingRuntimeResult): MatchResult = {
      val columns = left.runtimeResult.fieldNames().toIndexedSeq
      if (columns != expectedColumns) {
        MatchResult(matches = false, s"Expected result columns $expectedColumns, got $columns", "")
      } else if (maybeStatisticts.exists(_ != left.runtimeResult.queryStatistics())) {
        MatchResult(matches = false, s"Expected statistics ${left.runtimeResult.queryStatistics()}, got ${maybeStatisticts.get}", "")
      } else {
        val rows = consume(left)
        MatchResult(
          rowsMatcher.matches(columns, rows),
          s"""Expected:
             |
             |$rowsMatcher
             |
             |but got
             |
             |${rowsMatcher.formatRows(rows)}""".stripMargin,
          ""
        )
      }
    }
  }

  def consume(left: RecordingRuntimeResult): IndexedSeq[Array[AnyValue]] = {
    val seq = left.awaitAll()
    left.runtimeResult.close()
    seq
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

case class SineGraph(start: Node,
                     middle: Node,
                     end: Node,
                     sa1: Node,
                     sb1: Node,
                     sb2: Node,
                     sc1: Node,
                     sc2: Node,
                     sc3: Node,
                     ea1: Node,
                     eb1: Node,
                     eb2: Node,
                     ec1: Node,
                     ec2: Node,
                     ec3: Node,
                     startMiddle: Relationship,
                     endMiddle: Relationship)

case class RecordingRuntimeResult(runtimeResult: RuntimeResult, recordingQuerySubscriber: RecordingQuerySubscriber) {
  def awaitAll(): IndexedSeq[Array[AnyValue]] = {
    runtimeResult.consumeAll()
    runtimeResult.close()
    recordingQuerySubscriber.getOrThrow().asScala.toIndexedSeq
  }

  def pageCacheHits: Long = runtimeResult.asInstanceOf[ClosingRuntimeResult].pageCacheHits
  def pageCacheMisses: Long = runtimeResult.asInstanceOf[ClosingRuntimeResult].pageCacheMisses

}
case class ContextCondition[CONTEXT <: RuntimeContext](test: CONTEXT => Boolean, errorMsg: String)
