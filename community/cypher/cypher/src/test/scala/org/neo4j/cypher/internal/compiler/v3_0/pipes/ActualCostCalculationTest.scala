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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import java.io.File
import java.nio.file.Files
import java.util.concurrent.TimeUnit

import org.apache.commons.math3.stat.regression.{OLSMultipleLinearRegression, SimpleRegression}
import org.neo4j.cypher.internal.compiler.v3_0.commands.SingleQueryExpression
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Literal
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_0.{LabelId, PropertyKeyId, SemanticDirection, ast}
import org.neo4j.cypher.internal.spi.TransactionalContextWrapper
import org.neo4j.cypher.internal.spi.v3_0.TransactionBoundQueryContext.IndexSearchMonitor
import org.neo4j.cypher.internal.spi.v3_0.{TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.security.AccessMode
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.coreapi.{PropertyContainerLocker, InternalTransaction}
import org.neo4j.kernel.impl.query.Neo4jTransactionalContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Estimates values used by CardinalityCostModel, note that this takes at least on the order
 * of a couple of minutes to finish.
 */
class ActualCostCalculationTest extends CypherFunSuite {

  implicit val monitor = new PipeMonitor {
    def stopStep(queryId: AnyRef, pipe: Pipe) {}
    def stopSetup(queryId: AnyRef, pipe: Pipe) {}
    def startSetup(queryId: AnyRef, pipe: Pipe) {}
    def startStep(queryId: AnyRef, pipe: Pipe) {}
  }
  private val N = 100000
  private val STEPS = 100
  private val LABEL = Label.label("L")
  private val PROPERTY = "prop"
  private val RELATIONSHIP = "REL"

  ignore("do the test") {
    val path = Files.createTempDirectory("apa").toFile.getAbsolutePath
    val graph: GraphDatabaseQueryService = new GraphDatabaseCypherService(new GraphDatabaseFactory().newEmbeddedDatabase(new File(path)))
    try {
      graph.createIndex(LABEL, PROPERTY)
      val results = ResultTable.empty
      val chunk = N / STEPS
      for (count <- 1 to STEPS) {
        setUpDb(graph, chunk)


        results.addAll("AllNodeScan", runSimulation(graph, allNodes))
        results.addAll("NodeByLabelScan", runSimulation(graph, labelScan))
        results.addAll("NodeIndexSeek", runSimulation(graph, indexSeek(graph)))
        results.addAll("Expand", expandResult(graph))
      }

      results.normalizedResult.foreach {
        case (name, slope) => println(s"$name: COST = $slope * NROWS")
      }
    }
    finally {
      graph.shutdown()
    }
  }

  ignore("cost for eagerness") {
    val path = Files.createTempDirectory("apa").toFile.getAbsolutePath
    val graph: GraphDatabaseQueryService = new GraphDatabaseCypherService(new GraphDatabaseFactory().newEmbeddedDatabase(new File(path)))
    try {
      graph.createIndex(LABEL, PROPERTY)
      val results = ResultTable.empty
      val chunk = N / STEPS
      for (count <- 1 to STEPS) {
        setUpDb(graph, chunk)
        results.addAll("Eager", runSimulation(graph, eager(allNodes)))
      }

      results.foreach {
        case (_, dps) =>
          val res = dps.toList.sortBy(_.numberOfRows)
          println(res.map(_.elapsed).mkString(","))
      }
      results.result.foreach {
        case (name, slope) => println(s"$name: COST = $slope * NROWS")
      }
    }
    finally {
      graph.shutdown()
    }
  }

  ignore("hash joins") {
    val path = Files.createTempDirectory("apa").toFile.getAbsolutePath
    val graph: GraphDatabaseQueryService = new GraphDatabaseCypherService(new GraphDatabaseFactory().newEmbeddedDatabase(new File(path)))
    val labels = Seq("A", "B", "C", "D", "E", "F", "G", "H", "I", "J")
    val x = ListBuffer.empty[Array[Double]]
    val y = ListBuffer.empty[Double]

    try {
      setupDbForJoins(graph, labels)

      //permutate lhs, and rhs of the hashjoin, for each permutation
      //calculate cost of lhs, rhs and the cost for the hash join
      for {label1 <- labels
           label2 <- labels if label1 != label2} {

        val lhsPipe = labelScan(label1)
        val rhsPipe = labelScan(label2)
        val lhsCost = medianPerRowCount(runSimulation(graph, lhsPipe)).head
        val rhsCost = medianPerRowCount(runSimulation(graph, rhsPipe)).head
        val hashJoinCost = medianPerRowCount(runSimulation(graph, hashJoin(lhsPipe, rhsPipe))).head
        x.append(Array(lhsCost.elapsed, rhsCost.elapsed))
        y.append(hashJoinCost.elapsed)
      }

      //From the collected data, estimate A and B
      val regression = new OLSMultipleLinearRegression()
      regression.setNoIntercept(true)
      regression.newSampleData(y.toArray, x.toArray)
      val params = regression.estimateRegressionParameters()

      println(s"COST = LHS * ${params(0)} + RHS * ${params(1)}")

    } finally {
      graph.shutdown()
    }
  }

  class ResultTable {
    private val table = mutable.HashMap.empty[String, ListBuffer[DataPoint]]

    def foreach(f: ((String, Seq[DataPoint])) => Unit) = table.foreach(f)
    def add(name: String, dataPoint: DataPoint) =
      table.getOrElseUpdate(name, ListBuffer.empty).append(dataPoint)

    def addAll(name: String, dataPoints: Seq[DataPoint]) =
      table.getOrElseUpdate(name, ListBuffer.empty).appendAll(dataPoints)

    def normalizedResult = {
      val result = table.mapValues(calculateSimpleResult)
      val minValue = result.values.min
      result.mapValues(_/minValue)
    }

    def result = table.mapValues(calculateSimpleResult)

    override def toString: String = table.map{
      case (name, dataPoints) => s"$name: $dataPoints"
    }.mkString("\n")
  }

  object ResultTable {
    def empty = new ResultTable
    def apply() = new ResultTable
  }

  case class DataPoint(elapsed: Double, numberOfRows: Long) {
    def subtractTime(subtract: Double) = copy(elapsed = elapsed - subtract)

    override def toString: String = s"$numberOfRows, $elapsed"
  }

  private def expandResult(graph: GraphDatabaseQueryService) = {
    val scan = labelScan
    val scanCost = medianPerRowCount(runSimulation(graph, scan)).head
    val simulation = runSimulation(graph, expand(scan, RELATIONSHIP)).map(_.subtractTime(scanCost.elapsed))

    simulation
  }

  //From the provided data points, estimate slope and intercept in `cost = slope*NROWS + intercept`
  private def calculateSimpleResult(dataPoints: Seq[DataPoint]): Double= {
    if (dataPoints.isEmpty) throw new IllegalArgumentException("Cannot compute result without any data points")
    else if (dataPoints.size == 1) {
      val dp = dataPoints.head
      dp.elapsed / dp.numberOfRows.toDouble
    } else {
      val regression = new SimpleRegression(false)
      dataPoints.foreach(dp => regression.addData(dp.numberOfRows, dp.elapsed))
      regression.getSlope
    }
  }

  //For each rowcount find the median value
  private def medianPerRowCount(dataPoints: Seq[DataPoint]) =
    dataPoints.groupBy(_.numberOfRows).map {
      case (rowCount, dps) => DataPoint(median(dps.map(_.elapsed)), rowCount)
    }

  private def median(values: Seq[Double]) =
    if (values.length % 2 == 0) {
      val sorted = values.sorted
      (sorted(values.size / 2 - 1) + sorted(values.length / 2)) / 2.0
    } else {
      val sorted = values.sorted
      sorted(values.size / 2)
    }

  private def runSimulation(graph: GraphDatabaseQueryService, pipe: Pipe): Seq[DataPoint] =
    runSimulation(graph, Seq(pipe))

  //executes the provided pipes and returns execution times
  private def runSimulation(graph: GraphDatabaseQueryService, pipes: Seq[Pipe]) = {
    val results = new ListBuffer[DataPoint]
    graph.withTx { tx =>
      val transactionalContext = new TransactionalContextWrapper(new Neo4jTransactionalContext(graph, tx, graph.statement, new PropertyContainerLocker))
      val queryContext = new TransactionBoundQueryContext(transactionalContext)(mock[IndexSearchMonitor])
      val state = QueryStateHelper.emptyWith(queryContext)
      for (x <- 0 to 25) {
        for (pipe <- pipes) {
          val start = System.currentTimeMillis()
          val numberOfRows = pipe.createResults(state).size
          val elapsed = System.currentTimeMillis() - start

          //warmup
          if (x > 4) results.append(DataPoint(elapsed, numberOfRows))
        }
      }
    }
    //remove fastest and slowest
    results.sortBy(_.elapsed).slice(5, results.size - 5)
  }

  private def setUpDb(graph: GraphDatabaseQueryService, chunkSize: Int) {
    graph.withTx { _ =>
      for (i <- 1 to chunkSize) {
        val node = graph.createNode(LABEL)
        node.createRelationshipTo(graph.createNode(),
          RelationshipType.withName(RELATIONSHIP))
        node.setProperty(PROPERTY, 42)
      }
    }
  }

  //create a database where each subsequent label is more frequent
  private def setupDbForJoins(graph: GraphDatabaseQueryService, labels: Seq[String]) = {
    val nLabels = labels.size
    //divide so that each subsequent label is more frequent,
    //e.g. [100, 200, 300,...] with 100 + 200 + 300 ~ N
    val factor = 2 * N / (nLabels * (nLabels + 1))
    val sizes =  for (i <- 1 to nLabels) yield i * factor
    graph.withTx { _ =>
      for (i <- labels.indices) {
        val label = labels(i)
        val size = sizes(i)
        for (c <- 1 to size) {
          graph.createNode(Label.label(label))
        }
      }
    }
  }

  private def labelScan = new NodeByLabelScanPipe("x", LazyLabel(LABEL.name()))()

  private def labelScan(label: String) = new NodeByLabelScanPipe("x", LazyLabel(label))()

  private def hashJoin(l: Pipe, r: Pipe) = new NodeHashJoinPipe(Set("x"), l, r)()

  private def expand(l: Pipe, t: String) = new ExpandAllPipe(l, "x", "r", "n", SemanticDirection.OUTGOING, LazyTypes(Seq(t)))()

  private def allNodes = new AllNodesScanPipe("x")()

  private def eager(pipe: Pipe) = new EagerPipe(pipe)()

  private def indexSeek(graph: GraphDatabaseQueryService) = {
    graph.withTx { tx =>
      val transactionalContext = new TransactionalContextWrapper(new Neo4jTransactionalContext(graph, tx, graph.statement, new PropertyContainerLocker))
      val ctx = new TransactionBoundPlanContext(transactionalContext)
      val literal = Literal(42)

      val labelId = ctx.getOptLabelId(LABEL.name()).get
      val propKeyId = ctx.getOptPropertyKeyId(PROPERTY).get
      val labelToken = ast.LabelToken(LABEL.name(), LabelId(labelId))
      val propertyKeyToken = ast.PropertyKeyToken(PROPERTY, PropertyKeyId(propKeyId))

      new NodeIndexSeekPipe(LABEL.name(), labelToken, propertyKeyToken, SingleQueryExpression(literal), IndexSeek)()
    }
  }

  implicit class RichGraph(graph: GraphDatabaseQueryService) {
    def statement = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge]).get()
    val gds = graph.asInstanceOf[GraphDatabaseCypherService].getGraphDatabaseService

    def withTx[T](f: InternalTransaction => T): T = {
      val tx = graph.beginTransaction(KernelTransaction.Type.explicit, AccessMode.Static.FULL)
      try {
        val result = f(tx)
        tx.success()
        result
      } finally {
        tx.close()
      }
    }

    def shutdown() = gds.shutdown()

    def createNode(label: Label) = gds.createNode(label)

    def createIndex(label: Label, propertyName: String) = {
      graph.withTx { _ =>
        gds.schema().indexFor(label).on(propertyName).create()
      }

      graph.withTx { _ =>
        gds.schema().awaitIndexesOnline(10, TimeUnit.SECONDS)
      }
    }
  }
}

