/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import java.nio.file.Files
import java.util.concurrent.TimeUnit

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Identifier, Literal, Property}
import org.neo4j.cypher.internal.compiler.v2_2.commands.values.{UnresolvedLabel, UnresolvedProperty}
import org.neo4j.cypher.internal.compiler.v2_2.commands.{Equals, HasLabel, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_2.{LabelId, PropertyKeyId, ast}
import org.neo4j.cypher.internal.spi.v2_2.{TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.graphdb._
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge

import scala.collection.mutable
import scala.util.Random

class ActualCostCalculationTest extends CypherFunSuite {

  implicit val monitor = new PipeMonitor {
    def stopStep(queryId: AnyRef, pipe: Pipe) {}
    def stopSetup(queryId: AnyRef, pipe: Pipe) {}
    def startSetup(queryId: AnyRef, pipe: Pipe) {}
    def startStep(queryId: AnyRef, pipe: Pipe) {}
  }

  ignore("do the test") {
    val path = Files.createTempDirectory("apa").toFile.getAbsolutePath
    println(path)
    val graph: GraphDatabaseService = new GraphDatabaseFactory().newEmbeddedDatabase(path)

    println("creating db")
    createTheDatabase(graph)
    println("creating pipes")
    val pipes = createThePipes(graph)
    println("running tests")
    val results = new ResultTable()


    graph.withTx { tx =>
      val resources: ExternalResource = mock[ExternalResource]
      val queryContext = new TransactionBoundQueryContext(graph.asInstanceOf[GraphDatabaseAPI], tx, true, graph.statement)
      val state = QueryState(graph, queryContext, resources, params = Map.empty, decorator = NullPipeDecorator)

      for (x <- 0 to 30) {
        for ((name, pipe) <- pipes) {

          val start = System.currentTimeMillis()
          pipe.createResults(state).size
          val elapsed = System.currentTimeMillis() - start

          if (x > 0) results.add(name, elapsed)
        }
        println(x)
      }
    }

    println(results.toString)
  }

  private def createTheDatabase(graph: GraphDatabaseService) {
    val labelX = DynamicLabel.label("X")
    val labelY = DynamicLabel.label("Y")
    val labelZ = DynamicLabel.label("Z")
    val relType = DynamicRelationshipType.withName("T")
    val r = new Random()

    graph.withTx { _ =>
      for (i <- 1 to 100000) {
        val node = graph.createNode(labelX)
        if (r.nextDouble() < .1) {
          node.addLabel(labelY)
          node.createRelationshipTo(graph.createNode(), relType)
        }

        if (r.nextDouble() < .01) node.addLabel(labelZ)

        node.setProperty("prop", 42)
      }
    }

    graph.withTx { _ =>
      graph.schema().indexFor(labelX).on("prop").create()
    }

    graph.withTx { _ =>
      graph.schema().awaitIndexesOnline(10, TimeUnit.SECONDS)
    }
  }

  private def labelScan(x: String) = new NodeByLabelScanPipe("x", LazyLabel(x))()

  private def hashJoin(l: Pipe, r: Pipe) = new NodeHashJoinPipe(Set("x"), l, r)()

  private def expand(l: Pipe, t: String) = new ExpandAllPipe(l, "x", "r", "n", Direction.OUTGOING, LazyTypes(Seq(t)))()

  private def allNodes() = new AllNodesScanPipe("x")()

  private def createThePipes(graph: GraphDatabaseService): Seq[(String, Pipe)] =
    graph.withTx { _ =>
      val ctx = new TransactionBoundPlanContext(graph.statement, graph)
      val labelXId = ctx.getOptLabelId("X").get
      val propKeyId = ctx.getOptPropertyKeyId("prop").get
      val labelToken = ast.LabelToken("X", LabelId(labelXId))
      val propertyKeyToken = ast.PropertyKeyToken("prop", PropertyKeyId(propKeyId))
      val literal = Literal(42)

      Seq(
        "index seek" -> new NodeIndexSeekPipe("x", labelToken, propertyKeyToken, SingleQueryExpression(literal), false)(),
        "label scan X" -> labelScan("X"),
        "label scan Y" -> labelScan("Y"),
        "label scan Z" -> labelScan("Z"),
        "all nodes" -> allNodes(),
        "X JOIN Y" -> hashJoin(labelScan("X"), labelScan("Y")),
        "X JOIN Z" -> hashJoin(labelScan("X"), labelScan("Z")),
        "Y JOIN X" -> hashJoin(labelScan("Y"), labelScan("X")),
        "Y JOIN Z" -> hashJoin(labelScan("Y"), labelScan("Z")),
        "Z JOIN X" -> hashJoin(labelScan("Z"), labelScan("X")),
        "Z JOIN Y" -> hashJoin(labelScan("Z"), labelScan("Y")),
        "expand" -> expand(labelScan("Y"), "T"),
        "Selection by reading property" -> new FilterPipe(allNodes(), Equals(Property(Identifier("x"), UnresolvedProperty("prop")), Literal("42")))(),
        "Selection by checking label" -> new FilterPipe(allNodes(), HasLabel(Identifier("x"), UnresolvedLabel("X")))(),
        "Selection by comparing two literals" -> new FilterPipe(allNodes(), Equals(Literal("42"), Literal("42")))()
      )
    }

  implicit class RichGraph(graph: GraphDatabaseService) {
    def statement = graph.asInstanceOf[GraphDatabaseAPI].getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge]).instance()

    def withTx[T](f: Transaction => T): T = {
      val tx = graph.beginTx()
      try {
        val result = f(tx)
        tx.success()
        result
      } finally {
        tx.close()
      }
    }
  }
}

class ResultTable() {
  var _results = new mutable.HashMap[String, Seq[Long]]

  def add(name: String, elapsed: Long): Unit = {
    val current = _results.getOrElseUpdate(name, Seq.empty)
    _results += name -> (current :+ elapsed)
  }

  override def toString: String = {
    _results.map {
      case (name, results) => s"$name|${results.mkString("|")}"
    }.mkString("\n")
  }
}
