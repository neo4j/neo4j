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

import org.junit.{Ignore, After, Before, Test}
import org.neo4j.graphdb._
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.cypher.internal.compiler.v2_1.runtime._
import collection.mutable
import org.neo4j.cypher.internal.spi.v2_1.StatementContext
import org.neo4j.graphdb.{Direction => GraphDirection}

class OperatorTest extends ExecutionEngineHelper {

  var tx: Transaction = _
  var ctx: StatementContext = _

  @Before def init() {
    tx = graph.beginTx()
    ctx = new StatementContext(statement, graph)
  }

  @After def after() {
    tx.close()
  }

  trait TableKey

  case class Long(idx: Int) extends TableKey

  case class Object(idx: Int) extends TableKey

  implicit class RichOperator(inner: Operator) {
    def toList(data: Registers)(implicit table: Map[String, TableKey]): List[Map[String, Any]] = {
      val resultBuilder = List.newBuilder[Map[String, Any]]

      while (inner.next()) {
        val result = new mutable.HashMap[String, Any]()
        table.foreach {
          case (name, Long(idx)) => result += name -> data.getLongRegister(idx)
          case (name, Object(idx)) => result += name -> data.getObjectRegister(idx)
        }
        resultBuilder += result.toMap
      }

      resultBuilder.result()
    }
  }

  @Test def all_nodes_on_empty_database() {
    val allNodesScan = new AllNodesScanOp(ctx, new MapRegisters(), 0)
    allNodesScan.open()
    assert(allNodesScan.next() === false, "Expected not to find any nodes")
  }

  @Test def all_nodes_on_database_with_three_nodes() {
    val data = new MapRegisters()
    val allNodesScan = new AllNodesScanOp(ctx, data, 0)

    createNode()
    createNode()
    createNode()

    implicit val table = Map("a" -> Long(0))

    assert(allNodesScan.toList(data) === List(Map("a" -> 0), Map("a" -> 1), Map("a" -> 2)))
  }
  
  @Test def labelScan() {
    val data = new MapRegisters()
    val registerIdx = 0
    val labelToken = 0

    createNode()
    val a1 = createLabeledNode("A")
    createLabeledNode("B")
    val a2 = createLabeledNode("A")

    val allNodesScan = new LabelScanOp(ctx, registerIdx, labelToken, data)

    implicit val table = Map("a" -> Long(0))
    assert(allNodesScan.toList(data) === List(Map("a" -> a1.getId), Map("a" -> a2.getId)))
  }

  @Test def expand() {
    val data = new MapRegisters()

    val source = createLabeledNode("A")
    val destination = createNode()
    val relId = relate(source, destination).getId

    val sourceId = 0
    val destinationId = 1

    val lhs = {
      val labelToken = 0
      new LabelScanOp(ctx, sourceId, labelToken, data)
    }
    val expand = new ExpandToNodeOp(ctx, sourceId, destinationId, lhs, data, Direction.OUTGOING)

    implicit val table: Map[String, TableKey] = Map("a" -> Long(0), "b" -> Long(1))
    assert(expand.toList(data) === List(Map("a" -> source.getId, "b" -> destination.getId)))
  }


  @Test def hash_join() {
    val data = new MapRegisters()

    for (i <- 0.until(10)) {
      val middle = createLabeledNode("A", "B")
      val lhs = createLabeledNode("A")
      val rhs = createLabeledNode("B")

      if (i >= 2) {
        relate(lhs, middle)
      }

      if (i < 8) {
        relate(rhs, middle)
      }
    }

    val id1 = 0
    val id2 = 1
    val joinKeyId = 2
    val labelAToken = 0
    val labelBToken = 1

    val labelScan1 = new LabelScanOp(ctx, id1, labelAToken, data)
    val lhs = new ExpandToNodeOp(ctx, id1, joinKeyId, labelScan1, data, Direction.OUTGOING)

    val labelScan2 = new LabelScanOp(ctx, id2, labelBToken, data)
    val rhs = new ExpandToNodeOp(ctx, id2, joinKeyId, labelScan2, data, Direction.OUTGOING)

    val hashJoin = new HashJoinOp(joinKeyId, Array(id1), Array.empty, lhs, rhs, data)

    implicit val table = Map("a" -> Long(0), "b" -> Long(1), "c" -> Long(2))

    assert(hashJoin.toList(data).size === 6)
  }

  @Test def hash_join2() {
    val data = new MapRegisters()
    val b = createNode()
    
    for(i <- 0 until 4) {
      val a = createLabeledNode("A")
      relate(a, b)
    }
    for(i <- 0 until 4) {
      val a = createLabeledNode("B")
      relate(a, b)
    }

    val id1 = 0
    val id2 = 1
    val joinKeyId = 2
    val labelAToken = 0
    val labelBToken = 1

    val labelScan1 = new LabelScanOp(ctx, id1, labelAToken, data)
    val lhs = new ExpandToNodeOp(ctx, id1, joinKeyId, labelScan1, data, Direction.OUTGOING)

    val labelScan2 = new LabelScanOp(ctx, id2, labelBToken, data)
    val rhs = new ExpandToNodeOp(ctx, id2, joinKeyId, labelScan2, data, Direction.OUTGOING)

    val hashJoin = new HashJoinOp(joinKeyId, Array(id1), Array.empty, lhs, rhs, data)

    implicit val table = Map("a" -> Long(0), "b" -> Long(1), "c" -> Long(2))

    assert(hashJoin.toList(data).size === 16)
  }

  @Ignore @Test def performance_of_expand() {
    val data = new MapRegisters()

    for (i <- 0.until(10000)) {
      val source = createLabeledNode("A")
      for (j <- 0.until(10)) {
        val destination = createNode()
        relate(source, destination)
      }
    }
    tx.success()
    tx.close()
    tx = graph.beginTx()

    val sourceId = 0
    val destinationId = 1
    val labelToken = 0

    var opsTime = new Counter(20)
    var cypherTime= new Counter(20)
    var coreTime = new Counter(20)
    var inlinedTime = new Counter(20)

    (0 until 100) foreach {
      x =>
        val start = System.nanoTime()

        val lhs = {
          new LabelScanOp(ctx, sourceId, labelToken, data)
        }
        val expand = new ExpandToNodeOp(ctx, sourceId, destinationId, lhs, data, Direction.OUTGOING)

        var count = 0
        expand.open()
        while (expand.next()) {
          count += 1
        }
        expand.close()
        val end = System.nanoTime()

        val duration = (end - start) / 1000000
        println(s"new operator time: ${duration} count: ${count}")
        opsTime += duration
    }

    (0 until 100) foreach {
      x =>
        val start = System.nanoTime()
        val count = execute("match (a:A)-[r]->() return r").size
        val end = System.nanoTime()

        val duration = (end - start) / 1000000
        println(s"old cypher time: ${duration} count: ${count}")
        cypherTime += duration
    }

    val label = DynamicLabel.label("A")

    (0 until 100) foreach {
      x =>
        val start = System.nanoTime()
        val allNodes: ResourceIterable[Node] = GlobalGraphOperations.at(graph).getAllNodesWithLabel(label)
        val nodes: ResourceIterator[Node] = allNodes.iterator()
        var count = 0
        while (nodes.hasNext) {
          val current: Node = nodes.next()
          val relationships = current.getRelationships(GraphDirection.OUTGOING).iterator()
          while(relationships.hasNext()) {
            count += 1
            relationships.next()
          }
        }
        val end = System.nanoTime()

        val duration = (end - start) / 1000000
        println(s"core time: ${duration} count: ${count}")
        coreTime += duration
    }

    println(s"ops: ${opsTime.avg}")
    println(s"cypher: ${cypherTime.avg}")
    println(s"core: ${coreTime.avg}")
    println(s"inline: ${inlinedTime.avg}")
  }

  class Counter(skip: Int) {
    var sum: Double = 0.0
    var count = -skip

    def +=(v: Double) {
      if (count >=0) {
        sum += v
      }
      count += 1
    }

    def avg = sum/count
  }
}
