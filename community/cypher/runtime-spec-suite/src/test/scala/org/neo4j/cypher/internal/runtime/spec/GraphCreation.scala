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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.GraphCreation.ComplexGraph
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.Label.label
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.schema.ConstraintCreator
import org.neo4j.graphdb.schema.IndexCreator
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.kernel.api.KernelTransaction

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Mixin trait for graph creation helpers, given a runtime test support instance.
 */
trait GraphCreation[CONTEXT <: RuntimeContext] {

  protected def runtimeTestSupport: RuntimeTestSupport[CONTEXT]

  /**
   * This method should be invoked with the complete graph setup given as a block.
   * It creates a new transaction and converts the result to entities that are valid in the new transaction.
   * It could be overridden to simply call `f` to test the case where the data is created in the same transaction.
   *
   * There is no need to call this method if the setup does not create any graph entities, e.g. if you use input.
   *
   * @param f the graph creation
   * @return the graph, with entities that are valid in the new transaction
   */
  def givenGraph[T <: AnyRef](f: => T): T = {
    givenWithTransactionType(f, runtimeTestSupport.defaultTransactionType)
  }

  /**
   * This method should be invoked with the complete graph setup given as a block.
   * It creates a new transaction with the supplied transactionType and converts the result to entities that are valid in the new transaction.
   *
   * @param f the graph creation
   * @param transactionType the requested type of the restarted transaction, IMPLICIT or EXPLICIT
   * @return the graph, with entities that are valid in the new transaction
   */
  def givenWithTransactionType[T <: AnyRef](f: => T, transactionType: KernelTransaction.Type): T = {
    val result = f
    runtimeTestSupport.restartTx(transactionType)
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
  def givenGraph(f: => Unit): Unit = {
    givenWithTransactionType(unitF = f, runtimeTestSupport.defaultTransactionType)
  }

  /**
   * This method performs some actions and then does a rollback on the transaction
   */
  def rollback(f: => Unit): Unit = {
    f
    runtimeTestSupport.rollbackAndRestartTx()
  }

  /**
   * This method should be invoked with the complete graph setup given as a block, if the graph is not needed later on (e.g. for assertions).
   * It creates a new transaction with the supplied transactionType.
   * It could be overridden to simply call `f` to test the case where the data is created in the same transaction
   *
   * There is no need to call this method if the setup does not create any graph entities, e.g. if you use input.
   *
   * @param unitF the graph creation
   * @param transactionType the requested type of the restarted transaction, IMPLICIT or EXPLICIT
   */
  def givenWithTransactionType(unitF: => Unit, transactionType: KernelTransaction.Type): Unit = {
    unitF
    runtimeTestSupport.restartTx(transactionType)
  }

  private val reattachEntitiesToNewTransaction: Rewriter = topDown {
    Rewriter.lift {
      case n: Node         => runtimeTestSupport.tx.getNodeById(n.getId)
      case r: Relationship => runtimeTestSupport.tx.getRelationshipById(r.getId)
    }
  }

  // GRAPHS

  def bipartiteGraph(
    nNodes: Int,
    aLabel: String,
    bLabel: String,
    relType: String,
    aProperties: PartialFunction[Int, Map[String, Any]] = PartialFunction.empty[Int, Map[String, Any]],
    bProperties: PartialFunction[Int, Map[String, Any]] = PartialFunction.empty[Int, Map[String, Any]]
  ): (Seq[Node], Seq[Node]) = {
    val aNodes = nodePropertyGraph(nNodes, aProperties, aLabel)
    val bNodes = nodePropertyGraph(nNodes, bProperties, bLabel)
    val relationshipType = RelationshipType.withName(relType)
    for { a <- aNodes; b <- bNodes } {
      a.createRelationshipTo(b, relationshipType)
    }
    (aNodes, bNodes)
  }

  def bipartiteGraphMultiLabel(
    nNodes: Int,
    aLabel: String,
    bLabel: String,
    relTypes: String*
  ): (Seq[Node], Seq[Node]) = {
    val aNodes = nodePropertyGraph(nNodes, PartialFunction.empty[Int, Map[String, Any]], aLabel)
    val bNodes = nodePropertyGraph(nNodes, PartialFunction.empty[Int, Map[String, Any]], bLabel)
    val relationshipTypes = for { relType <- relTypes } yield RelationshipType.withName(relType)
    for { a <- aNodes; b <- bNodes } {
      for (relationshipType <- relationshipTypes) {
        a.createRelationshipTo(b, relationshipType)
      }
    }
    (aNodes, bNodes)
  }

  def bidirectionalBipartiteGraph(
    nNodes: Int,
    aLabel: String,
    bLabel: String,
    relTypeAB: String,
    relTypeBA: String
  ): (Seq[Node], Seq[Node], Seq[Relationship], Seq[Relationship]) = {
    val aNodes = nodeGraph(nNodes, aLabel)
    val bNodes = nodeGraph(nNodes, bLabel)
    val relationshipTypeAB = RelationshipType.withName(relTypeAB)
    val relationshipTypeBA = RelationshipType.withName(relTypeBA)
    val (aRels, bRels) =
      (for { a <- aNodes; b <- bNodes } yield {
        val aRel = a.createRelationshipTo(b, relationshipTypeAB)
        val bRel = b.createRelationshipTo(a, relationshipTypeBA)
        (aRel, bRel)
      }).unzip
    (aNodes, bNodes, aRels, bRels)
  }

  def nodeGraph(nNodes: Int, labels: String*): Seq[Node] = {
    for (_ <- 0 until nNodes) yield {
      runtimeTestSupport.tx.createNode(labels.map(Label.label): _*)
    }
  }

  def lineGraph(nNodes: Int, relationshipType: String, labels: String*): (Seq[Node], Seq[Relationship]) = {
    val nodes = nodeGraph(nNodes, labels: _*)

    val relationships = Seq.newBuilder[Relationship]
    var prevNode = nodes.head
    for (node <- nodes.tail) {
      relationships.addOne(
        prevNode.createRelationshipTo(node, RelationshipType.withName(relationshipType))
      )
      prevNode = node
    }
    (nodes, relationships.result())
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
      val head = runtimeTestSupport.tx.createNode(startLabel)
      var previous: Node = head
      val relationships =
        for (relType <- relTypes) yield {
          val n =
            if (relType eq relTypes.last)
              runtimeTestSupport.tx.createNode(endLabel)
            else
              runtimeTestSupport.tx.createNode()

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
   * Create one directed connected graph, by cross-linking multiple chain graphs.
   * E.g., for `chainCount=3` & `chainDepth=4`, the following graph would be created:
   *
   * {{{
   *      *-->*-->*-->*
   *    /   X   X   X   \
   *  *-->*-->*-->*-->*-->*
   *    \   X   X   X   /
   *      *-->*-->*-->*
   *}}}
   *
   * @return start & end nodes
   */
  def linkedChainGraph(chainCount: Int, chainDepth: Int): (Node, Node) = {
    val relType = RelationshipType.withName("R")
    val start = runtimeTestSupport.tx.createNode()

    def extendChain(prevHeads: Seq[Node]): Seq[Node] = {
      val newHeads = (0 until chainCount).map(_ => runtimeTestSupport.tx.createNode())
      for {
        newHead <- newHeads
        prevHead <- prevHeads
      } {
        prevHead.createRelationshipTo(newHead, relType)
      }
      newHeads
    }

    @scala.annotation.tailrec
    def makeLinkedChain(prevHeads: Seq[Node], depth: Int): Seq[Node] = {
      if (depth >= chainDepth) {
        prevHeads
      } else {
        makeLinkedChain(extendChain(prevHeads), depth + 1)
      }
    }

    val chainHeads = makeLinkedChain(Seq(start), 0)
    val end = runtimeTestSupport.tx.createNode()
    for (n <- chainHeads) {
      n.createRelationshipTo(end, relType)
    }

    (start, end)
  }

  /**
   * Create one directed connected graph, by linking multiple chain graphs.
   * E.g., for `chainCount=3` & `chainDepth=4`, the following graph would be created:
   *
   * {{{
   *      *-->*-->*-->*
   *    /               \
   *  *-->*-->*-->*-->*-->*
   *    \               /
   *      *-->*-->*-->*
   *}}}
   *
   * @return start & end nodes
   */
  def linkedChainGraphNoCrossLinking(chainCount: Int, chainDepth: Int): (Node, Node) = {
    val relType = RelationshipType.withName("R")
    val start = runtimeTestSupport.tx.createNode()
    val end = runtimeTestSupport.tx.createNode()

    for (_ <- 0 until chainCount) {
      var currentNode = start
      for (_ <- 0 until chainDepth) {
        val n = runtimeTestSupport.tx.createNode()
        currentNode.createRelationshipTo(n, relType)
        currentNode = n
      }
      currentNode.createRelationshipTo(end, relType)
    }

    (start, end)
  }

  /**
   * Default node creation function for [[gridGraph]]
   */
  private def createNodeWithCoordinateLables(row: Int, col: Int): Node = {
    runtimeTestSupport.tx.createNode(
      Label.label(s"${row},${col}")
    )
  }

  /**
   * Default relationship type name creation function for [[gridGraph]]
   */
  private def rightDownRelTypeName(cords1: (Int, Int), cords2: (Int, Int)): String = {
    if (cords2._1 - cords1._1 == 1 && cords2._2 == cords1._2) {
      "DOWN"
    } else if (cords2._2 - cords1._2 == 1 && cords2._1 == cords1._1) {
      "RIGHT"
    } else {
      throw new IllegalArgumentException("rightDownRelTypeName should only be called with adjacent coordinates")
    }
  }

  /**
   * Creates a two dimensional grid graph. When the function is called with default parameters,
   * the created graph will look like:
   *
   * <p>
   *
   *{{{
   * (:'0,0')-[:RIGHT]->(:'0,1')-[:RIGHT]->(:'0,2')-[:RIGHT]->(:'0,3')-[:RIGHT]->(:'0,4')
   *    |                  |                  |                  |                  |
   * [:DOWN]            [:DOWN]            [:DOWN]            [:DOWN]            [:DOWN]
   *    |                  |                  |                  |                  |
   *    V                  V                  V                  V                  V
   * (:'1,0')-[:RIGHT]->(:'1,1')-[:RIGHT]->(:'1,2')-[:RIGHT]->(:'1,3')-[:RIGHT]->(:'1,4')
   *    |                  |                  |                  |                  |
   * [:DOWN]            [:DOWN]            [:DOWN]            [:DOWN]            [:DOWN]
   *    |                  |                  |                  |                  |
   *    V                  V                  V                  V                  V
   * (:'2,0')-[:RIGHT]->(:'2,1')-[:RIGHT]->(:'2,2')-[:RIGHT]->(:'2,3')-[:RIGHT]->(:'2,4')
   *    |                  |                  |                  |                  |
   * [:DOWN]            [:DOWN]            [:DOWN]            [:DOWN]            [:DOWN]
   *    |                  |                  |                  |                  |
   *    V                  V                  V                  V                  V
   * (:'3,0')-[:RIGHT]->(:'3,1')-[:RIGHT]->(:'3,2')-[:RIGHT]->(:'3,3')-[:RIGHT]->(:'3,4')
   *    |                  |                  |                  |                  |
   * [:DOWN]            [:DOWN]            [:DOWN]            [:DOWN]            [:DOWN]
   *    |                  |                  |                  |                  |
   *    V                  V                  V                  V                  V
   * (:'4,0')-[:RIGHT]->(:'4,1')-[:RIGHT]->(:'4,2')-[:RIGHT]->(:'4,3')-[:RIGHT]->(:'4,4')
   * }}}
   *
   * @param nRows the amount of rows of the grid graph
   * @param nCols the amount of columns of the grid graph
   * @param nodeCreationFunction a function which accepts coordinates and creates a node
   * @param relationshipTypeNameCreationFunction a function which accepts coordinates of two nodes and creates a relationship type name
   * @return A tuple of nodes and relationships in the graph. The nodes are returned row wise, starting with the uppermost row
   */
  def gridGraph(
    nRows: Int = 5,
    nCols: Int = 5,
    nodeCreationFunction: (Int, Int) => Node = createNodeWithCoordinateLables,
    relationshipTypeNameCreationFunction: ((Int, Int), (Int, Int)) => String = rightDownRelTypeName
  ): (Seq[Node], Seq[Relationship]) = {

    var nodes = Seq.empty[Node]
    var rels = Seq.empty[Relationship]

    for {
      row <- 0 until nRows
      col <- 0 until nCols
    } {
      val node = nodeCreationFunction(row, col)
      nodes = nodes :+ node
      if (col > 0) {
        val prevNodeInRow = nodes(row * nCols + col - 1)
        rels = rels :+ prevNodeInRow.createRelationshipTo(
          node,
          RelationshipType.withName(relationshipTypeNameCreationFunction((row, col - 1), (row, col)))
        )
      }
      if (row > 0) {
        val prevNodeInCol = nodes((row - 1) * nCols + col)
        rels = rels :+ prevNodeInCol.createRelationshipTo(
          node,
          RelationshipType.withName(relationshipTypeNameCreationFunction((row - 1, col), (row, col)))
        )
      }
    }
    (nodes, rels)
  }

  /**
   * Create a lollipop graph:
   *
   * {{{
   *             -[r1:R]->
   *   (n1:START)         (n2)-[r3:R]->(n3)
   *             -[r2:R]->
   *}}}
   */
  def lollipopGraph(): (Seq[Node], Seq[Relationship]) = {
    val n1 = runtimeTestSupport.tx.createNode(Label.label("START"))
    val n2 = runtimeTestSupport.tx.createNode()
    val n3 = runtimeTestSupport.tx.createNode()
    val relType = RelationshipType.withName("R")
    val r1 = n1.createRelationshipTo(n2, relType)
    val r2 = n1.createRelationshipTo(n2, relType)
    val r3 = n2.createRelationshipTo(n3, relType)
    (Seq(n1, n2, n3), Seq(r1, r2, r3))
  }

  /**
   * Create a sine graph:
   *
   *{{{
   *       <- sc1 <- sc2 <- sc3 <-
   *       +>    sb1 +> sb2     +>
   *       ->        sa1        ->
   * start ----------------------> middle <---------------------- end
   *                                      ->        ea1        ->
   *                                      +>    eb1 +> eb2     +>
   *                                      -> ec1 -> ec2 -> ec3 ->
   *
   * where
   *   start has label :START
   *   middle has label :MIDDLE
   *   end has label :END
   *   -> has type :A
   *   +> has type :B
   *}}}
   *
   */
  def sineGraph(): SineGraph = {
    val start = runtimeTestSupport.tx.createNode(Label.label("START"))
    val middle = runtimeTestSupport.tx.createNode(Label.label("MIDDLE"))
    val end = runtimeTestSupport.tx.createNode(Label.label("END"))

    val A = RelationshipType.withName("A")
    val B = RelationshipType.withName("B")

    def chain(relType: RelationshipType, nodes: Node*): Unit = {
      for (i <- 0 until nodes.length - 1) {
        nodes(i).createRelationshipTo(nodes(i + 1), relType)
      }
    }

    val startMiddle = start.createRelationshipTo(middle, A)
    val endMiddle = end.createRelationshipTo(middle, A)

    val sa1 = runtimeTestSupport.tx.createNode()
    val sb1 = runtimeTestSupport.tx.createNode()
    val sb2 = runtimeTestSupport.tx.createNode()
    val sc1 = runtimeTestSupport.tx.createNode()
    val sc2 = runtimeTestSupport.tx.createNode()
    val sc3 = runtimeTestSupport.tx.createNode()

    chain(A, start, sa1, middle)
    chain(B, start, sb1, sb2, middle)
    chain(A, middle, sc3, sc2, sc1, start)

    val ea1 = runtimeTestSupport.tx.createNode()
    val eb1 = runtimeTestSupport.tx.createNode()
    val eb2 = runtimeTestSupport.tx.createNode()
    val ec1 = runtimeTestSupport.tx.createNode()
    val ec2 = runtimeTestSupport.tx.createNode()
    val ec3 = runtimeTestSupport.tx.createNode()

    chain(A, middle, ea1, end)
    chain(B, middle, eb1, eb2, end)
    chain(A, middle, ec1, ec2, ec3, end)

    SineGraph(start, middle, end, sa1, sb1, sb2, sc1, sc2, sc3, ea1, eb1, eb2, ec1, ec2, ec3, startMiddle, endMiddle)
  }

  def circleGraph(nNodes: Int, labels: String*): (Seq[Node], Seq[Relationship]) = {
    circleGraph(nNodes, relType = "R", outDegree = 1, labels = labels.toSeq)
  }

  def circleGraph(
    nNodes: Int,
    relType: String,
    outDegree: Int,
    labels: Seq[String] = Seq.empty
  ): (Seq[Node], Seq[Relationship]) = {
    val nodes =
      for (_ <- 0 until nNodes) yield {
        runtimeTestSupport.tx.createNode(labels.map(Label.label): _*)
      }

    val rels = new ArrayBuffer[Relationship]
    val rType = RelationshipType.withName(relType)
    for (i <- 0 until nNodes) {
      val a = nodes(i)
      for (j <- 0 until outDegree) {
        val b = nodes((i + j + 1) % nNodes)
        rels += a.createRelationshipTo(b, rType)
      }
    }
    (nodes, rels.toSeq)
  }

  def starGraph(ringSize: Int, labelCenter: String, labelRing: String): (Seq[Node], Seq[Relationship]) = {
    val ring =
      for (_ <- 0 until ringSize) yield {
        runtimeTestSupport.tx.createNode(Label.label(labelRing))
      }
    val center = runtimeTestSupport.tx.createNode(Label.label(labelCenter))

    val rels = new ArrayBuffer[Relationship]
    val rType = RelationshipType.withName("R")
    for (i <- 0 until ringSize) {
      val a = ring(i)
      rels += a.createRelationshipTo(center, rType)
    }
    (ring :+ center, rels.toSeq)
  }

  /**
   * A star graph with rings of different labels
   * Will create ringSize * labelRings number of nodes.
   * Relationships are in the outgoing direction from center node.
   */
  def starGraphMultiLabel(
    ringSize: Int,
    labelCenter: String,
    labelRings: Seq[String]
  ): (Seq[Node], Seq[Relationship]) = {
    val ring =
      for {
        _ <- 0 until ringSize
        label <- labelRings
      } yield {
        runtimeTestSupport.tx.createNode(Label.label(label))
      }
    val center = runtimeTestSupport.tx.createNode(Label.label(labelCenter))

    val rels = new ArrayBuffer[Relationship]
    val rType = RelationshipType.withName("R")
    for (i <- 0 until ring.size) {
      val a = ring(i)
      rels += center.createRelationshipTo(a, rType)
    }
    (ring :+ center, rels.toSeq)
  }

  /**
   * A star graph where each node in the ring is also the center of another star graph, and so on, recursively, limited by depth.
   * The center of it all can have a special label, all other nodes will have the same label.
   */
  def nestedStarGraph(
    depth: Int,
    ringSize: Int,
    labelCenter: String,
    labelRing: String
  ): (Seq[Node], Seq[Relationship], Node) = {
    val globalCenter = runtimeTestSupport.tx.createNode(Label.label(labelCenter))

    val nodes = new ArrayBuffer[Node]
    val rels = new ArrayBuffer[Relationship]

    def recurse(depth: Int, localCenter: Node): Unit = {
      def star(center: Node): Seq[Node] = {
        val ring =
          for (_ <- 0 until ringSize) yield {
            runtimeTestSupport.tx.createNode(Label.label(labelRing))
          }
        val rType = RelationshipType.withName("R")
        for (i <- 0 until ringSize) {
          val a = ring(i)
          rels += a.createRelationshipTo(center, rType)
        }
        ring
      }

      if (depth > 0) {
        val ring = star(localCenter)
        nodes ++= ring
        ring.foreach(recurse(depth - 1, _))
      }
    }
    nodes += globalCenter
    recurse(depth, globalCenter)
    (nodes.toSeq, rels.toSeq, globalCenter)
  }

  //  (n0:START)                                                  (n6:LOOP)
  //             ↘             →                                ↗     |
  //  (n1:START) → (n3:MIDDLE) → (n4:MIDDLE) → (n5:MIDDLE:LOOP)       |
  //             ↗             ←                                ↖     ↓
  //  (n2:START)                                                  (n7:LOOP)
  def complexGraph(): ComplexGraph = {
    val tx = runtimeTestSupport.tx
    val n0 = tx.createNode(label("START"))
    val n1 = tx.createNode(label("START"))
    val n2 = tx.createNode(label("START"))
    val n3 = tx.createNode(label("MIDDLE"))
    val n4 = tx.createNode(label("MIDDLE"))
    val n5 = tx.createNode(label("MIDDLE"), label("LOOP"))
    val n6 = tx.createNode(label("LOOP"))
    val n7 = tx.createNode(label("LOOP"))
    n0.setProperty("foo", 0)
    n1.setProperty("foo", 1)
    n2.setProperty("foo", 2)
    val r03 = n0.createRelationshipTo(n3, RelationshipType.withName("R"))
    val r13 = n1.createRelationshipTo(n3, RelationshipType.withName("R"))
    val r23 = n2.createRelationshipTo(n3, RelationshipType.withName("R"))
    val r34a = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
    val r34b = n3.createRelationshipTo(n4, RelationshipType.withName("R"))
    val r43 = n4.createRelationshipTo(n3, RelationshipType.withName("R"))
    val r45 = n4.createRelationshipTo(n5, RelationshipType.withName("R"))
    val r56 = n5.createRelationshipTo(n6, RelationshipType.withName("R"))
    val r67 = n6.createRelationshipTo(n7, RelationshipType.withName("R"))
    val r75 = n7.createRelationshipTo(n5, RelationshipType.withName("R"))
    ComplexGraph(n0, n1, n2, n3, n4, n5, n6, n7, r03, r13, r23, r34a, r34b, r43, r45, r56, r67, r75)
  }

  /**
   * Same as a nestedStarGraph, but do not return the nodes and relationships, only the center node and the node count
   * This is useful if you want to measure heap usage and want to avoid retaining unnecessary memory in your test case
   */
  def nestedStarGraphCenterOnly(depth: Int, ringSize: Int, labelCenter: String, labelRing: String): (Node, Int) = {
    var nsg = nestedStarGraph(depth, ringSize, labelCenter, labelRing)
    val nNodes = nsg._1.size
    val globalCenter = nsg._3
    nsg = null
    (globalCenter, nNodes)
  }

  /**
   * Same as a nestedStarGraphCenterOnly, but with more connections.
   * The nodes in a ring are also connected to two of its neighbours within the same ring, like a circle formation.
   * Every relationship is doubled by a relationship in the other direction.
   * Every node has a self-loop relationship.
   */
  def connectedNestedStarGraph(depth: Int, ringSize: Int, labelCenter: String, labelRing: String): (Node, Int) = {
    val globalCenter = runtimeTestSupport.tx.createNode(Label.label(labelCenter))
    globalCenter.createRelationshipTo(globalCenter, RelationshipType.withName("SELF"))

    val nodes = new ArrayBuffer[Node]
    val rels = new ArrayBuffer[Relationship]

    def recurse(depth: Int, localCenter: Node): Unit = {
      def star(center: Node): Seq[Node] = {
        val ring =
          for (_ <- 0 until ringSize) yield {
            runtimeTestSupport.tx.createNode(Label.label(labelRing))
          }
        val rType = RelationshipType.withName("R")
        val rTypeRing = RelationshipType.withName("RR")
        var prevRingNode = ring(ringSize - 1)
        for (i <- 0 until ringSize) {
          val a = ring(i)
          rels += a.createRelationshipTo(center, rType)
          rels += center.createRelationshipTo(a, rType)
          rels += a.createRelationshipTo(prevRingNode, rTypeRing)
          rels += prevRingNode.createRelationshipTo(a, rTypeRing)
          rels += a.createRelationshipTo(a, RelationshipType.withName("SELF"))
          prevRingNode = a
        }
        ring
      }

      if (depth > 0) {
        val ring = star(localCenter)
        nodes ++= ring
        ring.foreach(recurse(depth - 1, _))
      }
    }
    nodes += globalCenter
    recurse(depth, globalCenter)
    val nNodes = nodes.size
    (globalCenter, nNodes)
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
      val source = runtimeTestSupport.tx.getNodeById(from.getId)
      val relationshipsByType =
        for {
          c <- connectivities
          numConnections = random.nextInt(c.atMost - c.atLeast) + c.atLeast
          if numConnections > 0
        } yield {
          val relType = RelationshipType.withName(c.relType)

          val endNodes =
            for (_ <- 0 until numConnections) yield {
              val to = runtimeTestSupport.tx.getNodeById(nodes(random.nextInt(nodes.length)).getId)
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
      val node = runtimeTestSupport.tx.createNode(labelArray.toSeq: _*)
      properties.runWith(_.foreach(kv => node.setProperty(kv._1, kv._2)))(i)
      node
    }
  }

  def nodePropertyGraphFunctional(
    nNodes: Int,
    properties: Int => Map[String, Any],
    labels: Int => Seq[String]
  ): Seq[Node] = {
    for (i <- 0 until nNodes) yield {
      val node = runtimeTestSupport.tx.createNode(labels(i).map(Label.label): _*)
      properties(i).foreach { case (key, value) => node.setProperty(key, value) }
      node
    }
  }

  /**
   * Connect the given nodes.
   *
   * @param nodes the nodes to connect
   * @param rels tuples that each describe a relationship to connect:
   *             (from index in `nodes`, to index in `nodes`, relationship type)
   * @return the created relationships
   */
  def connect(nodes: Seq[Node], rels: Seq[(Int, Int, String)]): Seq[Relationship] = {
    rels.map {
      case (from, to, typ) =>
        nodes(from).createRelationshipTo(nodes(to), RelationshipType.withName(typ))
    }
  }

  /**
   * Connect the given nodes.
   *
   * @param nodes the nodes to connect
   * @param rels tuples that each describe a relationship to connect:
   *             (from index in `nodes`, to index in `nodes`, relationship type, properties)
   * @return the created relationships
   */
  def connectWithProperties(nodes: Seq[Node], rels: Seq[(Int, Int, String, Map[String, Any])]): Seq[Relationship] = {
    rels.map {
      case (from, to, typ, props) =>
        val r = nodes(from).createRelationshipTo(nodes(to), RelationshipType.withName(typ))
        props.foreach((r.setProperty _).tupled)
        r
    }
  }

  // INDEXES

  /**
   * Creates a RANGE index and restarts the transaction. This should be called before any data creation operation.
   */
  def nodeIndex(label: String, properties: String*): Unit =
    nodeIndex(IndexType.RANGE, label, properties: _*)

  /**
   * Creates an index and restarts the transaction. This should be called before any data creation operation.
   */
  def nodeIndex(indexType: IndexType, label: String, properties: String*): Unit = {
    nodeIndex(label) { creator =>
      properties.foldLeft(creator.withIndexType(indexType)) { case (newCreator, prop) => newCreator.on(prop) }
    }
  }

  def nodeIndex(label: String)(f: IndexCreator => IndexCreator): Unit = {
    runtimeTestSupport.restartTx()
    try {
      f(runtimeTestSupport.tx.schema().indexFor(Label.label(label))).create()
    } finally {
      runtimeTestSupport.restartTx()
    }
    runtimeTestSupport.tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
  }

  /**
   * Creates a RANGE index and restarts the transaction. This should be called before any data creation operation.
   */
  def relationshipIndex(relType: String, properties: String*): Unit =
    relationshipIndex(IndexType.RANGE, relType, properties: _*)

  /**
   * Creates an index and restarts the transaction. This should be called before any data creation operation.
   */
  def relationshipIndex(indexType: IndexType, relType: String, properties: String*): Unit = {
    try {
      var creator = runtimeTestSupport.tx.schema().indexFor(RelationshipType.withName(relType)).withIndexType(indexType)
      properties.foreach(p => creator = creator.on(p))
      creator.create()
    } finally {
      runtimeTestSupport.restartTx()
    }
    runtimeTestSupport.tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
  }

  /**
   * Creates a  b-tree index and restarts the transaction. This should be called before any data creation operation.
   */
  def relationshipIndexWithProvider(indexProvider: String, relationshipType: String, properties: String*): Unit = {
    val query =
      s"CREATE BTREE INDEX FOR ()-[r:$relationshipType]-() ON (${properties.map(p => s"r.`$p`").mkString(",")}) OPTIONS {indexProvider: '$indexProvider'}"
    runtimeTestSupport.tx.execute(query)
    runtimeTestSupport.restartTx()
    runtimeTestSupport.tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
  }

  /**
   * Creates a unique node index and restarts the transaction. This should be called before any data creation operation.
   */
  def uniqueNodeIndex(label: String, properties: String*): Unit = {
    nodeConstraint(label) { creator =>
      properties.foldLeft(creator) { case (acc, p) => acc.assertPropertyIsUnique(p) }
    }
  }

  def nodeConstraint(label: String)(f: ConstraintCreator => ConstraintCreator): Unit = {
    runtimeTestSupport.restartTx()
    try {
      val creator = runtimeTestSupport.tx.schema().constraintFor(Label.label(label))
      f(creator).create()
    } finally {
      runtimeTestSupport.restartTx()
    }
    runtimeTestSupport.tx.schema().awaitIndexesOnline(2, TimeUnit.MINUTES)
  }

  def uniqueNodeIndex(indexType: IndexType, label: String, properties: String*): Unit = {
    nodeConstraint(label) { creator =>
      properties.foldLeft(creator) { case (acc, prop) => acc.assertPropertyIsUnique(prop) }
    }
  }

  /**
   * Creates a unique relationship index and restarts the transaction. This should be called before any data creation operation.
   */
  def uniqueRelationshipIndex(relationshipType: String, properties: String*): Unit = {
    try {
      val creator = properties.foldLeft(
        runtimeTestSupport.tx.schema().constraintFor(RelationshipType.withName(relationshipType)).withIndexType(
          IndexType.RANGE
        )
      ) {
        case (acc, prop) => acc.assertPropertyIsUnique(prop)
      }
      creator.create()
    } finally {
      runtimeTestSupport.restartTx()
    }
    runtimeTestSupport.tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
  }

  def uniqueRelationshipIndex(indexType: IndexType, relationshipType: String, properties: String*): Unit = {
    runtimeTestSupport.restartTx()
    try {
      val creator = runtimeTestSupport.tx.schema().constraintFor(
        RelationshipType.withName(relationshipType)
      ).withIndexType(indexType)
      properties
        .foldLeft(creator) { case (acc, prop) => acc.assertPropertyIsUnique(prop) }
        .create()
    } finally {
      runtimeTestSupport.restartTx()
    }
    runtimeTestSupport.tx.schema().awaitIndexesOnline(10, TimeUnit.MINUTES)
  }

  /**
   * Creates a node key constraint and restarts the transaction. This should be called before any data creation operation.
   */
  def nodeKey(label: String, properties: String*): Unit = {
    nodeConstraint(label) { creator =>
      properties.foldLeft(creator) { case (acc, prop) => acc.assertPropertyIsNodeKey(prop) }
    }
  }
}

object GraphCreation {

  case class ComplexGraph(
    n0: Node,
    n1: Node,
    n2: Node,
    n3: Node,
    n4: Node,
    n5: Node,
    n6: Node,
    n7: Node,
    r03: Relationship,
    r13: Relationship,
    r23: Relationship,
    r34a: Relationship,
    r34b: Relationship,
    r43: Relationship,
    r45: Relationship,
    r56: Relationship,
    r67: Relationship,
    r75: Relationship
  )
}

case class SineGraph(
  start: Node,
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
  endMiddle: Relationship
)
