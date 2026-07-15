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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.collection.trackable.HeapTrackingSkipList
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.FoundNodes
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.GlobalState.ScheduleSource
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeState
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathWriter
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.Propagator
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.SignpostStack
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipExpansion
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State

import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

import scala.collection.mutable
import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
 * Streams a JSON Lines trace of the PPBFS algorithm to disk, one file per source row,
 * named ppbfs-row<i>-node<sourceId>.jsonl.
 *
 * The files can be consumed by the ppbfs-visualizer web app
 * (https://github.com/neo-technology/stateful-shortest-visualiser)
 *
 * Every line is flushed as it is written so that a crash mid-run leaves a usable log.
 *
 * A debugging tool - not for production use!
 */
class JsonlPPBFSHooks(dir: Path) extends PPBFSHooks with AutoCloseable {
  private var writer: BufferedWriter = _
  private var rowIndex = -1
  private var seq = -1
  private var nfaWritten = false

  override def newRow(nodeId: Long): Unit = {
    closeWriter()
    rowIndex += 1
    seq = -1
    nfaWritten = false
    Files.createDirectories(dir)
    val path = dir.resolve(s"ppbfs-row$rowIndex-node$nodeId.jsonl")
    println(s"Writing JSONL to $path")
    writer = Files.newBufferedWriter(
      path,
      StandardOpenOption.CREATE,
      StandardOpenOption.TRUNCATE_EXISTING,
      StandardOpenOption.WRITE
    )
    line(s"""{"t":"header","v":1,"sourceNode":$nodeId}""")
    event("newRow", "nodeId" -> num(nodeId))
  }

  override def nextLevel(currentDepth: Int): Unit = event("nextLevel", "depth" -> num(currentDepth))
  override def trace(currentDepth: Int): Unit = event("trace", "depth" -> num(currentDepth))
  override def finished(): Unit = event("finished")

  override def discover(node: NodeState, direction: TraversalDirection): Unit = {
    if (!nfaWritten) {
      dumpNfa(node.state())
      nfaWritten = true
    }
    event("discover", "node" -> jNode(node), "direction" -> str(direction.name))
  }

  // NodeState
  override def addSourceSignpost(signpost: TwoWaySignpost, lengthFromSource: Int): Unit =
    event("addSourceSignpost", "signpost" -> jSignpost(signpost), "lengthFromSource" -> num(lengthFromSource))

  override def addTargetSignpost(signpost: TwoWaySignpost, lengthToTarget: Int): Unit =
    event("addTargetSignpost", "signpost" -> jSignpost(signpost), "lengthToTarget" -> num(lengthToTarget))

  override def propagateLengthPair(nodeState: NodeState, lengthFromSource: Int, lengthToTarget: Int): Unit =
    event(
      "propagateLengthPair",
      "node" -> jNode(nodeState),
      "lengthFromSource" -> num(lengthFromSource),
      "lengthToTarget" -> num(lengthToTarget)
    )

  override def validateSourceLength(nodeState: NodeState, lengthFromSource: Int, tracedLengthToTarget: Int): Unit =
    event(
      "validateSourceLength",
      "node" -> jNode(nodeState),
      "lengthFromSource" -> num(lengthFromSource),
      "tracedLengthToTarget" -> num(tracedLengthToTarget)
    )

  // PathTracer
  override def returned(signposts: SignpostStack): Unit = event("returned", "path" -> jPath(signposts))
  override def found(signposts: SignpostStack): Unit = event("found", "path" -> jPath(signposts))
  override def invalid(signposts: SignpostStack): Unit = event("invalid", "path" -> jPath(signposts))

  override def skippingDuplicateRelationship(signposts: SignpostStack): Unit =
    event("skippingDuplicateRelationship", "path" -> jPath(signposts))

  override def pushSignpost(signposts: SignpostStack): Unit =
    event("pushSignpost", "signpost" -> jSignpost(signposts.headSignpost()))

  override def popSignpost(signposts: SignpostStack, popped: TwoWaySignpost): Unit =
    event("popSignpost", "signpost" -> jSignpost(popped))

  override def initializeTarget(nodeState: NodeState): Unit =
    event("initializeTarget", "node" -> jNode(nodeState))

  // Propagator
  override def propagate(
    nodesToPropagate: HeapTrackingSkipList[Propagator.QueuedPropagation],
    totalLength: Int
  ): Unit = {
    val queue = nodesToPropagate.iterator().asScala.map { qp =>
      obj(
        "node" -> jNode(qp.nodeState()),
        "lengthFromSource" -> num(qp.sourceLength()),
        "lengthToTarget" -> num(qp.totalLength() - qp.sourceLength())
      )
    }.toSeq
    event("propagate", "totalLength" -> num(totalLength), "queue" -> arr(queue))
  }

  override def propagateAllAtLengths(lengthFromSource: Int, lengthToTarget: Int): Unit =
    event(
      "propagateAllAtLengths",
      "lengthFromSource" -> num(lengthFromSource),
      "lengthToTarget" -> num(lengthToTarget)
    )

  override def schedule(
    nodeState: NodeState,
    lengthFromSource: Int,
    lengthToTarget: Int,
    source: ScheduleSource
  ): Unit =
    event(
      "schedule",
      "node" -> jNode(nodeState),
      "lengthFromSource" -> num(lengthFromSource),
      "lengthToTarget" -> num(lengthToTarget),
      "scheduleSource" -> str(source.name)
    )

  // TargetTracker
  override def decrementTargetCount(nodeState: NodeState, remainingTargetCount: Int): Unit =
    event("decrementTargetCount", "node" -> jNode(nodeState), "remainingTargetCount" -> num(remainingTargetCount))

  override def addTarget(nodeState: NodeState): Unit = event("addTarget", "node" -> jNode(nodeState))

  // Signpost
  override def pruneSourceLength(sourceSignpost: TwoWaySignpost, lengthFromSource: Int): Unit =
    event("pruneSourceLength", "signpost" -> jSignpost(sourceSignpost), "lengthFromSource" -> num(lengthFromSource))

  override def setValidated(sourceSignpost: TwoWaySignpost, lengthFromSource: Int): Unit =
    event("setValidated", "signpost" -> jSignpost(sourceSignpost), "lengthFromSource" -> num(lengthFromSource))

  override def addSourceLength(signpost: TwoWaySignpost, sourceLength: Int): Unit =
    event("addSourceLength", "signpost" -> jSignpost(signpost), "lengthFromSource" -> num(sourceLength))

  // BFSExpander
  override def expand(direction: TraversalDirection, foundNodes: FoundNodes): Unit = {
    val frontier = foundNodes
      .frontier(direction)
      .iterator()
      .asScala
      .flatMap(_.iterator().asScala)
      .filter(_ != null)
      .map(jNode)
      .toSeq
    event(
      "expand",
      "direction" -> str(direction.name),
      "forwardDepth" -> num(foundNodes.forwardDepth()),
      "backwardDepth" -> num(foundNodes.backwardDepth()),
      "frontier" -> arr(frontier)
    )
  }

  override def expandNode(nodeId: Long, states: HeapTrackingArrayList[State], direction: TraversalDirection): Unit =
    event(
      "expandNode",
      "nodeId" -> num(nodeId),
      "states" -> arr(states.iterator().asScala.filter(_ != null).map(s => num(s.id())).toSeq),
      "direction" -> str(direction.name)
    )

  override def cursorSetNode(nodeId: Long): Unit = event("cursorSetNode", "nodeId" -> num(nodeId))
  override def cursorNextRelationship(nodeId: Long): Unit = event("cursorNextRelationship", "nodeId" -> num(nodeId))

  override def close(): Unit = closeWriter()

  // NFA serialization

  private def dumpNfa(start: State): Unit = {
    val seen = mutable.LinkedHashMap[Int, State](start.id -> start)
    val queue = mutable.Queue(start)
    while (queue.nonEmpty) {
      val s = queue.dequeue()
      val targets =
        s.getNodeJuxtapositions.map(_.targetState()) ++ s.getRelationshipExpansions.map(_.targetState())
      targets.foreach { t =>
        if (!seen.contains(t.id)) {
          seen(t.id) = t
          queue.enqueue(t)
        }
      }
    }
    val states = seen.values.toSeq.sortBy(_.id)
    val stateJson = states.map { s =>
      obj(
        "id" -> num(s.id),
        "name" -> str(stateName(s)),
        "isStart" -> bool(s.isStartState),
        "isFinal" -> bool(s.isFinalState)
      )
    }
    val transitionJson = states.flatMap { s =>
      s.getNodeJuxtapositions.toSeq.map { nj =>
        obj("from" -> num(s.id), "to" -> num(nj.targetState().id), "kind" -> str("NJ"), "label" -> str(""))
      } ++
        s.getRelationshipExpansions.toSeq.map { re =>
          obj("from" -> num(s.id), "to" -> num(re.targetState().id), "kind" -> str("RE"), "label" -> str(reLabel(re)))
        }
    }
    line(s"""{"t":"nfa","states":${arr(stateJson)},"transitions":${arr(transitionJson)}}""")
  }

  private def stateName(s: State): String = {
    val n = s.slotOrName().toString
    if (n.isEmpty) s"S${s.id}" else n
  }

  private def reLabel(re: RelationshipExpansion): String = {
    val name = re.slotOrName().toString
    val types = if (re.types() == null) "" else ":" + re.types().mkString("|")
    val inner = s"[$name$types]"
    re.direction() match {
      case Direction.OUTGOING => s"-$inner->"
      case Direction.INCOMING => s"<-$inner-"
      case Direction.BOTH     => s"-$inner-"
    }
  }

  // JSON emission helpers (values are pre-rendered JSON strings)

  protected def jNode(n: NodeState): String =
    obj("id" -> num(n.id), "state" -> num(n.state.id))

  protected def jSignpost(sp: TwoWaySignpost): String = {
    val rel = sp match {
      case r: TwoWaySignpost.RelSignpost => num(r.relId)
      case _                             => "null"
    }
    obj("prev" -> jNode(sp.prevNode), "fwd" -> jNode(sp.forwardNode), "rel" -> rel)
  }

  protected def jPath(signposts: SignpostStack): String = {
    val entities = mutable.ArrayBuffer.empty[String]
    signposts.materialize(new PathWriter {
      def writeNode(slotOrName: SlotOrName, id: Long): Unit =
        entities += obj("type" -> str("node"), "id" -> num(id), "slotOrName" -> str(slotOrName.toString))
      def writeRel(slotOrName: SlotOrName, id: Long): Unit =
        entities += obj("type" -> str("rel"), "id" -> num(id), "slotOrName" -> str(slotOrName.toString))
    })
    arr(entities.toSeq)
  }

  protected def event(name: String, fields: (String, String)*): Unit = {
    require(writer != null, "JsonlPPBFSHooks received an event before newRow")
    seq += 1
    val all = Seq("t" -> str("event"), "seq" -> num(seq), "name" -> str(name)) ++ fields
    line(all.map { case (k, v) => s""""${escape(k)}":$v""" }.mkString("{", ",", "}"))
  }

  protected def obj(fields: (String, String)*): String =
    fields.map { case (k, v) => s""""${escape(k)}":$v""" }.mkString("{", ",", "}")

  protected def arr(items: Seq[String]): String = items.mkString("[", ",", "]")
  protected def num(n: Long): String = n.toString
  protected def num(n: Int): String = n.toString
  protected def bool(b: Boolean): String = b.toString
  protected def str(s: String): String = "\"" + escape(s) + "\""

  private def escape(s: String): String = s.flatMap {
    case '"'          => "\\\""
    case '\\'         => "\\\\"
    case c if c < ' ' => "\\u%04x".format(c.toInt)
    case c            => c.toString
  }

  private def line(s: String): Unit = {
    writer.write(s)
    writer.newLine()
    writer.flush()
  }

  private def closeWriter(): Unit = {
    if (writer != null) {
      writer.close()
      writer = null
    }
  }
}
