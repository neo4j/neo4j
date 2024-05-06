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
import org.neo4j.collection.trackable.HeapTrackingIntObjectHashMap
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.FoundNodes
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.GlobalState
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeState
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.Propagator.NodeStateSkipList
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.LoggingPPBFSHooks.Debug
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.LoggingPPBFSHooks.Info
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.LoggingPPBFSHooks.Level
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.language.implicitConversions

/**
 * A debugging tool for following the weaving path of the PPBFS algorithm - not for production use!
 * */
class LoggingPPBFSHooks(minLevel: Level) extends PPBFSHooks {
  private val PADDING = 34

  override def addSourceSignpost(signpost: TwoWaySignpost, sourceLength: Int): Unit = {
    log(
      Debug,
      "signpost" -> signpost,
      "sourceLength" -> sourceLength
    )
  }

  override def addTargetSignpost(signpost: TwoWaySignpost, targetLength: Int): Unit = {
    log(
      Debug,
      "signpost" -> signpost,
      "targetLength" -> targetLength
    )
  }

  override def propagateLengthPair(nodeState: NodeState, sourceLength: Int, targetLength: Int): Unit = {
    log(
      Debug,
      "nodeState" -> nodeState,
      "sourceLength" -> sourceLength,
      "targetLength" -> targetLength
    )
  }

  override def propagateAllAtLengths(sourceLength: Int, targetLength: Int): Unit = {
    log(
      Debug,
      "sourceLength" -> sourceLength,
      "targetLength" -> targetLength
    )
  }

  override def validateSourceLength(nodeState: NodeState, sourceLength: Int, tracedTargetLength: Int): Unit = {
    log(
      Debug,
      "nodeState" -> nodeState,
      "sourceLength" -> sourceLength,
      "tracedTargetLength" -> tracedTargetLength
    )
  }

  override def decrementTargetCount(nodeState: NodeState, remainingTargetCount: Int): Unit = {
    log(
      Debug,
      "nodeState" -> nodeState,
      "prior remainingTargetCount" -> remainingTargetCount
    )
  }

  override def pruneSourceLength(sourceSignpost: TwoWaySignpost, sourceLength: Int): Unit = {
    log(
      Debug,
      "sourceLength" -> sourceLength,
      "sourceSignpost" -> sourceSignpost
    )
  }

  override def setVerified(sourceSignpost: TwoWaySignpost, sourceLength: Int): Unit = {
    log(
      Debug,
      "sourceLength" -> sourceLength,
      "sourceSignpost" -> sourceSignpost
    )
  }

  override def addSourceLength(signpost: TwoWaySignpost, sourceLength: Int): Unit = {
    log(
      Debug,
      "signpost" -> signpost,
      "sourceLength" -> sourceLength
    )
  }

  override def skippingDuplicateRelationship(getTracedPath: () => PathTracer.TracedPath): Unit = {
    log(Debug, "duplicate rels skipped" -> getTracedPath().toString)
  }

  override def returnPath(tracedPath: PathTracer.TracedPath): Unit = {
    log(Info, "tracedPath" -> tracedPath.toString)
  }

  override def invalidTrail(getTracedPath: () => PathTracer.TracedPath): Unit = {
    log(Info, "invalidTrail" -> getTracedPath().toString)
  }

  override def schedule(
    nodeState: NodeState,
    sourceLength: Int,
    targetLength: Int,
    source: GlobalState.ScheduleSource
  ): Unit = {
    log(
      Debug,
      "nodeState" -> nodeState,
      "sourceLength" -> sourceLength,
      "targetLength" -> targetLength,
      "scheduleSource" -> source
    )
  }

  override def propagate(
    nodesToPropagate: HeapTrackingIntObjectHashMap[HeapTrackingIntObjectHashMap[NodeStateSkipList]],
    totalLength: Int
  ): Unit = {
    color = DebugSupport.Magenta

    if (nodesToPropagate.notEmpty()) {
      val str = new StringBuilder

      nodesToPropagate.forEachKeyValue((totalLength: Int, v: HeapTrackingIntObjectHashMap[NodeStateSkipList]) => {
        v.forEachKeyValue((sourceLength: Int, s: NodeStateSkipList) => {
          str.append("\n")
            .append(" ".repeat(PADDING))
            .append("- ")
            .append(list(
              "totalLength" -> totalLength,
              "sourceLength" -> sourceLength,
              "nodes" -> s.toString
            ))
        })
      })

      log(Debug, "nodesToPropagate" -> str)
    }
  }

  override def addTarget(nodeState: NodeState): Unit = {
    log(Debug, "targetNode" -> nodeState)
  }

  private var color = DebugSupport.Blue

  override def trace(currentDepth: Int): Unit = {
    color = DebugSupport.Green
  }

  override def nextLevel(currentDepth: Int): Unit = {
    color = DebugSupport.White
    System.out.println()
    log(Info, "level" -> currentDepth)
    color = DebugSupport.Yellow
  }

  override def newRow(nodeId: Long): Unit = {
    System.out.println("\n*** New row from node " + nodeId + " ***")
  }

  override def activateSignpost(currentLength: Int, signpost: TwoWaySignpost): Unit = {
    log(
      Debug,
      "currentLength" -> currentLength,
      "signpost" -> signpost
    )
  }

  override def deactivateSignpost(currentLength: Int, signpost: TwoWaySignpost): Unit = {
    log(
      Debug,
      "currentLength" -> currentLength,
      "signpost" -> signpost
    )
  }

  override def expand(direction: TraversalDirection, foundNodes: FoundNodes): Unit = {
    color = DebugSupport.Yellow
    val frontier = foundNodes.frontier(direction)
      .iterator()
      .asScala
      .flatMap(_.iterator().asScala)
      .filter(_ != null)
      .mkString("{", ",", "}")

    log(
      Debug,
      "direction" -> direction,
      "forwardDepth" -> foundNodes.forwardDepth(),
      "backwardDepth" -> foundNodes.backwardDepth(),
      "frontier" -> frontier
    )
  }

  override def expandNode(nodeId: Long, states: HeapTrackingArrayList[State], direction: TraversalDirection): Unit = {
    val stateNames = states.iterator().asScala
      .filter(_ != null)
      .map { s =>
        s.slotOrName() match {
          case SlotOrName.VarName(name, _) => name
          case _                           => s.id()
        }
      }
      .mkString("{", ",", "}")

    log(
      Debug,
      "node" -> nodeId,
      "states" -> stateNames,
      "direction" -> direction
    )
  }

  override def discover(nodeState: NodeState, direction: TraversalDirection): Unit = {
    log(
      Debug,
      "nodeState" -> nodeState,
      "direction" -> direction
    )
  }

  implicit private def pairToString(pair: (String, Any)): String = pair._1 + ": " + pair._2
  private def list(pairs: (String, Any)*): String = pairs.map(pairToString).mkString(", ")

  private def log(level: Level, items: String*) = logMsg(level, items.mkString(", ") + "\n", 4)

  private def logMsg(level: Level, message: String, offset: Int): Unit =
    if (level.value >= minLevel.value) {
      val builder = new StringBuilder().append(color).append(DebugSupport.Bold)

      val stack = Thread.currentThread.getStackTrace()
      val outerFrame = stack(offset)
      val qualifiedName = outerFrame.getClassName.split("\\.")
      val simpleClassName = qualifiedName(qualifiedName.length - 1)
      val simpleName = simpleClassName + '.' + outerFrame.getMethodName
      builder.append(simpleName)
      val paddingSize = (PADDING - simpleName.length).max(1)
      builder.append(" ".repeat(paddingSize))

      val innerFrame = stack(offset - 1)
      if (innerFrame.getMethodName != outerFrame.getMethodName) {
        builder.append(innerFrame.getMethodName).append(' ')
      }
      builder.append(DebugSupport.Reset).append(message)
      System.out.print(builder)
    }
}

object LoggingPPBFSHooks {
  sealed abstract class Level(val value: Int)
  case object Info extends Level(2)
  case object Debug extends Level(1)

  val info = new LoggingPPBFSHooks(Info)
  val debug = new LoggingPPBFSHooks(Debug)
}
