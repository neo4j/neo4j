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
import org.neo4j.collection.trackable.HeapTrackingUnifiedSet
import org.neo4j.cypher.internal.runtime.debug.DebugSupport
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeData
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.LoggingPPBFSHooks.Debug
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.LoggingPPBFSHooks.Info
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.LoggingPPBFSHooks.Level

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.language.implicitConversions

/**
 * A debugging tool for following the weaving path of the PPBFS algorithm - not for production use!
 * */
class LoggingPPBFSHooks(minLevel: Level) extends PPBFSHooks {
  private val PADDING = 34

  override def addSourceSignpost(signpost: TwoWaySignpost, lengthFromSource: Int): Unit = {
    log(
      Debug,
      "signpost" -> signpost,
      "lengthFromSource" -> lengthFromSource
    )
  }

  override def addTargetSignpost(signpost: TwoWaySignpost, lengthToTarget: Int): Unit = {
    log(
      Debug,
      "signpost" -> signpost,
      "lengthToTarget" -> lengthToTarget
    )
  }

  override def propagateLengthPair(nodeData: NodeData, lengthFromSource: Int, lengthToTarget: Int): Unit = {
    log(
      Debug,
      "nodeData" -> nodeData,
      "lengthFromSource" -> lengthFromSource,
      "lengthToTarget" -> lengthToTarget
    )
  }

  override def propagateAllAtLengths(lengthFromSource: Int, lengthToTarget: Int): Unit = {
    log(
      Debug,
      "lengthFromSource" -> lengthFromSource,
      "lengthToTarget" -> lengthToTarget
    )
  }

  override def validateLengthState(nodeData: NodeData, lengthFromSource: Int, tracedLengthToTarget: Int): Unit = {
    log(
      Debug,
      "nodeData" -> nodeData,
      "lengthFromSource" -> lengthFromSource,
      "tracedLengthToTarget" -> tracedLengthToTarget
    )
  }

  override def decrementTargetCount(nodeData: NodeData, remainingTargetCount: Int): Unit = {
    log(
      Debug,
      "node" -> nodeData,
      "prior remainingTargetCount" -> remainingTargetCount
    )
  }

  override def pruneSourceLength(sourceSignpost: TwoWaySignpost, lengthFromSource: Int): Unit = {
    log(
      Debug,
      "lengthFromSource" -> lengthFromSource,
      "sourceSignpost" -> sourceSignpost
    )
  }

  override def setVerified(sourceSignpost: TwoWaySignpost, lengthFromSource: Int): Unit = {
    log(
      Debug,
      "lengthFromSource" -> lengthFromSource,
      "sourceSignpost" -> sourceSignpost
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

  override def schedulePropagation(nodeData: NodeData, lengthFromSource: Int, lengthToTarget: Int): Unit = {
    log(
      Debug,
      "nodeData" -> nodeData,
      "lengthFromSource" -> lengthFromSource,
      "lengthToTarget" -> lengthToTarget
    )
  }

  override def propagateAll(
    nodesToPropagate: HeapTrackingIntObjectHashMap[HeapTrackingIntObjectHashMap[HeapTrackingUnifiedSet[NodeData]]],
    totalLength: Int
  ): Unit = {
    val str = new StringBuilder

    nodesToPropagate.forEachKeyValue(
      (totalLength: Int, v: HeapTrackingIntObjectHashMap[HeapTrackingUnifiedSet[NodeData]]) => {
        v.forEachKeyValue((lengthFromSource: Int, s: HeapTrackingUnifiedSet[NodeData]) => {
          str.append("\n")
            .append(" ".repeat(PADDING))
            .append("- ")
            .append(list(
              "totalLength" -> totalLength,
              "lengthFromSource" -> lengthFromSource,
              "nodes" -> s.asScala.mkString("[", ", ", "]")
            ))
        })
      }
    )

    if (nodesToPropagate.isEmpty) {
      str.append("(none)")
    }

    log(Debug, "nodesToPropagate" -> str)
  }

  override def addTarget(nodeData: NodeData): Unit = {
    log(Debug, "targetNode" -> nodeData)
  }

  private var color = DebugSupport.Blue

  private def toggleColor(): Unit = {
    if (color eq DebugSupport.Blue) color = DebugSupport.Yellow
    else color = DebugSupport.Blue
  }

  override def nextLevel(currentDepth: Int): Unit = {
    toggleColor()
    log(Info, "level" -> currentDepth)
  }

  override def newRow(nodeId: Long): Unit = {
    toggleColor()
    System.out.println("\n*** New row from node " + nodeId + " ***\n")
  }

  override def finishedPropagation(targets: HeapTrackingArrayList[NodeData]): Unit = {
    log(Debug, "targets" -> targets.asScala.mkString("[", ", ", "]"))
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
