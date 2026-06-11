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

import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.NodeState
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TraversalDirection
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.TwoWaySignpost.NodeSignpost

import scala.collection.mutable

/**
 * Prints a graphviz visualization of the final state of the product graph to stdout.
 *
 * Useful for debugging *small* graph+nfa combinations!
 */
class VisualizingPPBSHooks(compress: Boolean) extends PPBFSHooks {
  val nodes = mutable.HashSet.empty[NodeState]
  val rels = mutable.HashSet.empty[TwoWaySignpost]

  override def discover(node: NodeState, direction: TraversalDirection): Unit = {
    nodes.add(node)
  }

  override def addSourceSignpost(signpost: TwoWaySignpost, lengthFromSource: Int): Unit = {
    rels.add(signpost)
  }

  override def addTargetSignpost(signpost: TwoWaySignpost, lengthToTarget: Int): Unit = {
    rels.add(signpost)
  }

  override def finished(): Unit = {
    if (compress) {
      println(graphCompressed())
    } else {
      println(graphUncompressed())
    }
  }

  private def stateName(n: NodeState) =
    n.state().slotOrName() match {
      case SlotOrName.VarName(name, _) => name
      case _                           => n.state().id.toString
    }

  private def nodeName(n: NodeState) = {
    val state = stateName(n)
    s""""(${n.id},$state)""""
  }

  private def portName(n: NodeState) = {
    val state = stateName(n)
    s"${n.id}:$state"
  }

  private val sourceEdgeColor = "#1b8b1baa"
  private val sourceColor = "green"
  private val targetColor = "purple"
  private val targetEdgeColor = "#380d85aa"

  // compress nodes into a table
  private def graphCompressed() = {

    val vertices = nodes
      .toSeq
      .groupBy(_.id)
      .map { case (id, nodes) =>
        val nodeCells = nodes.map(state => s"<TD PORT=\"${stateName(state)}\">${stateName(state)}</TD>").mkString("")

        s"""$id [label=<
           |<TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0">
           |  <TR><TD>$id</TD>$nodeCells</TR>
           |</TABLE>>];""".stripMargin
      }
      .mkString("\n")

    val edges = rels
      .filter(sp => sp.prevNode.id != sp.forwardNode.id)
      .map { rel =>
        val sb = new StringBuilder()
          .append(portName(rel.prevNode))
          .append(" -> ")
          .append(portName(rel.forwardNode))
          .append(s"[color=\"$targetEdgeColor;0.5:$sourceEdgeColor\" ")

        rel match {
          case _: NodeSignpost =>
            sb.append("style=dotted penwidth=1 ")
          case signpost: TwoWaySignpost.RelSignpost =>
            sb.append("label=\"[").append(signpost.relId).append("]\" ")
          case null => ()
        }

        if (!rel.lengths.isEmpty) {
          sb
            .append(
              s"headlabel=<<font color='$sourceColor' point-size='4'>"
            )
            .append(rel.lengths.renderSourceLengths())
            .append("</font>> ")
        }

        if (rel.minTargetDistance != -1) {
          sb
            .append(
              s"taillabel=<<font color='$targetColor' point-size='4'>"
            )
            .append(rel.minTargetDistance)
            .append("</font>> ")
        }

        sb
          .append("]")
          .append(";")
          .toString()
      }.mkString("\n")

    s"""
       |digraph {
       |// styling
       |edge[labelfontsize=7 arrowsize=0.3 penwidth=0.5 labelfontname=courier]
       |node[fontsize=8 fontname=courier shape=plaintext]
       |
       |// node states
       |$vertices
       |
       |// signposts
       |$edges
       |}""".stripMargin

  }

  private def graphUncompressed() = {
    val vertices = nodes
      .toSeq
      .sortBy(_.id())
      .map { node =>
        val name = nodeName(node)
        if (node.isTarget) name + " [peripheries=2]"
        else if (node.state().isStartState) name + " [style=\"filled\" fillcolor=\"#f2eeca\"]"
        else name
      }
      .mkString("\n")

    val edges = rels.map { rel =>
      val sb = new StringBuilder()
        .append(nodeName(rel.prevNode))
        .append(" -> ")
        .append(nodeName(rel.forwardNode))
        .append(s"[color=\"$targetEdgeColor;0.5:$sourceEdgeColor\" ")

      rel match {
        case _: NodeSignpost =>
          sb.append("style=dotted penwidth=1 ")
        case signpost: TwoWaySignpost.RelSignpost =>
          sb.append("label=\"[").append(signpost.relId).append("]\" ")
        case null => ()
      }

      if (!rel.lengths.isEmpty) {
        sb
          .append(
            s"headlabel=<<font color='$sourceColor' point-size='6'>"
          )
          .append(rel.lengths.renderSourceLengths())
          .append("</font>> ")
      }

      if (rel.minTargetDistance != -1) {
        sb
          .append(
            s"taillabel=<<font color='$targetColor' point-size='6'>"
          )
          .append(rel.minTargetDistance)
          .append("</font>> ")
      }

      sb
        .append("]")
        .append(";")
        .toString()
    }.mkString("\n")

    s"""
       |digraph {
       |// styling
       |edge[labelfontsize=7 arrowsize=0.3 penwidth=0.5 labelfontname=courier fontname=courier fontsize=7]
       |node[fontsize=8 fontname=courier]
       |
       |// node states
       |$vertices
       |
       |// signposts
       |$edges
       |}""".stripMargin
  }
}
