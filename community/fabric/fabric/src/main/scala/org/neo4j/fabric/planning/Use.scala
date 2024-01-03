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
package org.neo4j.fabric.planning

import org.neo4j.cypher.internal.ast.GraphSelection
import org.neo4j.cypher.internal.util.InputPosition

sealed trait Use {
  def graphSelection: GraphSelection
  def position: InputPosition
}

object Use {

  final case class Default(graphSelection: GraphSelection) extends Use {
    def position: InputPosition = graphSelection.position
  }

  final case class Declared(graphSelection: GraphSelection) extends Use {
    def position: InputPosition = graphSelection.position
  }

  final case class Inherited(use: Use)(pos: InputPosition) extends Use {
    def graphSelection: GraphSelection = use.graphSelection
    def position: InputPosition = pos
  }

  @scala.annotation.tailrec
  def show(use: Use): String = use match {
    case s: Default   => show(s.graphSelection) + " (transaction default)"
    case d: Declared  => show(d.graphSelection)
    case i: Inherited => show(root(i))
  }

  def show(graphSelection: GraphSelection): String = graphSelection.graphReference.print

  @scala.annotation.tailrec
  private def root(use: Use): Use = use match {
    case i: Inherited => root(i.use)
    case u            => u
  }
}
