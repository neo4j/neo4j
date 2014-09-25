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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.pipes.{EagerPipe, Pipe}

case object addEagernessIfNecessary extends MappedRewriter(bottomUp) {

  val rewriter = Rewriter.lift {
    case eagerPipe: EagerPipe =>
      eagerPipe

    case toPipe: Pipe =>
      val oldChildren = toPipe.children.toList
      val innerRewriter = topDownUntilMatching(childRewriter(toPipe))
      val newChildren = oldChildren.map { child =>
        val result = innerRewriter(child)
        result.getOrElse(child)
      }
      toPipe.dup(newChildren)
  }

  def childRewriter(toPipe: Pipe) = {
    val sources = toPipe.sources
    Rewriter.lift {
      case fromPipe: Pipe if sources.contains(fromPipe) && wouldInterfere(fromPipe.effects, toPipe.effects) =>
        new EagerPipe(fromPipe)(fromPipe.monitor)
    }
  }

  def wouldInterfere(from: Effects, to: Effects): Boolean = {
    val nodesInterfere = from.contains(Effects.READS_NODES) && to.contains(Effects.WRITES_NODES)
    val relsInterfere = from.contains(Effects.READS_RELATIONSHIPS) && to.contains(Effects.WRITES_RELATIONSHIPS)
    nodesInterfere || relsInterfere
  }
}
