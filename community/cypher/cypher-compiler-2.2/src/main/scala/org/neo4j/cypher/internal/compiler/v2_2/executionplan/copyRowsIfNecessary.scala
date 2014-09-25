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

import org.neo4j.cypher.internal.compiler.v2_2.pipes._
import org.neo4j.cypher.internal.compiler.v2_2.{MappedRewriter, Rewriter, bottomUp, topDownUntilMatching}

case object copyRowsIfNecessary extends MappedRewriter(bottomUp) {

  val rewriter = Rewriter.lift {
    case toPipe: Pipe =>
      val oldChildren = toPipe.children.toList
      val innerRewriter = topDownUntilMatching(childRewriter(toPipe))
      val newChildren = oldChildren.map { child =>
        val result = innerRewriter(child)
        result.getOrElse(child)
      }
      toPipe.dup(newChildren)
  }


  override def apply(v: AnyRef): Option[AnyRef] = {
    val rewritten = super.apply(v)
    val result = rewritten match {
      case Some(pipe: Pipe with RonjaPipe) if QueryLifetime.needsCopy(pipe.providedRowLifetime) =>
        Some(CopyRowPipe(pipe)(pipe.estimatedCardinality)(pipe.monitor))
      case other =>
        other
    }
    result
  }

  def childRewriter(toPipe: Pipe) = {
    val sources = toPipe.sources
    Rewriter.lift {
      case fromPipe: Pipe with RonjaPipe if sources.contains(fromPipe) && toPipe.requiredRowLifetime.needsCopy(fromPipe.providedRowLifetime) =>
        CopyRowPipe(fromPipe)(fromPipe.estimatedCardinality)(fromPipe.monitor)
    }
  }
}
