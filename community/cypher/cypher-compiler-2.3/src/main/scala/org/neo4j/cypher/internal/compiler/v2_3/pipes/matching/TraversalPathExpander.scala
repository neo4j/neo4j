/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes.matching

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.DynamicJavaIterable
import pipes.QueryState
import org.neo4j.graphdb.{Relationship, Path, PathExpander}
import org.neo4j.graphdb.traversal.BranchState
import java.lang.{Iterable => JIterable}

class TraversalPathExpander(params: ExecutionContext, queryState: QueryState) extends PathExpander[Option[ExpanderStep]] {
  def expand(path: Path, state: BranchState[Option[ExpanderStep]]): JIterable[Relationship] = {

    val result: Iterable[Relationship] = state.getState match {
      case None => Seq()

      case Some(step) =>
        val node = path.endNode()
        val (rels, next)  = step.expand(node, params, queryState)
        state.setState(next)
        rels
    }

    val javaResult = DynamicJavaIterable(result)
    javaResult
  }

  def reverse(): PathExpander[Option[ExpanderStep]] = this
}
