/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.executionplan

import org.neo4j.cypher.internal.compiler.v3_1.pipes.{EagerPipe, LoadCSVPipe, Pipe}
import org.neo4j.cypher.internal.frontend.v3_1.notification.{InternalNotification, EagerLoadCsvNotification}

object checkForEagerLoadCsv extends (Pipe => Option[InternalNotification]) {

  def apply(pipe: Pipe) = {
    import org.neo4j.cypher.internal.frontend.v3_1.Foldable._
    sealed trait SearchState
    case object NoEagerFound extends SearchState
    case object EagerFound extends SearchState
    case object EagerWithLoadCsvFound extends SearchState

    // Walk over the pipe tree and check if an Eager is to be executed after a LoadCsv
    val resultState = pipe.treeFold[SearchState](NoEagerFound) {
      case _: LoadCSVPipe => {
        case EagerFound => (EagerWithLoadCsvFound, None)
        case e => (e, None)
      }
      case _: EagerPipe =>
        acc =>
          (EagerFound, Some(identity))
    }

    resultState match {
      case EagerWithLoadCsvFound => Some(EagerLoadCsvNotification)
      case _ => None
    }
  }
}
