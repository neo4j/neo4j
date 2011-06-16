/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.pipes

import org.neo4j.cypher.SymbolTable

class SlicePipe(source:Pipe, skip:Option[Int], limit:Option[Int]) extends Pipe {
  val symbols: SymbolTable = source.symbols

  def foreach[U](f: (Map[String, Any]) => U) {
    val slicedResult = (skip, limit) match {
      case (None, None) => source
      case (Some(x), None) => source.drop(x)
      case (None, Some(x)) => source.take(x)
      case (Some(startAt), Some(count)) => source.slice(startAt, startAt + count)
    }

    slicedResult.foreach(f)
  }
}