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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.graphdb.GraphDatabaseService
import org.scalatest.Assertions


class LazyIterator[T](count: Int, f: (Int, GraphDatabaseService) => T) extends Iterator[T]() with Assertions {
  var db: Option[GraphDatabaseService] = None

  def this(count: Int, f: Int => T) = {
    this(count, (count: Int, _) => f(count))
    db = Some(null)
  }

  var counter = 0

  def hasNext: Boolean = counter < count

  def next(): T = {
    val graph = db.getOrElse(fail("Iterator needs that database set before it can be used"))

    counter += 1
    f(counter, graph)
  }

  override def toString(): String = counter.toString
}
