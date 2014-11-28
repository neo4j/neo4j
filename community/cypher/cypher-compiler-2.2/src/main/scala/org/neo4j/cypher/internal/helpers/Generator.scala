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
package org.neo4j.cypher.internal.helpers

abstract class Generator[+T] extends Iterator[T] {
  private var preparing = true
  private var open: Boolean = true

  protected def prepareNext(): Unit
  protected def deliverNext: T

  final def hasNext = {
    if (preparing) {
      prepareNext()
      preparing = false
    }
    open
  }

  final def next() = {
    if (preparing) {
      prepareNext()
      preparing = false
    }

    if (open) {
      preparing = true
      deliverNext
    }
    else {
      Iterator.empty.next()
    }
  }

  protected final def isOpen: Boolean = open

  protected final def close(): Unit = {
    open = false
  }
}
