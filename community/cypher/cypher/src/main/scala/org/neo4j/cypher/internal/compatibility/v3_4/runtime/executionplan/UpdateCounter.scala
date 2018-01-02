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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan

class UpdateCounter {
  private var uncommittedRows = 0L
  private var totalRows = 0L

  def +=(increment: Long) {
    assert(increment > 0L, s"increment must be positive but was: $increment")
    uncommittedRows += increment
    totalRows += increment
  }

  def resetIfPastLimit(limit: Long)(f: => Unit) {
    if (uncommittedRows >= limit) {
      f
      uncommittedRows = 0
    }
  }
}
