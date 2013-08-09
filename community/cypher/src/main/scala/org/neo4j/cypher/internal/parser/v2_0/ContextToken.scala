/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v2_0

import org.parboiled.Context
import org.parboiled.buffers.InputBuffer
import org.parboiled.support.IndexRange

case class BufferPosition(buffer: InputBuffer, offset: Int) extends InputPosition {
  private val position = buffer.getPosition(offset)
  val line = position.line
  val column = position.column
}


object ContextToken {
  def apply(ctx: Context[Any]): ContextToken = new ContextToken(ctx, ctx.getMatchRange)
  def apply(ctx: Context[Any], start: Int, end: Int) = new ContextToken(ctx, new IndexRange(start, end))
}

case class ContextToken(val ctx: Context[Any], val range: IndexRange) extends InputToken {
  lazy val startPosition = BufferPosition(ctx.getInputBuffer, range.start)
  lazy val endPosition = BufferPosition(ctx.getInputBuffer, range.end)
  override lazy val toString = ctx.getInputBuffer().extract(range)
}
