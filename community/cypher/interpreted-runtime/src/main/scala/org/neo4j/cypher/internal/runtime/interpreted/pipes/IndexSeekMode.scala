/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.v3_4.logical.plans.{QueryExpression, RangeQueryExpression}
import org.neo4j.values.virtual.NodeValue

case class IndexSeekModeFactory(unique: Boolean, readOnly: Boolean) {
  def fromQueryExpression[T](qexpr: QueryExpression[T]): IndexSeekMode = qexpr match {
    case _: RangeQueryExpression[_] if unique => UniqueIndexSeekByRange
    case _: RangeQueryExpression[_] => IndexSeekByRange
    case _ if unique && !readOnly => LockingUniqueIndexSeek
    case _ if unique => UniqueIndexSeek
    case _ => IndexSeek
  }
}

object IndexSeekMode {
  type MultipleValueQuery = (QueryState) => (Seq[Any]) => Iterator[NodeValue]

  def assertSingleValue(values: Seq[Any]): Any = {
    if(values.size != 1)
      throw new InternalException("Composite lookups not yet supported")
    values.head
  }
}

sealed abstract class IndexSeekMode(val name: String)

sealed trait ExactSeek {
  self: IndexSeekMode =>
}

case object IndexSeek extends IndexSeekMode("NodeIndexSeek") with ExactSeek

case object UniqueIndexSeek extends IndexSeekMode("NodeUniqueIndexSeek") with ExactSeek

case object LockingUniqueIndexSeek extends IndexSeekMode("NodeUniqueIndexSeek(Locking)")

sealed trait SeekByRange {
  self: IndexSeekMode =>
}

case object IndexSeekByRange extends IndexSeekMode("NodeIndexSeekByRange") with SeekByRange

case object UniqueIndexSeekByRange extends IndexSeekMode("NodeUniqueIndexSeekByRange") with SeekByRange
