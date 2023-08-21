/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.logical.plans.CompositeQueryExpression
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.exceptions.InternalException
import org.neo4j.values.virtual.VirtualNodeValue

case class IndexSeekModeFactory(unique: Boolean, readOnly: Boolean) {

  def fromQueryExpression[T](qexpr: QueryExpression[T]): IndexSeekMode = qexpr match {
    case _: RangeQueryExpression[_]                                             => NonLockingSeek
    case qe: CompositeQueryExpression[_] if unique && !readOnly && qe.exactOnly => LockingUniqueIndexSeek
    case _: CompositeQueryExpression[_] if unique                               => NonLockingSeek
    case _ if unique && !readOnly                                               => LockingUniqueIndexSeek
    case _ if unique                                                            => NonLockingSeek
    case _                                                                      => NonLockingSeek
  }
}

object IndexSeekMode {
  type MultipleValueQuery = QueryState => Seq[Any] => Iterator[VirtualNodeValue]

  def assertSingleValue(values: Seq[Any]): Any = {
    if (values.size != 1)
      throw new InternalException("Composite lookups not yet supported")
    values.head
  }
}

sealed trait IndexSeekMode

case object NonLockingSeek extends IndexSeekMode

case object LockingUniqueIndexSeek extends IndexSeekMode
