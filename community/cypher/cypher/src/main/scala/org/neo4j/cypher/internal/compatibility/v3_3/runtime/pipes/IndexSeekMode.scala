/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.cypher.internal.compiler.v3_3.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v3_3.commands.{QueryExpression, RangeQueryExpression}
import org.neo4j.cypher.internal.compiler.v3_3.pipes.IndexSeekMode.{MultipleValueQuery, assertSingleValue}
import org.neo4j.cypher.internal.frontend.v3_3.InternalException
import org.neo4j.graphdb.Node

case class IndexSeekModeFactory(unique: Boolean, readOnly: Boolean) {
  def fromQueryExpression[T](qexpr: QueryExpression[T]) = qexpr match {
    case _: RangeQueryExpression[_] if unique => UniqueIndexSeekByRange
    case _: RangeQueryExpression[_] => IndexSeekByRange
    case _ if unique && !readOnly => LockingUniqueIndexSeek
    case _ if unique => UniqueIndexSeek
    case _ => IndexSeek
  }
}

object IndexSeekMode {
  type MultipleValueQuery = (QueryState) => (Seq[Any]) => Iterator[Node]

  def assertSingleValue(values: Seq[Any]): Any = {
    if(values.size != 1)
      throw new InternalException("Composite lookups not yet supported")
    values.head
  }
 }

sealed trait IndexSeekMode {
  def indexFactory(descriptor: IndexDescriptor): MultipleValueQuery

  def name: String
}

sealed trait ExactSeek {
  self: IndexSeekMode =>
  override def indexFactory(descriptor: IndexDescriptor): MultipleValueQuery =
    (state: QueryState) => (values: Seq[Any]) => state.query.indexSeek(descriptor, values)
}

case object IndexSeek extends IndexSeekMode with ExactSeek {
  override def name: String = "NodeIndexSeek"
}

case object UniqueIndexSeek extends IndexSeekMode with ExactSeek {
  override def name: String = "NodeUniqueIndexSeek"
}

case object LockingUniqueIndexSeek extends IndexSeekMode {

  override def indexFactory(descriptor: IndexDescriptor): MultipleValueQuery =
    (state: QueryState) => (x: Seq[Any]) => {
      state.query.lockingUniqueIndexSeek(descriptor, x).toIterator
    }

  override def name: String = "NodeUniqueIndexSeek(Locking)"
}

sealed trait SeekByRange {
  self: IndexSeekMode =>
  override def indexFactory(descriptor: IndexDescriptor): MultipleValueQuery =
    (state: QueryState) => (x: Seq[Any]) => state.query.indexSeekByRange(descriptor, assertSingleValue(x))
}

case object IndexSeekByRange extends IndexSeekMode with SeekByRange {
  override def name: String = "NodeIndexSeekByRange"
}

case object UniqueIndexSeekByRange extends IndexSeekMode with SeekByRange {
  override def name: String = "NodeUniqueIndexSeekByRange"
}
