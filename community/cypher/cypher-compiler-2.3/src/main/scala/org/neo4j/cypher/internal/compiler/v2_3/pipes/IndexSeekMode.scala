/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_3.commands.{QueryExpression, RangeQueryExpression}
import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.index.IndexDescriptor

case class IndexSeekModeFactory(unique: Boolean) {
  def fromQueryExpression[T](qexpr: QueryExpression[T]) = qexpr match {
    case _: RangeQueryExpression[_] if unique => UniqueIndexSeekByRange
    case _: RangeQueryExpression[_] => IndexSeekByRange
    case _ if unique => UniqueIndexSeek
    case _ => IndexSeek
  }
}

sealed trait IndexSeekMode {
  def indexFactory(descriptor: IndexDescriptor): (QueryState) => (Any) => Iterator[Node]
  def name: String
}

case object IndexSeek extends IndexSeekMode {

  override def indexFactory(descriptor: IndexDescriptor): (QueryState) => (Any) => Iterator[Node] =
    (state: QueryState) => (x: Any) => state.query.indexSeek(descriptor, x)

  override def name: String = "NodeIndexSeek"
}

case object UniqueIndexSeek extends IndexSeekMode {

  override def indexFactory(descriptor: IndexDescriptor): (QueryState) => (Any) => Iterator[Node] =
    (state: QueryState) => (x: Any) => state.query.uniqueIndexSeek(descriptor, x).toIterator

  override def name: String = "NodeUniqueIndexSeek"
}

case object IndexSeekByRange extends IndexSeekMode {

  override def indexFactory(descriptor: IndexDescriptor): (QueryState) => (Any) => Iterator[Node] =
    (state: QueryState) => (x: Any) => state.query.indexSeekByRange(descriptor, x)

  override def name: String = "NodeIndexSeekByRange"
}

case object UniqueIndexSeekByRange extends IndexSeekMode {

  override def indexFactory(descriptor: IndexDescriptor): (QueryState) => (Any) => Iterator[Node] =
    (state: QueryState) => (x: Any) => state.query.indexSeekByRange(descriptor, x)

  override def name: String = "NodeUniqueIndexSeekByRange"
}
