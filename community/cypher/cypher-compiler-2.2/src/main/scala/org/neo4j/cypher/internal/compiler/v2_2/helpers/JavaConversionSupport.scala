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
package org.neo4j.cypher.internal.compiler.v2_2.helpers

import org.neo4j.collection.primitive.{PrimitiveIntIterator, PrimitiveLongIterator}

/**
 * Created by stepup on 20.05.15.
 */
object JavaConversionSupport
{
  def asScala(iterator: PrimitiveLongIterator): Iterator[Long] = new Iterator[Long] {
    def hasNext = iterator.hasNext
    def next() = iterator.next()
  }

  def mapToScala[T](iterator: PrimitiveLongIterator)(f: Long => T): Iterator[T] = new Iterator[T] {
    def hasNext = iterator.hasNext
    def next() = f(iterator.next())
  }

  def asScala(iterator: PrimitiveIntIterator): Iterator[Int] = new Iterator[Int] {
    def hasNext = iterator.hasNext
    def next() = iterator.next()
  }

  def mapToScala[T](iterator: PrimitiveIntIterator)(f: Int => T): Iterator[T] = new Iterator[T] {
    def hasNext = iterator.hasNext
    def next() = f(iterator.next())
  }
}
