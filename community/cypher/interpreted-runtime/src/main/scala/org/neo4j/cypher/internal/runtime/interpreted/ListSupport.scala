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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{ArrayValue, Values}
import org.neo4j.values.virtual.{ListValue, MapValue, VirtualValues}

import scala.collection.Seq

object IsList extends ListSupport {

  def unapply(x: AnyValue): Option[ListValue] = {
    val collection = isList(x)
    if (collection) {
      Some(makeTraversable(x))
    } else {
      None
    }
  }
}

trait ListSupport {

  class NoValidValuesExceptions extends Exception

  def isList(x: AnyValue): Boolean = castToList.isDefinedAt(x)

  def asListOf[T](test: PartialFunction[AnyValue, T])(input: Iterable[AnyValue]): Option[Iterable[T]] =
    Some(input map { (elem: AnyValue) => if (test.isDefinedAt(elem)) test(elem) else return None })

  def makeTraversable(z: AnyValue): ListValue = if (isList(z)) {
    castToList(z)
  } else {
    if (z == Values.NO_VALUE) VirtualValues.EMPTY_LIST else VirtualValues.list(z)
  }

  protected def castToList: PartialFunction[AnyValue, ListValue] = {
    case x: ArrayValue => VirtualValues.fromArray(x)
    case x: ListValue => x
    case x: MapValue => VirtualValues.list(x) // TODO: This is slightly peculiar. Excercise this in tests to clarify behavior
  }

  implicit class RichSeq[T](inner: Seq[T]) {

    def foldMap[A](acc: A)(f: (A, T) => (A, T)): (A, Seq[T]) = {
      val builder = Seq.newBuilder[T]
      var current = acc

      for (element <- inner) {
        val (newAcc, newElement) = f(current, element)
        current = newAcc
        builder += newElement
      }

      (current, builder.result())
    }

    def asNonEmptyOption = if (inner.isEmpty) None else Some(inner)
  }

}
