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
package org.neo4j.cypher.internal.helpers

import collection.Seq
import collection.Map
import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap}
import collection.JavaConverters._

// TODO: This is only here for accomodating cypher-compiler-1.9. Do not touch, do not import, purge it with fire post 2.3
object IsCollection extends CollectionSupport {
  def unapply(x: Any):Option[Iterable[Any]] = {
    val collection = isCollection(x)
    if (collection) {
      Some(makeTraversable(x))
    } else {
      None
    }
  }
}

// TODO: This is only here for accomodating cypher-compiler-1.9. Do not touch, do not import, purge it with fire post 2.3
trait CollectionSupport {

  def singleOr[T](in:Iterator[T], or: => Exception):Iterator[T] = new Iterator[T] {
    var used = false
    def hasNext: Boolean = in.hasNext

    def next(): T = {
      if(used) {
        throw or
      }

      used = true

      in.next()
    }
  }

  class NoValidValuesExceptions extends Exception

  def isCollection(x: Any) = castToIterable.isDefinedAt(x)

  def liftAsCollection[T](test: PartialFunction[Any, T])(input: Any): Option[Iterable[T]] = try {
    input match {
      case single if test.isDefinedAt(single) => Some(Seq(test(single)))

      case IsCollection(coll) =>
        val mappedCollection = coll map {
          case elem if test.isDefinedAt(elem) => test(elem)
          case _                              => throw new NoValidValuesExceptions
        }

        Some(mappedCollection)

      case _ => None
    }
  } catch {
    case _: NoValidValuesExceptions => None
  }

  def asCollectionOf[T](test: PartialFunction[Any, T])(input: Iterable[Any]): Option[Iterable[T]] =
    Some(input map { (elem: Any) => if (test.isDefinedAt(elem)) test(elem) else return None })

  def makeTraversable(z: Any): Iterable[Any] = if (castToIterable.isDefinedAt(z)) {
    castToIterable(z)
  } else {
    if (z == null) Iterable() else Iterable(z)
  }

  protected def castToIterable: PartialFunction[Any, Iterable[Any]] = {
    case x: Array[_]        => x
    case x: Map[_, _]       => Iterable(x)
    case x: JavaMap[_, _]   => Iterable(x.asScala)
    case x: Traversable[_]  => x.toIterable
    case x: JavaIterable[_] => x.asScala.map {
      case y: JavaMap[_, _] => y.asScala
      case y                => y
    }
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
