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
package org.neo4j.cypher.internal.helpers

import collection.Seq
import collection.Map
import java.lang.{Iterable => JavaIterable}
import java.util.{Map => JavaMap}
import collection.JavaConverters._

object IsCollection extends CollectionSupport {
  def unapply(x: Any):Option[Iterable[Any]] = {
    val collection = isCollection(x)
    if (collection) {
      Some(castToIterable(x))
    } else {
      None
    }
  }
}

trait CollectionSupport {
  def isCollection(x: Any) = castToIterable.isDefinedAt(x)

  def asCollectionOf[T](test: PartialFunction[Any, T])(input: Iterable[Any]): Option[Iterable[T]] =
    Some(input.map { (elem: Any) => if (test.isDefinedAt(elem)) test(elem) else return None })

  def makeTraversable(z: Any): Iterable[Any] = if (castToIterable.isDefinedAt(z)) {
    castToIterable(z)
  } else {
    Stream(z)
  }

  def castToIterable: PartialFunction[Any, Iterable[Any]] = {
    case x: Seq[_] => x
    case x: Array[_] => x
    case x: Map[_, _] => Stream(x)
    case x: JavaMap[_, _] => Stream(x.asScala)
    case x: Iterable[_] => x.toIterable
    case x: Traversable[_] => x.toIterable
    case x: JavaIterable[_] => x.asScala.map {
      case y: JavaMap[_, _] => y.asScala
      case y => y
    }
  }
}