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
package org.neo4j.cypher.internal.frontend.v2_3.perty.helpers

import scala.reflect.runtime.universe._

// Helper class for working with type tags. It provides
//
// - Get the most specific runtime (erasure-based) type tag for a value
// - Construct TypeTag given it's Type and defining mirror
//   (if these don't match, all hell breaks loose)
// - Recover elem (or key, val) type tags from container (or map-like) type tags
//
object TypeTagSupport {

  def mostSpecificRuntimeTypeTag[T](value: T, tag: TypeTag[T]): TypeTag[T] = {
    if (value == null) {
      tag
    } else {
      val mirror = tag.mirror
      val clazzTpe = mirror.classSymbol(value.getClass).toType
      val providedTpe = tag.tpe

      if (providedTpe.erasure <:< clazzTpe) {
        tag
      } else {
        fromType[T](clazzTpe, mirror)
      }
    }
  }

  def fromType[T](tpe: Type, mirror: reflect.api.Mirror[reflect.runtime.universe.type]): TypeTag[T] = {
    TypeTag(mirror, new reflect.api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
        assert(m eq mirror, s"TypeTag[$tpe] was defined in mirror $mirror. It is invalid to migrate it to mirror $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
  }

  def fromContainerElem[K[X], E](tag: TypeTag[K[E]]): Option[TypeTag[E]] = tag.tpe match {
    case TypeRef(_, _, List(tpe)) =>
      Some(fromType(tpe, tag.mirror))
    case _  /* empty container */ =>
      None
  }

  def fromMapElem[K[X, Y], A, B](tag: TypeTag[K[A, B]]): Option[(TypeTag[A], TypeTag[B])] = tag.tpe match {
    case TypeRef(_, _, List(keyTpe, valTpe)) =>
      Some((fromType[A](keyTpe, tag.mirror), fromType[B](valTpe, tag.mirror)))
    case _ /* empty map */ =>
      None
  }
}
