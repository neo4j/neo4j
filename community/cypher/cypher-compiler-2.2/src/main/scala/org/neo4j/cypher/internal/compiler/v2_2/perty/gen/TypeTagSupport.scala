/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.perty.gen

import org.neo4j.cypher.internal.compiler.v2_2.perty.SimpleExtractor
import org.neo4j.cypher.internal.compiler.v2_2.perty.gen.TypeTagSupport._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

// Helper class for working with type tags. It provides
//
// - Construct TypeTag given it's Type and defining mirror
//   (if these don't match, all hell breaks loose)
// - Recover elem (or key, val) type tags from container (or map-like) type tags
//
object TypeTagSupport {

  def fromType[T](tpe: Type, mirror: reflect.api.Mirror[reflect.runtime.universe.type]): TypeTag[T] = {
    TypeTag(mirror, new reflect.api.TypeCreator {
      def apply[U <: reflect.api.Universe with Singleton](m: reflect.api.Mirror[U]) = {
        assert(m eq mirror, s"TypeTag[$tpe] was defined in mirror $mirror. It is invalid to migrate it to mirror $m.")
        tpe.asInstanceOf[U#Type]
      }
    })
  }

  def fromContainerElem[K[X], E](tag: TypeTag[K[E]]): TypeTag[E] = tag.tpe match {
    case TypeRef(_, _, List(tpe)) => fromType(tpe, tag.mirror)
  }

  def fromMapElem[K[X, Y], A, B](tag: TypeTag[K[A, B]]): (TypeTag[A], TypeTag[B]) = tag.tpe match {
    case TypeRef(_, _, List(keyTpe, valTpe)) =>
      (fromType(keyTpe, tag.mirror), fromType(valTpe, tag.mirror))
  }
}

abstract class HasType[I : TypeTag, +O : TypeTag] extends SimpleExtractor[Any, O] {
  val expectedTpe = typeOf[I]

  def apply[X <: Any : TypeTag](x: X): Option[O] = {
    val givenTpe = typeOf[X]
    if (givenTpe <:< expectedTpe) mapTyped(x.asInstanceOf[I]) else None
  }

  def mapTyped[X <: I : TypeTag](x: X): Option[O]
}

abstract class HasProductType[O : TypeTag] extends HasType[Product, Seq[O]] {
  def mapTyped[X <: Product : TypeTag](product: X): Option[Seq[O]] = {
    val tag = implicitly[TypeTag[X]]
    val mirror = tag.mirror
    val builder = Seq.newBuilder[O]
    product.productIterator.foreach { elem =>
      val instanceMirror = mirror.reflect(elem)(ClassTag(elem.getClass))
      val clazzTpe = instanceMirror.symbol.selfType
      val tpeTag = fromType[Any](clazzTpe, mirror)
      val part = mapElem(elem)(tpeTag)
      builder += part
    }
    Some(builder.result())
  }

  def mapElem[X : TypeTag](v: X): O
}

abstract class HasContainerType[K[X], O](implicit val weakTag: WeakTypeTag[K[_]], val outTag: TypeTag[O])
  extends SimpleExtractor[Any, O] {

  def apply[X <: Any : TypeTag](x: X): Option[O] = {
    val givenTag = typeTag[X]
    if (givenTag.tpe <:< weakTag.tpe) {
      val elemTag = fromContainerElem[K, Any](givenTag.asInstanceOf[TypeTag[K[Any]]])
      mapTyped[Any](x.asInstanceOf[K[Any]])(elemTag)
    } else
      None
  }

  def mapTyped[E : TypeTag](data: K[E]): Option[O]
}

abstract class HasMapType[M[X, Y], O](implicit val weakTag: WeakTypeTag[M[_, _]], val outTag: TypeTag[O])
  extends SimpleExtractor[Any, O] {

  def apply[X <: Any : TypeTag](x: X): Option[O] = {
    val givenTag = typeTag[X]
    if (givenTag.tpe <:< weakTag.tpe) {
      val (keyTag, valTag) = fromMapElem[M, Nothing, Any](givenTag.asInstanceOf[TypeTag[M[Nothing, Any]]])
      mapTyped[Nothing, Any](x.asInstanceOf[M[Nothing, Any]])(keyTag, valTag)
    } else
      None
  }

  def mapTyped[K : TypeTag, V : TypeTag](data: M[K, V]): Option[O]
}
