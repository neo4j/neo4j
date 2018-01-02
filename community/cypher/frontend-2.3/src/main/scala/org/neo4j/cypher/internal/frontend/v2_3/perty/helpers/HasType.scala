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

import org.neo4j.cypher.internal.frontend.v2_3.perty.SimpleExtractor
import org.neo4j.cypher.internal.frontend.v2_3.perty.helpers.TypeTagSupport._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


abstract class HasType[I : TypeTag, +O : TypeTag] extends SimpleExtractor[Any, O] {
  val expectedTpe = typeOf[I]

  def apply[X <: Any : TypeTag](x: X): Option[O] = {
    val givenTpe = TypeTagSupport.mostSpecificRuntimeTypeTag(x, typeTag[X]).tpe
    if (givenTpe <:< expectedTpe) mapTyped(x.asInstanceOf[I]) else None
  }

  def mapTyped[X <: I : TypeTag](x: X): Option[O]
}

abstract class HasProductType[O : TypeTag] extends HasType[Product, Seq[O]] {
  def mapTyped[X <: Product : TypeTag](product: X): Option[Seq[O]] = {
    val tag = TypeTagSupport.mostSpecificRuntimeTypeTag(product, typeTag[X])
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
    val givenTag = TypeTagSupport.mostSpecificRuntimeTypeTag(x, typeTag[X])
    if (givenTag.tpe <:< weakTag.tpe) {
      val elemTag = fromContainerElem[K, Any](givenTag.asInstanceOf[TypeTag[K[Any]]])
      elemTag
        .map { tag => mapTyped[Any](x.asInstanceOf[K[Any]])(tag) }
        .getOrElse { mapEmpty(x.asInstanceOf[K[Any]]) }
    } else
      None
  }

  def mapEmpty[E](data: K[E]): Option[O]
  def mapTyped[E : TypeTag](data: K[E]): Option[O]
}

abstract class HasMapType[M[X, Y], O](implicit val weakTag: WeakTypeTag[M[_, _]], val outTag: TypeTag[O])
  extends SimpleExtractor[Any, O] {

  def apply[X <: Any : TypeTag](x: X): Option[O] = {
    val givenTag = TypeTagSupport.mostSpecificRuntimeTypeTag(x, typeTag[X])
    if (givenTag.tpe <:< weakTag.tpe) {
      val optTags = fromMapElem[M, Nothing, Any](givenTag.asInstanceOf[TypeTag[M[Nothing, Any]]])
      optTags
        .map { tags => mapTyped[Nothing, Any](x.asInstanceOf[M[Nothing, Any]])(tags._1, tags._2) }
        .getOrElse { mapEmpty(x.asInstanceOf[M[Nothing, Any]]) }
    } else
      None
  }

  def mapEmpty[K, V](data: M[K, V]): Option[O]
  def mapTyped[K : TypeTag, V : TypeTag](data: M[K, V]): Option[O]
}




