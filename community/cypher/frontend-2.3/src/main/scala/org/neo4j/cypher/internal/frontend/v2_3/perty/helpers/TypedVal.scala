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

import TypeTagSupport.mostSpecificRuntimeTypeTag
import org.neo4j.cypher.internal.frontend.v2_3.perty.Extractor

import scala.reflect.runtime.universe._

sealed abstract class TypedVal[T] {
  def tag: TypeTag[T]
  def value: T

  def apply[U >: T, O](extractor: Extractor[U, O]): Option[O] = extractor[T](value)(tag)
}

final case class StrictVal[T](value: T)(implicit valueTag: TypeTag[T]) extends TypedVal[T] {
  def tag = mostSpecificRuntimeTypeTag(value, valueTag)
}

object LazyVal {
  def apply[T](lazyValue: => T)(implicit lazyValueTag: TypeTag[T]) = new TypedVal[T] {
    private var _hasValue: Boolean = false
    private var _value: T = null.asInstanceOf[T]

    def value = {
      if (!_hasValue) {
        _value = lazyValue
      }
      _value
    }

    def tag = mostSpecificRuntimeTypeTag(value, lazyValueTag)
  }
}
