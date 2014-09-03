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
package org.neo4j.cypher.internal.compiler.v2_2.perty.bling

import scala.reflect.runtime.universe.TypeTag

abstract class SimpleExtractor[-I : TypeTag, O : TypeTag] extends Extractor[I, O] {
  self =>

  override type Self[-U, V] = SimpleExtractor[U, V]

  def mapInput[U : TypeTag, H <: I : TypeTag](f: Extractor[U, H]): Self[U, O] =
    new SimpleExtractor[U, O] {
      def apply[X <: U : TypeTag](x: X): Option[O] = f(x).flatMap(self(_))
    }

  def mapOutput[H <: I : TypeTag](f: Extractor[O, O]): Self[H, O] =
    new SimpleExtractor[H, O] {
      def apply[X <: H : TypeTag](x: X): Option[O] = self(x).flatMap(f(_))
    }

  override def orElse[H <: I : TypeTag](first: Extractor[H, O]): SimpleExtractor[H, O] =
    new SimpleExtractor[H, O] {
      def apply[X <: H : TypeTag](x: X) = self(x).orElse(first(x))
    }

  override def andThen[P >: O : TypeTag, V : TypeTag](other: Extractor[P, V]): SimpleExtractor[I, V] =
    new SimpleExtractor[I, V] {
      def apply[X <: I : TypeTag](x: X): Option[V] = self(x).flatMap(other(_))
    }

  // implementation specific "orElse" composition
  def ++[H <: I : TypeTag](other: Self[H, O]) = self orElse other
}
