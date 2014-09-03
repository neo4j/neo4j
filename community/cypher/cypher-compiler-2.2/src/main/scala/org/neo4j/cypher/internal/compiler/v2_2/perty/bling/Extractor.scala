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

import scala.language.higherKinds
import scala.language.implicitConversions

import scala.reflect.runtime.universe._

/**
 * Type of an "unapply-like" converter from I to O
 **/
abstract class Extractor[-I : TypeTag, O : TypeTag] {
  self =>

  type Self[-U, V] <: Extractor[U, V]

  // Run extractor, trying to extract a value of type O
  def apply[X <: I : TypeTag](x: X): Option[O]

  // Return extractor that runs other after running self if self does not extract a value
  def orElse[H <: I : TypeTag](other: Extractor[H, O]): Extractor[H, O]

  // Return extractor that runs other on any value extracted by self
  def andThen[P >: O : TypeTag, V : TypeTag](other: Extractor[P, V]): Extractor[I, V]

  // Return extractor with different input type that runs self for any value <: I && J
  def lift[J : TypeTag]: Self[J, O] = mapInput(Extractor.fromFilteredInput[J, I])

  // Return extractor with different output type that projects result of running self if O <: V
  def view[V : TypeTag]: Extractor[I, V] = andThen[O, V](Extractor.fromFilteredInput[O, V])

  // Return extractor that runs self on an input value obtained by mapping via f
  def mapInput[U : TypeTag, H <: I : TypeTag](f: Extractor[U, H]): Self[U, O]

  // Like andThen but returns Self
  def mapOutput[H <: I : TypeTag](f: Extractor[O, O]): Self[H, O]

  // Implementation specific "orElse" composition
  def ++[H <: I : TypeTag](other: Self[H, O]): Self[H, O]
}

object Extractor {
  def empty[O : TypeTag] = new SimpleExtractor[Any, O] {
    def apply[X <: Any : TypeTag](x: X) = None
  }

  def fromIdentity[T : TypeTag] = fromInput[T, T]

  def fromInput[I  <: O : TypeTag, O : TypeTag] = new SimpleExtractor[I, O] {
    def apply[X <: I : TypeTag](x: X) = Some(x)
  }

  def fromFilteredInput[I : TypeTag, O : TypeTag]  = new SimpleExtractor[I, O] {
    def apply[X <: I : TypeTag](x: X): Option[O] = {
      val typeX: Type = typeOf[X]
      val typeO: Type = typeOf[O]
      if (typeX <:< typeO) Some(x.asInstanceOf[O]) else None
    }
  }

  // Extractor from partial function
  implicit def pick[I : TypeTag, O : TypeTag](pf: PartialFunction[I, Option[O]]): Extractor[I, O] =
    extract(pf.lift.andThen(_.flatten))

  // Extractor from total function
  implicit def extract[I : TypeTag, O : TypeTag](f: I => Option[O]): Extractor[I, O] = {
    new SimpleExtractor[I, O] {
      def apply[X <: I : TypeTag](x: X) = f(x)
    }
  }

  // Use fallbackExtractor as recovery strategy for inner extractor by wrapping
  // a given drill
  implicit final class fallback[O : TypeTag](fallbackExtractor: Extractor[Any, O]) {
    def recover[I : TypeTag](original: Drill[I, O]): Drill[I, O] =
      (extractor: Extractor[Any, O]) => original(extractor orElse fallbackExtractor)
  }
}



