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

import scala.reflect.ClassTag

case class propagateExtractionFailure[M, O]() extends ExtractionFailureHandler[M, O] {
  def apply(f: (M => O) => O) =
      (layered: Extractor[M, O]) =>
        try { Some(f(catchInconvertible(layered))) }
        catch { case e: InconvertibleValueException[M, O] => None }
}

case class replaceExtractionFailure[M, O](replacement: O) extends ExtractionFailureHandler[M, O] {
  def apply(f: (M => O) => O) =
    (layered: Extractor[M, O]) =>
      Some(f(replaceInconvertible(layered, replacement)))
}

abstract class MapExtractionFailureHandler[E <: Throwable : ClassTag, M, O](g: E => Option[O]) extends ExtractionFailureHandler[M, O] {
  def apply(f: (M => O) => O) =
    (layered: Extractor[M, O]) =>
      try { Some(f(x => layered(x).get)) }
      catch { case e: E => g(e) }
}

case class mapExtractionFailure[E <: Throwable : ClassTag, M, O](g: E => Option[O])
  extends MapExtractionFailureHandler[E, M, O](g)

case class InconvertibleValueException[M, O](layer: Extractor[M, O], v: M)
  extends IllegalArgumentException(s"Could not run layered extractor on value of type ${v.getClass}")

case object catchInconvertible {
  def apply[M, O](extractor: Extractor[M, O]): M => O =
    m => extractor(m).getOrElse(throw InconvertibleValueException[M, O](extractor, m))
}

case object replaceInconvertible {
  def apply[M, O](extractor: Extractor[M, O], replacement: O): M => O =
    m => extractor(m).getOrElse(replacement)
}




