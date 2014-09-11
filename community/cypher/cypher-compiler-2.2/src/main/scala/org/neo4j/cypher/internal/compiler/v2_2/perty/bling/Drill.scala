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
 * A drill is an extractor that can be specialized over how to convert inner (child)
 * values by providing it with an inner extractor
 *
 * When used as a regular extractor, drills are specialized using their own fix point
 *
 * Additionally, drills may be provided with additional error handling by giving them
 * a function that maps over any input to specialize
 */
trait Drill[-I, O]  {
  self: Extractor[I, O] with Drill[I, O] =>

  def apply[X <: I : TypeTag](x: X): Option[O] = fixPoint(x)

  def recover(alt: Extractor[Any, O]): Self[I, O] with Drill[I, O] = ??? // recover(_ orElse alt)
  def recover(g: Extractor[Any, O] => Extractor[Any, O]): Self[I, O] with Drill[I, O]

  def fixPoint: Extractor[I, O]
  def specialize(inner: Extractor[Any, O]): Extractor[I, O]
}

object Drill {

  abstract class Simple[-I : TypeTag, O : TypeTag] extends SimpleExtractor[I, O] with Drill[I, O] {
    self =>

    override val fixPoint: Extractor[I, O] = self.mkFixPoint
  }

  // Drill that ignores it's specialization and always returns None
  case class empty[O : TypeTag]() extends SimpleExtractor[Any, O] with Drill[Any, O] {
    self =>

    override def apply[X <: Any : TypeTag](x: X) = None

    def recover(g: Extractor[Any, O] => Extractor[Any, O]) = self

    def fixPoint = self
    def specialize(inner: Extractor[Any, O]): Extractor[Any, O] = self
  }

  // Drill from total function
  implicit class mkDrill[I : TypeTag, O : TypeTag](f: Extractor[Any, O] => Extractor[I, O]) extends Simple[I, O] {
    self =>

    def recover(g: Extractor[Any, O] => Extractor[Any, O]) = mkDrill(g andThen f)

    def specialize(inner: Extractor[Any, O]) = f(inner)
  }

  // Helper for creating fix points
  implicit final class FixPoint[-I : TypeTag, O : TypeTag](drill: Drill[I, O]) extends SimpleExtractor[Any, O] {
    self =>

    def apply[X <: Any : TypeTag](x: X) =
      if (typeOf[X] <:< typeOf[I]) drill.fixPoint(x.asInstanceOf[I]) else None

    def mkFixPoint = drill.specialize(self)

    override def toString = s"FixPoint($drill)@${super.toString}"
  }
}



