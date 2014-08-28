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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.internal.helpers.PartialFunctionSupport._

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * See pp.Doc
 */
package object perty {
  // Knows how to layout a doc as series if print commands
  type DocFormatter = Doc => Seq[PrintCommand]

  // Given a doc generator for the layout of child components, knows how to represent a value of type T as a Doc
  type NestedDocGenerator[T] = PartialFunction[T, FixedDocGenerator[T] => Doc]

  // Knows how to represent a value of type T as a Doc
  type FixedDocGenerator[-T] = PartialFunction[T, Doc]

  type Representable[-T] = HasDocFormatter with HasDocGenerator[T]

  // Combines nested doc generator together with it's fixed doc generator
  final case class DocGenerator[T: ClassTag](nested: NestedDocGenerator[T])
    extends fixedPartialFunction(nested)
    with FixedDocGenerator[T] {

    def map[S: ClassTag](f: NestedDocGenerator[T] => NestedDocGenerator[S]) = DocGenerator[S](f(nested))
    def uplifted[S >: T: ClassTag]: DocGenerator[S] = DocGenerator(uplift[T, FixedDocGenerator[T] => Doc, S](nested))

    def applyWithFallback[S <: T](inner: FixedDocGenerator[S])(v: T)(implicit tag: ClassTag[S]) = {
      val fallback: NestedDocGenerator[T] = { case v: S => _ => inner(v) }
      DocGenerator[T](nested orElse fallback)(implicitly[ClassTag[T]])(v)
    }
  }

  object DocGenerator {
    implicit def fromNestedDocGenerator[T: ClassTag](nestedDocGenerator: NestedDocGenerator[T]) =
      DocGenerator(nestedDocGenerator)
  }

  // Builder for turning a sequence of print commands into a result of type T
  type PrintingConverter[+T] = mutable.Builder[PrintCommand, T]
}


