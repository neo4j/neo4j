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
package org.neo4j.cypher.internal.compiler.v2_1.pprint

import org.neo4j.cypher.internal.helpers.PartialFunctionSupport
import scala.reflect.ClassTag

sealed abstract class DocBuilder[T: ClassTag] {
  self =>

  import DocBuilder.asDocBuilder

  def nested: NestedDocGenerator[T]
  final def docGen: DocGenerator[T] = PartialFunctionSupport.fix(nested)

  def orElse(other: DocBuilder[T]): DocBuilderChain[T] = other match {
    case others: DocBuilderChain[T] => others.prepend(self)
    case _                          => SimpleDocBuilderChain(self, other)
  }

  final def uplifted[S >: T: ClassTag]: DocBuilder[S] =
    asDocBuilder[S](PartialFunctionSupport.uplift[T, DocGenerator[T] => Doc, S](self.nested))
}

object DocBuilder {
  implicit def asDocBuilder[T: ClassTag](nestedDocGen: NestedDocGenerator[T]): SingleDocBuilder[T] =
    new SingleDocBuilder[T] {
      val nested = nestedDocGen
    }
}

abstract class SingleDocBuilder[T: ClassTag] extends DocBuilder[T] {
  override val nested: NestedDocGenerator[T]
}

abstract class DocBuilderChain[T: ClassTag] extends DocBuilder[T] {
  self =>

  def builders: Seq[DocBuilder[T]]

  def nested: NestedDocGenerator[T] =
    builders
      .map(_.nested)
      .reduceLeftOption(_ orElse _)
      .getOrElse(PartialFunction.empty)

  override def orElse(other: DocBuilder[T]): DocBuilderChain[T] = other match {
    case chain: DocBuilderChain[T] => appendAll(chain.builders)
    case _                         => append(other)
  }

  def flatten = new SingleDocBuilder[T] {
    val nested = self.nested
  }

  def prepend(builder: DocBuilder[T]) = SimpleDocBuilderChain[T](builder +: builders: _*)

  def append(builder: DocBuilder[T]) = appendAll(Seq(builder))

  def appendAll(others: Seq[DocBuilder[T]]) = SimpleDocBuilderChain[T](builders ++ others: _*)
}

case class SimpleDocBuilderChain[T: ClassTag](builders: DocBuilder[T]*) extends DocBuilderChain[T]
