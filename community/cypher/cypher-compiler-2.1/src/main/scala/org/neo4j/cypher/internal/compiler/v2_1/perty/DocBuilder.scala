/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.perty

import org.neo4j.cypher.internal.helpers.PartialFunctionSupport
import scala.reflect.ClassTag

abstract class DocBuilder[T: ClassTag] extends HasDocGenerator[T] {
  self =>

  import DocBuilder.asDocBuilder

  def nestedDocGenerator: NestedDocGenerator[T]
  def docGenerator: DocGenerator[T]

  def orElse(other: DocBuilder[T]): DocBuilderChain[T] = other match {
    case others: DocBuilderChain[T] => others.prepend(self)
    case _                          => SimpleDocBuilderChain(self, other)
  }

  final def uplifted[S >: T: ClassTag]: DocBuilder[S] =
    asDocBuilder(PartialFunctionSupport.uplift(self.nestedDocGenerator))
}

object DocBuilder {
  implicit def asDocBuilder[T: ClassTag](nestedDocGen: NestedDocGenerator[T]): DocBuilder[T] =
    SimpleDocBuilder(nestedDocGen)
}

final case class SimpleDocBuilder[T: ClassTag](nestedDocGenerator: NestedDocGenerator[T]) extends DocBuilder[T] {
  val docGenerator: DocGenerator[T] = PartialFunctionSupport.fix(nestedDocGenerator)
}

abstract class CachingDocBuilder[T: ClassTag] extends DocBuilder[T] {

  def nestedDocGenerator = cachedDocGenerators.nestedDocGenerator
  def docGenerator = cachedDocGenerators.docGenerator

  protected def newNestedDocGenerator: NestedDocGenerator[T]

  private object cachedDocGenerators {
    lazy val nestedDocGenerator: NestedDocGenerator[T] = newNestedDocGenerator
    lazy val docGenerator: DocGenerator[T] = PartialFunctionSupport.fix(nestedDocGenerator)
  }
}

abstract class DocBuilderChain[T: ClassTag] extends CachingDocBuilder[T] {
  val builders: Seq[DocBuilder[T]]

  override protected def newNestedDocGenerator: NestedDocGenerator[T] =
    builders
      .map(_.nestedDocGenerator)
      .reduceLeftOption(_ orElse _)
      .getOrElse(PartialFunction.empty)

  override def orElse(other: DocBuilder[T]): DocBuilderChain[T] = other match {
    case chain: DocBuilderChain[T] => appendAll(chain.builders)
    case _                         => append(other)
  }

  def appendAll(others: Seq[DocBuilder[T]]) = SimpleDocBuilderChain[T](builders ++ others: _*)

  def prepend(builder: DocBuilder[T]) = SimpleDocBuilderChain[T](builder +: builders: _*)
  def append(builder: DocBuilder[T]) = appendAll(Seq(builder))
}

case class SimpleDocBuilderChain[T: ClassTag](builders: DocBuilder[T]*) extends DocBuilderChain[T]

trait TopLevelDocBuilder[T]  {
  self: DocBuilder[T] =>

  trait AsPretty extends GeneratedPretty[T] {
    prettySelf: T =>

    override def docGenerator: DocGenerator[T] = self.docGenerator
  }

  trait AsPrettyToString extends AsPretty with PrettyToString {
    prettySelf: T =>
  }
}
