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
package org.neo4j.cypher.internal.compiler.v2_2.perty

import org.neo4j.cypher.internal.compiler.v2_2.perty.DocBuilder._
import org.neo4j.cypher.internal.compiler.v2_2.perty.docbuilders.catchErrors
import org.neo4j.cypher.internal.compiler.v2_2.perty.impl.{CachingDocBuilder, SimpleDocBuilder, SimpleDocBuilderChain}

import scala.reflect.ClassTag

// DocBuilders encapsulate and compose doc generators
abstract class DocBuilder[T: ClassTag] extends HasDocGenerator[T] {
  self =>

  def docGenerator: DocGenerator[T]

  def orElse(other: DocBuilder[T]): DocBuilderChain[T] = other match {
    case others: DocBuilderChain[T] => others.after(self)
    case _                          => SimpleDocBuilderChain(self, other)
  }

  def uplifted[S >: T: ClassTag]: DocBuilder[S] = fromDocGenerator(docGenerator.uplifted)
}

object DocBuilder {
  implicit def fromDocGenerator[T: ClassTag](docGen: DocGenerator[T]): DocBuilder[T] =
    SimpleDocBuilder(docGen)
}

// Doc builder for combining other doc builders efficiently into a single, flat list
abstract class DocBuilderChain[T: ClassTag] extends CachingDocBuilder[T] {
  def builders: Seq[DocBuilder[T]]

  override def newDocGenerator: DocGenerator[T] =
    DocGenerator(
      builders
        .map(_.docGenerator.nested)
        .reduceLeftOption(_ orElse _)
        .getOrElse(PartialFunction.empty)
    )

  override def orElse(other: DocBuilder[T]): DocBuilderChain[T] = other match {
    case chain: DocBuilderChain[T] => SimpleDocBuilderChain[T](builders ++ chain.builders: _*)
    case _                         => SimpleDocBuilderChain[T](builders :+ other: _*)
  }

  def after(builder: DocBuilder[T]) = SimpleDocBuilderChain[T](builder +: builders: _*)
}

// Public super class of custom doc builder
abstract class CustomDocBuilder[T: ClassTag] extends CachingDocBuilder[T] with PrettyDocBuilder[T]

// Public super class of custom doc builder chain
abstract class CustomDocBuilderChain[T: ClassTag] extends DocBuilderChain[T] with PrettyDocBuilder[T] {
  override def newDocGenerator = super.newDocGenerator.map(docGen => catchErrors(docGen))
}









