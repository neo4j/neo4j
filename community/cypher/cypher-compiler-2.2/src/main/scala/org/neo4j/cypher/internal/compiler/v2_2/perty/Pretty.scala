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

import org.neo4j.cypher.internal.compiler.v2_2.perty
import org.neo4j.cypher.internal.compiler.v2_2.perty.docbuilders.defaultDocBuilder

trait Pretty[+T] {
  def toDoc(pretty: FixedDocGenerator[T]): Doc
}

trait PrettyToString[T] {
  self: Pretty[T] with Representable[T] =>

  def docGenerator: FixedDocGenerator[T] = defaultDocBuilder.docGenerator

  override def toString = printToString(self.docFormatter(toDoc(docGenerator)))
}

trait GeneratorToString[T] {
  self: T with Representable[T] =>

  override def toString = printToString(docFormatter(docGenerator(self)))
}

trait ToStringDocBuilder[T]  {
  self: DocBuilder[T] =>

  trait Representable[S <: T] extends HasDocFormatter with HasDocGenerator[S]

  trait GeneratorToString[S <: T] extends Representable[S] with perty.GeneratorToString[S] {
    prettySelf: S =>

    override def docGenerator: FixedDocGenerator[S] = self.docGenerator
  }

  trait PrettyToString[S <: T] extends Representable[S] with perty.Pretty[S] with perty.PrettyToString[S] {
    prettySelf: S =>

    override def docGenerator: FixedDocGenerator[S] = self.docGenerator
  }
}
