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
package org.neo4j.cypher.internal.compiler.v2_2.perty.gen

import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.perty.format.{quoteChar, quoteString}
import org.neo4j.cypher.internal.compiler.v2_2.perty.recipe.Pretty

import scala.reflect.runtime.universe.TypeTag

case object scalaDocGen extends CustomDocGen[Any] {

  import Pretty._

  def apply[X <: Any : TypeTag](x: X): Option[DocRecipe[Any]] = x match {

    case v: String =>
      Pretty(quoteString(v))

    case ch: Char =>
      Pretty(quoteChar(ch))

    //    case a: Array[_] => (inner) =>
    //      val innerDocs = a.map(inner)
    //      block("Array")(sepList(innerDocs))
    //
    //    case m: mutable.Map[_, _] => (inner) =>
    //      val mapType = m.getClass.getSimpleName
    //      val innerDocs = m.map { case (k, v) => nest(group(inner(k) :/: "→ " :: inner(v)))}
    //      block(mapType)(sepList(innerDocs))
    //
    //    case m: immutable.Map[_, _] => (inner) =>
    //      val innerDocs = m.map { case (k, v) => nest(group(inner(k) :/: "→ " :: inner(v)))}
    //      block("Map")(sepList(innerDocs))
    //
    //    case l: List[_] => (inner) =>
    //      group(l.foldRight[Doc]("⬨") { case (v, doc) => inner(v) :/: "⸬ " :: doc})
    //
    //    case s: mutable.Set[_] => (inner) =>
    //      val setType = s.getClass.getSimpleName
    //      val innerDocs = s.map(inner)
    //      block(setType)(sepList(innerDocs))
    //
    //    case s: immutable.Set[_] => (inner) =>
    //      val innerDocs = s.map(inner)
    //      block("Set")(sepList(innerDocs))
    //
    //    case s: Seq[_] => (inner) =>
    //      val seqType = s.getClass.getSimpleName
    //      val innerDocs = s.map(inner)
    //      block(seqType)(sepList(innerDocs))
    //
    //    case p: Product => (inner) =>
    //      if (isTuple(p)) {
    //        block("")(sepList(p.productIterator.map(inner)))
    //      } else {
    //        if (p.productArity == 0)
    //          text(p.productPrefix)
    //        else
    //          block(p.productPrefix)(sepList(p.productIterator.map(inner)))
    //      }
    //  }
    case _ =>
      None
  }

  private def isTuple(p: Product) = p.productPrefix.startsWith("Tuple")
}
