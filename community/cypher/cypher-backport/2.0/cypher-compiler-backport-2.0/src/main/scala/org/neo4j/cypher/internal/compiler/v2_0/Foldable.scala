/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0

import scala.annotation.tailrec

object Foldable {
  implicit class TreeAny(val that: Any) extends AnyVal {
    def children: Iterator[AnyRef] = that match {
      case p: Product => p.productIterator.asInstanceOf[Iterator[AnyRef]]
      case s: Seq[_] => s.toIterator.asInstanceOf[Iterator[AnyRef]]
      case _ => Iterator.empty
    }
  }

  implicit class FoldableAny(val that: Any) extends AnyVal {
    def fold[R](init: R)(f: PartialFunction[Any, R => R]): R =
      foldAcc(List(that), init, f.lift)

    def foldt[R](init: R)(f: PartialFunction[Any, (R, R => R) => R]): R =
      foldtAcc(List(that), init, f)
  }

  @tailrec
  private def foldAcc[R](those: List[Any], acc: R, f: Any => Option[R => R]): R = those match {
    case Nil =>
      acc
    case that :: rs =>
      foldAcc(that.children.toList ++ rs, f(that).fold(acc)(_(acc)), f)
  }

  // partially tail-recursive (recursion is unavoidable for partial function matches)
  private def foldtAcc[R](those: List[Any], acc: R, f: PartialFunction[Any, (R, R => R) => R]): R = those match {
    case Nil =>
      acc
    case that :: rs =>
      if (f.isDefinedAt(that))
        foldtAcc(rs, f(that)(acc, foldtAcc(that.children.toList, _, f)), f)
      else
        foldtAcc(that.children.toList ++ rs, acc, f)
  }
}

trait Foldable
