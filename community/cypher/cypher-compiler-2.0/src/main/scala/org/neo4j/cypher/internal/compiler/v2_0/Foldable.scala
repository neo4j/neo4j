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
package org.neo4j.cypher.internal.compiler.v2_0

import scala.annotation.tailrec
import scala.collection.mutable

object Foldable {
  implicit class TreeAny(val that: Any) extends AnyVal {
    def children: Iterator[AnyRef] = that match {
      case p: Product => p.productIterator.asInstanceOf[Iterator[AnyRef]]
      case s: Seq[_] => s.iterator.asInstanceOf[Iterator[AnyRef]]
      case _ => Iterator.empty.asInstanceOf[Iterator[AnyRef]]
    }

    def reverseChildren: Iterator[AnyRef] = that match {
      case p: Product => p.reverseProductIterator
      case s: Seq[_] => s.reverseIterator.asInstanceOf[Iterator[AnyRef]]
      case _ => Iterator.empty.asInstanceOf[Iterator[AnyRef]]
    }
  }

  implicit class ProductIteration(val p: Product) extends AnyVal {
    def reverseProductIterator: Iterator[AnyRef] = p.productArity match {
      case 0 => Iterator.empty.asInstanceOf[Iterator[AnyRef]]
      case 1 => Iterator.single[AnyRef](p.productElement(0).asInstanceOf[AnyRef])
      case _ => new Iterator[AnyRef] {
        private var c: Int = p.productArity - 1
        def hasNext = c >= 0
        def next() = { val result = p.productElement(c).asInstanceOf[AnyRef]; c -= 1; result }
      }
    }
  }

  implicit class FoldableAny(val that: Any) extends AnyVal {
    def fold[R](init: R)(f: PartialFunction[Any, R => R]): R =
      foldAcc(mutable.ArrayStack(that), init, f.lift)

    def foldt[R](init: R)(f: PartialFunction[Any, (R, R => R) => R]): R =
      foldtAcc(mutable.ArrayStack(that), init, f)
  }

  @tailrec
  private def foldAcc[R](remaining: mutable.ArrayStack[Any], acc: R, f: Any => Option[R => R]): R =
    if (remaining.isEmpty)
      acc
    else {
      val that = remaining.pop()
      foldAcc(remaining ++= that.reverseChildren, f(that).fold(acc)(_(acc)), f)
    }

  // partially tail-recursive (recursion is unavoidable for partial function matches)
  private def foldtAcc[R](remaining: mutable.ArrayStack[Any], acc: R, f: PartialFunction[Any, (R, R => R) => R]): R =
    if (remaining.isEmpty)
      acc
    else {
      val that = remaining.pop()
      if (f.isDefinedAt(that))
        foldtAcc(remaining, f(that)(acc, foldtAcc(mutable.ArrayStack() ++= that.reverseChildren, _, f)), f)
      else
        foldtAcc(remaining ++= that.reverseChildren, acc, f)
  }
}

trait Foldable
