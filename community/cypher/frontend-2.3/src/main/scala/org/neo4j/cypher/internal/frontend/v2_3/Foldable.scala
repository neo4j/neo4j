/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag

object Foldable {
  implicit class TreeAny(val that: Any) extends AnyVal {
    def children: Iterator[AnyRef] = that match {
      case p: Product => p.productIterator.asInstanceOf[Iterator[AnyRef]]
      case s: Seq[_] => s.iterator.asInstanceOf[Iterator[AnyRef]]
      case s: Set[_] => s.iterator.asInstanceOf[Iterator[AnyRef]]
      case _ => Iterator.empty.asInstanceOf[Iterator[AnyRef]]
    }

    def reverseChildren: Iterator[AnyRef] = that match {
      case p: Product => reverseProductIterator(p)
      case s: Seq[_] => s.reverseIterator.asInstanceOf[Iterator[AnyRef]]
      case s: Set[_] => s.iterator.asInstanceOf[Iterator[AnyRef]]
      case _ => Iterator.empty.asInstanceOf[Iterator[AnyRef]]
    }

    private def reverseProductIterator(p: Product) = new Iterator[AnyRef] {
      private var c: Int = p.productArity - 1
      def hasNext = c >= 0
      def next() = { val result = p.productElement(c).asInstanceOf[AnyRef]; c -= 1; result }
    }
  }

  implicit class FoldableAny(val that: Any) extends AnyVal {
    def fold[R](init: R)(f: PartialFunction[Any, R => R]): R =
      foldAcc(mutable.ArrayStack(that), init, f.lift)

    def treeFold[R](init: R)(f: PartialFunction[Any, (R, R => R) => R]): R =
      treeFoldAcc(mutable.ArrayStack(that), init, f.lift)

    def exists(f: PartialFunction[Any, Boolean]) =
      existsAcc(mutable.ArrayStack(that), f.lift)

    def findByClass[A : Manifest]: A =
      findAcc[A](mutable.ArrayStack(that))
  }

  @tailrec
  private def foldAcc[R](remaining: mutable.ArrayStack[Any], acc: R, f: Any => Option[R => R]): R =
    if (remaining.isEmpty) {
      acc
    } else {
      val that = remaining.pop()
      foldAcc(remaining ++= that.reverseChildren, f(that).fold(acc)(_(acc)), f)
    }

  // partially tail-recursive (recursion is unavoidable for partial function matches)
  private def treeFoldAcc[R](remaining: mutable.ArrayStack[Any], acc: R, f: Any => Option[(R, R => R) => R]): R =
    if (remaining.isEmpty) {
      acc
    } else {
      val that = remaining.pop()
      f(that) match {
        case None =>
          treeFoldAcc(remaining ++= that.reverseChildren, acc, f)
        case Some(pf) =>
          treeFoldAcc(remaining, pf(acc, treeFoldAcc(mutable.ArrayStack() ++= that.reverseChildren, _, f)), f)
      }
    }

  @tailrec
  private def existsAcc(remaining: mutable.ArrayStack[Any], f: Any => Option[Boolean]): Boolean =
    if (remaining.isEmpty) {
      false
    } else {
      val that = remaining.pop()
      f(that) match {
        case Some(true) =>
          true
        case _ =>
          existsAcc(remaining ++= that.reverseChildren, f)
      }
    }

  @tailrec
  private def findAcc[A : ClassTag](remaining: mutable.ArrayStack[Any]): A =
    if (remaining.isEmpty) {
      throw new NoSuchElementException
    } else {
      val that = remaining.pop()
      that match {
        case x: A => x
        case _ => findAcc(remaining ++= that.reverseChildren)
      }
    }
}

trait Foldable
