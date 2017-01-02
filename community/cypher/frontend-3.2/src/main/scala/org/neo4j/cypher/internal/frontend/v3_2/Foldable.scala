/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_2

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag

object Foldable {

  type TreeFold[R] = PartialFunction[Any, R => (R, Option[R => R])]

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

    /**
      * Fold of a tree structure
      *
      * This treefold will traverse the tree structure with a BFS strategy which can be customized as follows.
      *
      * The treefold behaviour is controlled by the given partial function: when the partial function is undefined on a
      * given node of the tree, that node is simply ignored and the tree traversal will continue.  If the partial
      * function is defined on the node than it will visited in different ways, depending on the output of the function
      * returned, as explained below.
      *
      * This function will be called with the current accumulator and it is expected to produce a pair compound of the
      * next accumulator and an optional function that given an accumulator will produce a new accumulator.
      *
      * If the optional function is undefined then the children of the current node are skipped and not traversed. Then
      * the new accumulator is used as initial value for traversing the siblings of the node.
      *
      * If the optional function is defined then the children are traversed and the new accumulator is used as initial
      * accumulator for this "inner treefold". After this computation the function is used for creating the accumulator
      * for traversing the siblings of the node by applying it to the output accumulator from the traversal of the
      * children.
      *
      * @param init the initial value of the accumulator
      * @param f    partial function that given a node in the tree might return a function that takes the current
      *             accumulator, and return a pair compound by the new accumulator for continuing the fold and an
      *             optional function that takes an accumulator and returns an accumulator
      * @tparam R the type of the accumulator/result
      * @return the accumulated result
      */
    def treeFold[R](init: R)(f: PartialFunction[Any, R => (R, Option[R => R])]): R =
      treeFoldAcc(mutable.ArrayStack(that), init, f.lift, new mutable.ArrayStack[(mutable.ArrayStack[Any], R => R)](), reverse = false)

    def reverseTreeFold[R](init: R)(f: PartialFunction[Any, R => (R, Option[R => R])]): R =
      treeFoldAcc(mutable.ArrayStack(that), init, f.lift, new mutable.ArrayStack[(mutable.ArrayStack[Any], R => R)](), reverse = true)

    def exists(f: PartialFunction[Any, Boolean]) =
      existsAcc(mutable.ArrayStack(that), f.lift)

    def findByClass[A : ClassTag]: A =
      findAcc[A](mutable.ArrayStack(that))

    def findByAllClass[A: ClassTag]: Seq[A] = {
      val remaining = mutable.ArrayStack(that)
      var result = mutable.ListBuffer[A]()

      while (remaining.nonEmpty) {
        val that = remaining.pop()
        that match {
          case x: A => result += x
          case _ =>
        }
        remaining ++= that.reverseChildren
      }

      result.toSeq
    }
  }

  @tailrec
  private def foldAcc[R](remaining: mutable.ArrayStack[Any], acc: R, f: Any => Option[R => R]): R =
    if (remaining.isEmpty) {
      acc
    } else {
      val that = remaining.pop()
      foldAcc(remaining ++= that.reverseChildren, f(that).fold(acc)(_(acc)), f)
    }

  @tailrec
  private def treeFoldAcc[R](remaining: mutable.ArrayStack[Any], acc: R, f: Any => Option[R => (R, Option[R => R])],
                             continuation: mutable.ArrayStack[(mutable.ArrayStack[Any], R => R)], reverse: Boolean): R =
    if (remaining.isEmpty) {
      if (continuation.isEmpty) {
        acc
      } else {
        val (stack, contAccFunc) = continuation.pop()
        treeFoldAcc(stack, contAccFunc(acc), f, continuation, reverse)
      }
    } else {
      val that = remaining.pop()
      f(that) match {
        case None =>
          val children = if (reverse) that.children else that.reverseChildren
          treeFoldAcc(remaining ++= children, acc, f, continuation, reverse)
        case Some(pf) =>
          pf(acc) match {
            case (newAcc, Some(contAccFunc)) =>
              continuation.push((remaining, contAccFunc))
              val children = if (reverse) that.children else that.reverseChildren
              treeFoldAcc(mutable.ArrayStack() ++= children, newAcc, f, continuation, reverse)
            case (newAcc, None) =>
              treeFoldAcc(remaining, newAcc, f, continuation,reverse)
          }
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