/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.util.v3_4

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
      case m: Map[_, _] => m.iterator.asInstanceOf[Iterator[AnyRef]]
      case _ => Iterator.empty.asInstanceOf[Iterator[AnyRef]]
    }

    def reverseChildren: Iterator[AnyRef] = that match {
      case p: Product => reverseProductIterator(p)
      case s: Seq[_] => s.reverseIterator.asInstanceOf[Iterator[AnyRef]]
      case s: Set[_] => s.iterator.asInstanceOf[Iterator[AnyRef]]
      case m: Map[_, _] => m.iterator.asInstanceOf[Iterator[AnyRef]]
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

    /*
    Allows searching through object tree and object collections
     */
    def treeExists(f: PartialFunction[Any, Boolean]): Boolean =
      existsAcc(mutable.ArrayStack(that), f.lift)

    /*
    Allows searching through object tree and object collections
     */
    def treeFind[A : ClassTag](f: PartialFunction[A, Boolean]): Option[A] =
      findAcc[A](mutable.ArrayStack(that), f.lift)

    /*
    Searches in trees, counting how many matches are found
     */
    def treeCount(f: PartialFunction[Any, Boolean]): Int = {
      countAcc(mutable.ArrayStack(that), f.lift, 0)
    }

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

      result
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
  private def countAcc(remaining: mutable.ArrayStack[Any], f: Any => Option[Boolean], acc: Int): Int =
    if (remaining.isEmpty) {
      acc
    } else {
      val that = remaining.pop()
      val next = f(that) match {
        case Some(true) =>
          acc + 1
        case _ =>
          acc
      }

      countAcc(remaining ++= that.reverseChildren, f, next)
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

  @tailrec
  private def findAcc[A : ClassTag](remaining: mutable.ArrayStack[Any], predicate: A => Option[Boolean]): Option[A] =
    if (remaining.isEmpty) {
      None
    } else {
      val that = remaining.pop()
      that match {
        case x: A if predicate(x).isDefined => Some(x)
        case _ => findAcc(remaining ++= that.reverseChildren, predicate)
      }
    }
}

trait Foldable
