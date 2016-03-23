/*
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
package org.neo4j.cypher.internal.frontend.v3_1

import java.lang.reflect.Method

import org.neo4j.cypher.internal.frontend.v3_1.Foldable._
import org.neo4j.cypher.internal.frontend.v3_1.Rewritable._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}

object Rewriter {
  def lift(f: PartialFunction[AnyRef, AnyRef]): Rewriter = f.orElse(PartialFunction(identity[AnyRef]))

  val noop = Rewriter.lift(PartialFunction.empty)
}

object Rewritable {
  implicit class IteratorEq[A <: AnyRef](val iterator: Iterator[A]) {
    def eqElements[B <: AnyRef](that: Iterator[B]): Boolean = {
      while (iterator.hasNext && that.hasNext) {
        if (!(iterator.next eq that.next))
          return false
      }
      !iterator.hasNext && !that.hasNext
    }
  }

  implicit class DuplicatableAny(val that: AnyRef) extends AnyVal {

    def dup(children: Seq[AnyRef]): AnyRef = try { that match {
        case a: Rewritable =>
          a.dup(children)
        case p: Product =>
          if (children.iterator eqElements p.children)
            p
          else
            p.copyConstructor.invoke(p, children: _*)
        case _: IndexedSeq[_] =>
          children.toIndexedSeq
        case _: Seq[_] =>
          children
        case _: Set[_] =>
          children.toSet
        case t =>
          t
      }
    } catch {
      case e: IllegalArgumentException =>
        throw new InternalException(s"Failed rewriting $that\nTried using children: $children", e)
    }
  }

  private val productCopyConstructors = new ThreadLocal[MutableHashMap[Class[_], Method]]() {
    override def initialValue: MutableHashMap[Class[_], Method] = new MutableHashMap[Class[_], Method]
  }

  implicit class DuplicatableProduct(val product: Product) extends AnyVal {

    def dup(children: Seq[AnyRef]): Product = product match {
      case a: Rewritable =>
        a.dup(children)
      case _ =>
        if (children.iterator eqElements product.children)
          product
        else
          copyConstructor.invoke(product, children: _*).asInstanceOf[Product]
    }

    def copyConstructor: Method = {
      val productClass = product.getClass
      productCopyConstructors.get.getOrElseUpdate(productClass, productClass.getMethods.find(_.getName == "copy").get)
    }
  }

  implicit class RewritableAny[T <: AnyRef](val that: T) extends AnyVal {
    def rewrite(rewriter: Rewriter): AnyRef = rewriter.apply(that)
    def endoRewrite(rewriter: Rewriter): T = rewrite(rewriter).asInstanceOf[T]
  }
}

case class TypedRewriter[T <: Rewritable](rewriter: Rewriter) extends (T => T) {
  def apply(that: T) = rewriter.apply(that).asInstanceOf[T]

  def narrowed[S <: T] = TypedRewriter[S](rewriter)
}

trait Rewritable {
  def dup(children: Seq[AnyRef]): this.type
}

object inSequence {
  private class InSequenceRewriter(rewriters: Seq[Rewriter]) extends Rewriter {
    override def apply(that: AnyRef): AnyRef = {
      val it = rewriters.iterator
      //this piece of code is used a lot and has been through profiling
      //please don't just remove it because it is ugly looking
      var result = that
      while (it.hasNext) {
        result = result.rewrite(it.next())
      }

      result
    }
  }

  def apply(rewriters: Rewriter*): Rewriter = new InSequenceRewriter(rewriters)
}

object topDown {
  private class TopDownRewriter(rewriter: Rewriter, val stopper: AnyRef => Boolean) extends Rewriter {
    override def apply(that: AnyRef): AnyRef = {
      val initialStack = mutable.ArrayStack((List(that), new mutable.MutableList[AnyRef]()))
      val result = rec(initialStack)
      assert(result.size == 1)
      result.head
    }

    @tailrec
    private def rec(stack: mutable.ArrayStack[(List[AnyRef], mutable.MutableList[AnyRef])]): mutable.MutableList[AnyRef] = {
      val (currentJobs, _) = stack.top
      if (currentJobs.isEmpty) {
        val (_, newChildren) = stack.pop()
        if (stack.isEmpty) {
          newChildren
        } else {
          val (job :: jobs, doneJobs) = stack.pop()
          val doneJob = job.dup(newChildren)
          stack.push((jobs, doneJobs += doneJob))
          rec(stack)
        }
      } else {
        val (newJob :: jobs, doneJobs) = stack.pop()
        if (stopper(newJob)) {
          stack.push((jobs, doneJobs += newJob))
        } else {
          val rewrittenJob = newJob.rewrite(rewriter)
          stack.push((rewrittenJob :: jobs, doneJobs))
          stack.push((rewrittenJob.children.toList, new mutable.MutableList()))
        }
        rec(stack)
      }
    }
  }

  def apply(rewriter: Rewriter, stopper: (AnyRef) => Boolean = _ => false): Rewriter =
    new TopDownRewriter(rewriter, stopper)
}

object bottomUp {

  private class BottomUpRewriter(val rewriter: Rewriter, val stopper: AnyRef => Boolean) extends Rewriter {
    override def apply(that: AnyRef): AnyRef = {
      val initialStack = mutable.ArrayStack((List(that), new mutable.MutableList[AnyRef]()))
      val result = rec(initialStack)
      assert(result.size == 1)
      result.head
    }

    @tailrec
    private def rec(stack: mutable.ArrayStack[(List[AnyRef], mutable.MutableList[AnyRef])]): mutable.MutableList[AnyRef] = {
      val (currentJobs, _) = stack.top
      if (currentJobs.isEmpty) {
        val (_, newChildren) = stack.pop()
        if (stack.isEmpty) {
          newChildren
        } else {
          val (job :: jobs, doneJobs) = stack.pop()
          val doneJob = job.dup(newChildren)
          val rewrittenDoneJob = doneJob.rewrite(rewriter)
          stack.push((jobs, doneJobs += rewrittenDoneJob))
          rec(stack)
        }
      } else {
        val next = currentJobs.head
        if (stopper(next)) {
          val (job :: jobs, doneJobs) = stack.pop()
          stack.push((jobs, doneJobs += job))
        } else {
          stack.push((next.children.toList, new mutable.MutableList()))
        }
        rec(stack)
      }
    }
  }

  def apply(rewriter: Rewriter, stopper: (AnyRef) => Boolean = _ => false): Rewriter =
    new BottomUpRewriter(rewriter, stopper)
}
