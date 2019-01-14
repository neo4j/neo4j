/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.lang.reflect.Method

import org.neo4j.cypher.internal.util.v3_4.Foldable._
import org.neo4j.cypher.internal.util.v3_4.Rewritable._

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.{HashMap => MutableHashMap}

object Rewriter {
  def lift(f: PartialFunction[AnyRef, AnyRef]): Rewriter =
    f.orElse(PartialFunction(identity[AnyRef]))

  val noop: Rewriter = Rewriter.lift(PartialFunction.empty)
}

object RewriterWithArgs {
  def lift(f: PartialFunction[(AnyRef, Seq[AnyRef]), AnyRef]): RewriterWithArgs =
    f.orElse(PartialFunction({
      // We need to dup anything not matched by f given the children
      case (p: Product, children) => Rewritable.dupProduct(p, children).asInstanceOf[AnyRef]
      case (a: AnyRef, children) => Rewritable.dupAny(a, children)
      case (null, _) => null
    }))
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

  private val productCopyConstructors = new ThreadLocal[MutableHashMap[Class[_], Method]]() {
    override def initialValue: MutableHashMap[Class[_], Method] =
      new MutableHashMap[Class[_], Method]
  }

  def copyConstructor(product: Product): Method = {
    def getCopyMethod(productClass: Class[_ <: Product]): Method = {
      try {
        productClass.getMethods.find(_.getName == "copy").get
      } catch {
        case e: NoSuchElementException =>
          throw new InternalException(
            s"Failed trying to rewrite $productClass - this class does not have a `copy` method"
          )
      }
    }

    val productClass = product.getClass
    productCopyConstructors.get.getOrElseUpdate(productClass, getCopyMethod(productClass))
  }

  def dupAny(that: AnyRef, children: Seq[AnyRef]): AnyRef =
    try {
      if (children.iterator eqElements that.children) {
        that
      } else {
        that match {
          case a: Rewritable =>
            a.dup(children)
          case p: Product =>
              copyConstructor(p).invoke(p, children: _*)
          case _: IndexedSeq[_] =>
            children.toIndexedSeq
          case _: Seq[_] =>
            children
          case _: Set[_] =>
            children.toSet
          case _: Map[_, _] =>
            children.map(value => value.asInstanceOf[(String, AnyRef)]).toMap
          case t =>
            t
        }
      }
    } catch {
      case e: IllegalArgumentException =>
        throw new InternalException(s"Failed rewriting $that\nTried using children: $children", e)
    }

  def dupProduct(product: Product, children: Seq[AnyRef]): Product = product match {
    case a: Rewritable =>
      a.dup(children)
    case _ =>
      if (children.iterator eqElements product.children)
        product
      else
        copyConstructor(product).invoke(product, children: _*).asInstanceOf[Product]
  }

  implicit class RewritableAny[T <: AnyRef](val that: T) extends AnyVal {
    def rewrite(rewriter: Rewriter): AnyRef = {
      val result = rewriter.apply(that)
      result
    }

    def rewrite(rewriter: RewriterWithArgs, args: Seq[AnyRef]): AnyRef = {
      val result = rewriter.apply((that, args))
      result
    }

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
  private class TopDownRewriter(rewriter: Rewriter, val stopper: AnyRef => Boolean)
      extends Rewriter {
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
          val doneJob = Rewritable.dupAny(job, newChildren)
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

  private class BottomUpRewriter(val rewriter: Rewriter, val stopper: AnyRef => Boolean)
      extends Rewriter {
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
          val doneJob = Rewritable.dupAny(job, newChildren)
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

object bottomUpWithArgs {

  private class BottomUpWithArgsRewriter(val rewriter: RewriterWithArgs, val stopper: AnyRef => Boolean)
    extends RewriterWithArgs {
    override def apply(tuple: (AnyRef, Seq[AnyRef])): AnyRef = {
      val (that: AnyRef, _) = tuple
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
          val doneJob = job.rewrite(rewriter, newChildren)
          stack.push((jobs, doneJobs += doneJob))
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

  def apply(rewriter: RewriterWithArgs, stopper: (AnyRef) => Boolean = _ => false): RewriterWithArgs =
    new BottomUpWithArgsRewriter(rewriter, stopper)
}
