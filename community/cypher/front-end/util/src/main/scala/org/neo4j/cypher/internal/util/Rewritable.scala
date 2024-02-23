/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.util

import org.neo4j.cypher.internal.util.Foldable.TreeAny
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

import scala.annotation.tailrec
import scala.collection.IterableFactory
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Rewriter {

  def lift(f: PartialFunction[AnyRef, AnyRef]): Rewriter =
    f.orElse({ case x => x })

  val noop: Rewriter = Rewriter.lift(PartialFunction.empty)
}

object RewriterWithParent {

  def lift(f: PartialFunction[(AnyRef, Option[AnyRef]), AnyRef]): RewriterWithParent =
    f.orElse({ case (x, _) => x })
}

object Rewritable {

  def copyProduct(product: Product, children: Array[AnyRef]): AnyRef = {
    if (CrossCompilation.isTeaVM())
      RewritableJavascript.copyProduct(product, children)
    else {
      RewritableJava.copyProduct(product, children)
    }
  }

  def numParameters(product: Product): Int = {
    if (CrossCompilation.isTeaVM())
      RewritableJavascript.numParameters(product.getClass)
    else {
      RewritableJava.numParameters(product)
    }
  }

  def includesPosition(product: Product): Boolean = {
    if (CrossCompilation.isTeaVM())
      RewritableJavascript.lastParamIsPosition(product.getClass)
    else {
      RewritableJava.includesPosition(product)
    }
  }

  implicit class IteratorEq[A <: AnyRef](val iterator: Iterator[A]) {

    def eqElements[B <: AnyRef](that: Iterator[B]): Boolean = {
      while (iterator.hasNext && that.hasNext) {
        if (!(iterator.next() eq that.next()))
          return false
      }
      !iterator.hasNext && !that.hasNext
    }
  }

  def dupAny(that: AnyRef, children: Seq[AnyRef]): AnyRef =
    try {
      if (children.iterator eqElements that.treeChildren) {
        that
      } else {
        that match {
          case a: RewritableUniversal =>
            a.dup(children)
          case _: scala.collection.IndexedSeq[_] =>
            children.toIndexedSeq
          case _: List[_] =>
            children.toList
          case _: scala.collection.Seq[_] =>
            children
          case _: scala.collection.immutable.ListSet[_] =>
            // We should use our own ListSet, but let us keep this anyway.
            children.to(IterableFactory.toFactory(ListSet))
          case _: ListSet[_] =>
            children.to(IterableFactory.toFactory(ListSet))
          case _: scala.collection.Set[_] =>
            children.toSet
          case _: scala.collection.Map[_, _] =>
            val builder = Map.newBuilder[AnyRef, AnyRef]
            children.iterator.grouped(2).foreach {
              case Seq(k, v) => builder.addOne((k, v))
              case _         => throw new IllegalStateException()
            }
            builder.result()
          case p: Product =>
            copyProduct(p, children.toArray)
          case t =>
            t
        }
      }
    } catch {
      case e: IllegalArgumentException =>
        throw new IllegalStateException(s"Failed rewriting $that\nTried using children: ${children.mkString(",")}", e)
    }

  implicit class RewritableAny[T <: AnyRef](val that: T) extends AnyVal {

    def rewrite(rewriter: Rewriter): AnyRef = {
      val result = rewriter.apply(that)
      result
    }

    def rewrite(rewriter: RewriterWithParent, parent: Option[AnyRef]): AnyRef = {
      val result = rewriter.apply((that, parent))
      result
    }

    def endoRewrite(rewriter: Rewriter): T = rewrite(rewriter).asInstanceOf[T]
  }
}

case class TypedRewriter[T <: Rewritable](rewriter: Rewriter) extends (T => T) {
  def apply(that: T): T = rewriter.apply(that).asInstanceOf[T]

  def narrowed[S <: T]: TypedRewriter[S] = TypedRewriter[S](rewriter)
}

/**
 * Mix into value classes to provide a custom copy constructor.
 */
trait RewritableUniversal extends Any {
  def dup(children: Seq[AnyRef]): this.type
}

/**
 * Mix into non-value classes to provide a custom copy constructor.
 */
trait Rewritable extends AnyRef with RewritableUniversal

object inSequence {

  private class InSequenceRewriter(rewriters: Seq[Rewriter]) extends Rewriter {

    override def apply(that: AnyRef): AnyRef = {
      val it = rewriters.iterator
      // this piece of code is used a lot and has been through profiling
      // please don't just remove it because it is ugly looking
      var result = that
      while (it.hasNext) {
        result = result.rewrite(it.next())
      }

      result
    }
  }

  private class InSequenceRewriterWithCancel(rewriters: Seq[Rewriter], cancellation: CancellationChecker)
      extends Rewriter {

    override def apply(that: AnyRef): AnyRef = {
      val it = rewriters.iterator
      var result = that
      while (it.hasNext) {
        cancellation.throwIfCancelled()
        result = result.rewrite(it.next())
      }

      result
    }
  }

  def apply(rewriters: Rewriter*): Rewriter =
    new InSequenceRewriter(rewriters)

  def apply(cancellation: CancellationChecker)(rewriters: Rewriter*): Rewriter =
    new InSequenceRewriterWithCancel(rewriters, cancellation)
}

trait RewriterStopper {
  def shouldStop(a: AnyRef): Boolean
}

object RewriterStopper {
  val neverStop: RewriterStopper = _ => false
}

object topDown {

  private class TopDownRewriter(
    rewriter: Rewriter,
    stopper: RewriterStopper,
    leftToRight: Boolean,
    cancellation: CancellationChecker
  ) extends Rewriter {

    override def apply(that: AnyRef): AnyRef = {
      val initialStack = mutable.Stack((List(that), new mutable.ListBuffer[AnyRef]()))
      val result = rec(initialStack)
      assert(result.size == 1)
      result.head
    }

    @tailrec
    private def rec(stack: mutable.Stack[(List[AnyRef], mutable.ListBuffer[AnyRef])]): mutable.ListBuffer[AnyRef] = {
      cancellation.throwIfCancelled()
      val (currentJobs, _) = stack.top
      if (currentJobs.isEmpty) {
        val (_, newChildren) = stack.pop()
        if (stack.isEmpty) {
          newChildren
        } else {
          stack.pop() match {
            case (job :: jobs, doneJobs) =>
              val args = if (leftToRight) newChildren.toSeq else newChildren.reverse.toSeq
              val doneJob = Rewritable.dupAny(job, args)
              stack.push((jobs, doneJobs += doneJob))
              rec(stack)
            case _ => throw new IllegalStateException("Empty job")
          }
        }
      } else {
        stack.pop() match {
          case (newJob :: jobs, doneJobs) =>
            if (stopper.shouldStop(newJob)) {
              stack.push((jobs, doneJobs += newJob))
            } else {
              val rewrittenJob = newJob.rewrite(rewriter)
              stack.push((rewrittenJob :: jobs, doneJobs))
              val newJobs =
                if (leftToRight) rewrittenJob.treeChildren.toList else rewrittenJob.reverseTreeChildren.toList
              stack.push((newJobs, new mutable.ListBuffer()))
            }
            rec(stack)
          case _ => throw new IllegalStateException("Empty job")
        }
      }
    }
  }

  def apply(
    rewriter: Rewriter,
    stopper: RewriterStopper = RewriterStopper.neverStop,
    leftToRight: Boolean = true,
    cancellation: CancellationChecker = CancellationChecker.NeverCancelled
  ): Rewriter =
    new TopDownRewriter(rewriter, stopper, leftToRight, cancellation)
}

trait RewriterStopperWithParent {
  def shouldStop(a: AnyRef, parent: Option[AnyRef]): Boolean
}

object RewriterStopperWithParent {
  val neverStop: RewriterStopperWithParent = (_, _) => false

  def apply(rewriterStopper: RewriterStopper): RewriterStopperWithParent =
    (a: AnyRef, _: Option[AnyRef]) => rewriterStopper.shouldStop(a)
}

/**
 * Top-down rewriter that also lets the rules see the parent of each node as additional context
 */
object topDownWithParent {

  private class TopDownWithParentRewriter(
    rewriter: RewriterWithParent,
    stopper: RewriterStopperWithParent,
    cancellation: CancellationChecker
  ) extends Rewriter {

    override def apply(that: AnyRef): AnyRef = {
      val initialStack = mutable.Stack((List(that), new ListBuffer[AnyRef]()))
      val result = rec(initialStack)
      assert(result.size == 1)
      result.head
    }

    @tailrec
    private def rec(stack: mutable.Stack[(List[AnyRef], mutable.ListBuffer[AnyRef])]): mutable.ListBuffer[AnyRef] = {
      cancellation.throwIfCancelled()
      val (currentJobs, _) = stack.top
      if (currentJobs.isEmpty) {
        val (_, newChildren) = stack.pop()
        if (stack.isEmpty) {
          newChildren
        } else {
          stack.pop() match {
            case (job :: jobs, doneJobs) =>
              val doneJob = Rewritable.dupAny(job, newChildren.toSeq)
              stack.push((jobs, doneJobs += doneJob))
              rec(stack)
            case _ => throw new IllegalStateException(s"Empty job")
          }
        }
      } else {
        stack.pop() match {
          case (newJob :: jobs, doneJobs) =>
            val maybeParent = {
              if (stack.isEmpty) {
                None
              } else {
                val (parentJobs, _) = stack.top
                parentJobs.headOption
              }
            }
            if (stopper.shouldStop(newJob, maybeParent)) {
              stack.push((jobs, doneJobs += newJob))
            } else {
              val rewrittenJob = newJob.rewrite(rewriter, maybeParent)
              stack.push((rewrittenJob :: jobs, doneJobs))
              stack.push((rewrittenJob.treeChildren.toList, new ListBuffer()))
            }
            rec(stack)
          case _ => throw new IllegalStateException("Empty jobs")
        }
      }
    }
  }

  def apply(
    rewriter: RewriterWithParent,
    stopper: RewriterStopperWithParent = RewriterStopperWithParent.neverStop,
    cancellation: CancellationChecker = CancellationChecker.NeverCancelled
  ): Rewriter =
    new TopDownWithParentRewriter(rewriter, stopper, cancellation)
}

object bottomUp {

  private class BottomUpRewriter(rewriter: Rewriter, stopper: RewriterStopper, cancellation: CancellationChecker)
      extends Rewriter {

    override def apply(that: AnyRef): AnyRef = {
      val initialStack = mutable.Stack((List(that), new ListBuffer[AnyRef]()))
      val result = rec(initialStack)
      assert(result.size == 1)
      result.head
    }

    @tailrec
    private def rec(stack: mutable.Stack[(List[AnyRef], mutable.ListBuffer[AnyRef])]): mutable.ListBuffer[AnyRef] = {
      cancellation.throwIfCancelled()
      val (currentJobs, _) = stack.top
      if (currentJobs.isEmpty) {
        val (_, newChildren) = stack.pop()
        if (stack.isEmpty) {
          newChildren
        } else {
          stack.pop() match {
            case (job :: jobs, doneJobs) =>
              val doneJob = Rewritable.dupAny(job, newChildren.toSeq)
              val rewrittenDoneJob = doneJob.rewrite(rewriter)
              stack.push((jobs, doneJobs += rewrittenDoneJob))
              rec(stack)
            case _ => throw new IllegalStateException("No jobs")
          }
        }
      } else {
        val next = currentJobs.head
        if (stopper.shouldStop(next)) {
          stack.pop() match {
            case (job :: jobs, doneJobs) => stack.push((jobs, doneJobs += job))
            case _                       => throw new IllegalStateException("No jobs")
          }
        } else {
          stack.push((next.treeChildren.toList, new ListBuffer()))
        }
        rec(stack)
      }
    }
  }

  def apply(
    rewriter: Rewriter,
    stopper: RewriterStopper = RewriterStopper.neverStop,
    cancellation: CancellationChecker = CancellationChecker.NeverCancelled
  ): Rewriter =
    new BottomUpRewriter(rewriter, stopper, cancellation)
}

object bottomUpWithParent {

  private class BottomUpWithParentRewriter(
    rewriter: RewriterWithParent,
    stopper: AnyRef => Boolean,
    cancellation: CancellationChecker
  ) extends Rewriter {

    override def apply(that: AnyRef): AnyRef = {
      val initialStack = mutable.Stack((List(that), new ListBuffer[AnyRef]()))
      val result = rec(initialStack)
      assert(result.size == 1)
      result.head
    }

    @tailrec
    private def rec(stack: mutable.Stack[(List[AnyRef], mutable.ListBuffer[AnyRef])]): mutable.ListBuffer[AnyRef] = {
      cancellation.throwIfCancelled()
      val (currentJobs, _) = stack.top
      if (currentJobs.isEmpty) {
        val (_, newChildren) = stack.pop()
        if (stack.isEmpty) {
          newChildren
        } else {
          stack.pop() match {
            case (job :: jobs, doneJobs) =>
              val doneJob = Rewritable.dupAny(job, newChildren.toSeq)
              val maybeParent = {
                if (stack.isEmpty) {
                  None
                } else {
                  val (parentJobs, _) = stack.top
                  parentJobs.headOption
                }
              }
              val rewrittenDoneJob = doneJob.rewrite(rewriter, maybeParent)
              stack.push((jobs, doneJobs += rewrittenDoneJob))
              rec(stack)
            case _ => throw new IllegalStateException("No jobs")
          }
        }
      } else {
        val next = currentJobs.head
        if (stopper(next)) {
          stack.pop() match {
            case (job :: jobs, doneJobs) => stack.push((jobs, doneJobs += job))
            case _                       => throw new IllegalStateException("No jobs")
          }
        } else {
          stack.push((next.treeChildren.toList, new ListBuffer()))
        }
        rec(stack)
      }
    }
  }

  def apply(
    rewriter: RewriterWithParent,
    stopper: AnyRef => Boolean = _ => false,
    cancellation: CancellationChecker = CancellationChecker.NeverCancelled
  ): Rewriter =
    new BottomUpWithParentRewriter(rewriter, stopper, cancellation)
}

object bottomUpWithRecorder {

  private class BottomUpRewriter(
    rewriter: Rewriter,
    stopper: RewriterStopper,
    recorder: (AnyRef, AnyRef) => Unit,
    cancellation: CancellationChecker
  ) extends Rewriter {

    override def apply(that: AnyRef): AnyRef = {
      val initialStack = mutable.Stack((List(that), new ListBuffer[AnyRef]()))
      val result = rec(initialStack)
      assert(result.size == 1)
      result.head
    }

    @tailrec
    private def rec(stack: mutable.Stack[(List[AnyRef], mutable.ListBuffer[AnyRef])]): mutable.ListBuffer[AnyRef] = {
      cancellation.throwIfCancelled()
      val (currentJobs, _) = stack.top
      if (currentJobs.isEmpty) {
        val (_, newChildren) = stack.pop()
        if (stack.isEmpty) {
          newChildren
        } else {
          stack.pop() match {
            case (job :: jobs, doneJobs) =>
              val doneJob = Rewritable.dupAny(job, newChildren.toSeq)
              val rewrittenDoneJob = doneJob.rewrite(rewriter)
              if (!(doneJob eq rewrittenDoneJob))
                recorder(doneJob, rewrittenDoneJob)
              stack.push((jobs, doneJobs += rewrittenDoneJob))
              rec(stack)
            case _ => throw new IllegalStateException("Empty jobs")
          }
        }
      } else {
        val next = currentJobs.head
        if (stopper.shouldStop(next)) {
          stack.pop() match {
            case (job :: jobs, doneJobs) => stack.push((jobs, doneJobs += job))
            case _                       => throw new IllegalStateException("Empty jobs")
          }
        } else {
          stack.push((next.treeChildren.toList, new ListBuffer()))
        }
        rec(stack)
      }
    }
  }

  def apply(
    rewriter: Rewriter,
    stopper: RewriterStopper = RewriterStopper.neverStop,
    recorder: (AnyRef, AnyRef) => Unit = (_, _) => (),
    cancellation: CancellationChecker = CancellationChecker.NeverCancelled
  ): Rewriter =
    new BottomUpRewriter(rewriter, stopper, recorder, cancellation)
}
