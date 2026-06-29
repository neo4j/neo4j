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

import java.lang.FunctionalInterface

import scala.annotation.tailrec
import scala.collection.IterableFactory
import scala.collection.immutable.ArraySeq
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

@FunctionalInterface
trait Rewriter extends (AnyRef => AnyRef)

object Rewriter {

  def apply(f: AnyRef => AnyRef): Rewriter =
    new Rewriter {
      override def apply(v1: AnyRef): AnyRef = f(v1)
    }

  implicit def fromFunction(f: AnyRef => AnyRef): Rewriter = Rewriter(f)

  /**
   * JVM return type must be [[scala.Function1]] (erased `Lscala/Function1`) so bytecode stays link‑compatible with
   * Scala 2.13 front‑end jars and TeaVM (`Rewriter$.lift(...)Lscala/Function1;`). The returned value still implements
   * [[Rewriter]] at runtime.
   */
  def lift(f: PartialFunction[AnyRef, AnyRef]): scala.Function1[AnyRef, AnyRef] =
    new Rewriter {
      override def apply(v1: AnyRef): AnyRef = f.applyOrElse(v1, identity[AnyRef])
    }

  /** Bridges [[scala.Function1]] to [[Rewriter]] for APIs that still use erased `Function1` in JVM descriptors. */
  def fromFunction1(f: scala.Function1[AnyRef, AnyRef]): Rewriter =
    f match {
      case r: Rewriter => r
      case _           => Rewriter(f.apply)
    }

  val noop: Rewriter = Rewriter(identity[AnyRef])

  trait TopDownMergeableRewriter {
    def innerRewriter: Rewriter
  }

  trait BottomUpMergeableRewriter {
    def innerRewriter: Rewriter
  }

  def mergeTopDown(rewriters: TopDownMergeableRewriter*): Rewriter =
    RewriterMergeHelpers.mergeTopDown(rewriters)

  def mergeBottomUp(rewriters: BottomUpMergeableRewriter*): Rewriter =
    RewriterMergeHelpers.mergeBottomUp(rewriters)
}

@FunctionalInterface
trait RewriterWithParent extends (((AnyRef, Option[AnyRef])) => AnyRef)

object RewriterWithParent {

  def apply(f: ((AnyRef, Option[AnyRef])) => AnyRef): RewriterWithParent =
    new RewriterWithParent {
      override def apply(v1: (AnyRef, Option[AnyRef])): AnyRef = f(v1)
    }

  implicit def fromFunction(f: ((AnyRef, Option[AnyRef])) => AnyRef): RewriterWithParent = RewriterWithParent(f)

  def lift(f: PartialFunction[(AnyRef, Option[AnyRef]), AnyRef]): RewriterWithParent =
    new RewriterWithParent {
      override def apply(v1: (AnyRef, Option[AnyRef])): AnyRef = f.applyOrElse(v1, { case (x, _) => x })
    }
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
        val dis = iterator.next()
        val dat = that.next()
        val same = dis match {
          case v: java.lang.Integer   => v == dat
          case v: java.lang.Long      => v == dat
          case v: java.lang.Double    => v == dat
          case v: java.lang.Float     => v == dat
          case v: java.lang.Short     => v == dat
          case v: java.lang.Byte      => v == dat
          case v: java.lang.Character => v == dat
          case v: java.lang.Boolean   => v == dat
          case _                      => dis eq dat
        }
        if (!same)
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
          case _: scala.collection.immutable.ArraySeq[_] =>
            ArraySeq.from(children)
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

    /**
     * Single [[scala.Function1]] parameter keeps JVM bytecode compatible with both Scala 2.13 (erased
     * `Rewriter` alias) and Scala 3 ([[Rewriter]] extends `Function1`).
     */
    def rewrite(rewriter: scala.Function1[AnyRef, AnyRef]): AnyRef =
      Rewriter.fromFunction1(rewriter).apply(that)

    def rewrite(rewriter: RewriterWithParent, parent: Option[AnyRef]): AnyRef = {
      val result = rewriter.apply((that, parent))
      result
    }

    /** JVM `endoRewrite$extension(…, Rewriter)` is added by buildtools; no Scala `Rewriter` overload (subtyping clash). */
    def endoRewrite(rewriter: scala.Function1[AnyRef, AnyRef]): T =
      rewrite(rewriter).asInstanceOf[T]
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

  def stopOn[T <: AnyRef](implicit ct: ClassTag[T]): RewriterStopper = {
    val stopOnClass = ct.runtimeClass
    value => stopOnClass.isAssignableFrom(value.getClass)
  }
}

object topDown {

  private def applyImpl(
    rewriter: Rewriter,
    stopper: RewriterStopper,
    leftToRight: Boolean,
    cancellation: CancellationChecker
  ): Rewriter =
    new TopDownRewriter(rewriter, stopper, leftToRight, cancellation)

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

  /** No default parameters (JVM bridge for older call sites); avoids Scala 3 default-accessor clashes with the [[scala.Function1]] overload. */
  def apply(
    rewriter: Rewriter,
    stopper: RewriterStopper,
    leftToRight: Boolean,
    cancellation: CancellationChecker
  ): Rewriter =
    applyImpl(rewriter, stopper, leftToRight, cancellation)

  def apply(
    rewriter: scala.Function1[AnyRef, AnyRef],
    stopper: RewriterStopper = RewriterStopper.neverStop,
    leftToRight: Boolean = true,
    cancellation: CancellationChecker = CancellationChecker.NeverCancelled
  ): scala.Function1[AnyRef, AnyRef] =
    applyImpl(Rewriter.fromFunction1(rewriter), stopper, leftToRight, cancellation)

  /** Prefer over [[apply]] when Scala would otherwise not choose between the [[Rewriter]] and [[scala.Function1]] overloads. */
  def onRewriter(
    rewriter: Rewriter,
    stopper: RewriterStopper = RewriterStopper.neverStop,
    leftToRight: Boolean = true,
    cancellation: CancellationChecker = CancellationChecker.NeverCancelled
  ): Rewriter =
    applyImpl(rewriter, stopper, leftToRight, cancellation)
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

  private def applyImpl(
    rewriter: Rewriter,
    stopper: RewriterStopper,
    cancellation: CancellationChecker
  ): Rewriter =
    new BottomUpRewriter(rewriter, stopper, cancellation)

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
    stopper: RewriterStopper,
    cancellation: CancellationChecker
  ): Rewriter =
    applyImpl(rewriter, stopper, cancellation)

  def apply(
    rewriter: scala.Function1[AnyRef, AnyRef],
    stopper: RewriterStopper = RewriterStopper.neverStop,
    cancellation: CancellationChecker = CancellationChecker.NeverCancelled
  ): scala.Function1[AnyRef, AnyRef] =
    applyImpl(Rewriter.fromFunction1(rewriter), stopper, cancellation)

  def onRewriter(
    rewriter: Rewriter,
    stopper: RewriterStopper = RewriterStopper.neverStop,
    cancellation: CancellationChecker = CancellationChecker.NeverCancelled
  ): Rewriter =
    applyImpl(rewriter, stopper, cancellation)
}

object bottomUpWithParent {

  private class BottomUpWithParentRewriter(
    rewriter: RewriterWithParent,
    stopper: RewriterStopper,
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
    rewriter: RewriterWithParent,
    stopper: RewriterStopper = RewriterStopper.neverStop,
    cancellation: CancellationChecker
  ): Rewriter =
    new BottomUpWithParentRewriter(rewriter, stopper, cancellation)
}

object bottomUpWithRecorder {

  private val noopRecorder: (AnyRef, AnyRef) => Unit = (a: AnyRef, b: AnyRef) => ()

  private def applyImpl(
    rewriter: Rewriter,
    stopper: RewriterStopper,
    recorder: (AnyRef, AnyRef) => Unit,
    cancellation: CancellationChecker
  ): Rewriter =
    new BottomUpRewriter(rewriter, stopper, recorder, cancellation)

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
    stopper: RewriterStopper,
    recorder: (AnyRef, AnyRef) => Unit,
    cancellation: CancellationChecker
  ): Rewriter =
    applyImpl(rewriter, stopper, recorder, cancellation)

  def apply(
    rewriter: scala.Function1[AnyRef, AnyRef],
    stopper: RewriterStopper = RewriterStopper.neverStop,
    recorder: (AnyRef, AnyRef) => Unit = noopRecorder,
    cancellation: CancellationChecker
  ): scala.Function1[AnyRef, AnyRef] =
    applyImpl(Rewriter.fromFunction1(rewriter), stopper, recorder, cancellation)

  def onRewriter(
    rewriter: Rewriter,
    stopper: RewriterStopper = RewriterStopper.neverStop,
    recorder: (AnyRef, AnyRef) => Unit = noopRecorder,
    cancellation: CancellationChecker
  ): Rewriter =
    applyImpl(rewriter, stopper, recorder, cancellation)
}

private object RewriterMergeHelpers {

  def mergeTopDown(rewriters: Seq[Rewriter.TopDownMergeableRewriter]): Rewriter = {
    new Rewriter {
      override def apply(v1: AnyRef): AnyRef = instance.apply(v1)

      private val instance =
        topDown.onRewriter(Rewriter(rewriters.map(_.innerRewriter).reduce(_ andThen _)))
    }
  }

  def mergeBottomUp(rewriters: Seq[Rewriter.BottomUpMergeableRewriter]): Rewriter = {
    new Rewriter {
      override def apply(v1: AnyRef): AnyRef = instance.apply(v1)

      private val instance =
        bottomUp.onRewriter(Rewriter(rewriters.map(_.innerRewriter).reduce(_ andThen _)))
    }
  }
}
