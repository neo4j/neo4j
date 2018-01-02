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

import java.lang.reflect.Method


import scala.annotation.tailrec
import scala.collection.mutable.{HashMap => MutableHashMap}
import org.neo4j.cypher.internal.frontend.v2_3.Foldable._
import org.neo4j.cypher.internal.frontend.v2_3.Rewritable._

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

    def dup(children: Seq[AnyRef]): AnyRef = that match {
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

  class InSequenceRewriter(rewriters: Seq[Rewriter]) extends Rewriter {
    def apply(that: AnyRef): AnyRef = {
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

  def apply(rewriters: Rewriter*) = new InSequenceRewriter(rewriters)
}

object topDown {

  class TopDownRewriter(rewriter: Rewriter) extends Rewriter {
    def apply(that: AnyRef): AnyRef = {
      val rewrittenThat = that.rewrite(rewriter)
      //this piece of code is used a lot and has been through profiling
      //please don't just remove it because it is ugly looking
      val children = rewrittenThat.children.toList
      val buffer = new Array[AnyRef](children.size)
      val it = children.iterator
      var index = 0
      while (it.hasNext) {
        buffer(index) = apply(it.next())
        index += 1
      }

      rewrittenThat.dup(buffer)
    }
  }

  def apply(rewriter: Rewriter) = new TopDownRewriter(rewriter)
}

object bottomUp {

  class BottomUpRewriter(val rewriter: Rewriter) extends Rewriter {
    def apply(that: AnyRef): AnyRef = {
      //this piece of code is used a lot and has been through profiling
      //please don't just remove it because it is ugly looking
      val children = that.children.toList
      val buffer = new Array[AnyRef](children.size)
      val it = children.iterator
      var index = 0
      while (it.hasNext) {
        buffer(index) = apply(it.next())
        index += 1
      }

      val rewrittenThat = that.dup(buffer)
      rewriter.apply(rewrittenThat)
    }
  }

  def apply(rewriter: Rewriter) = new BottomUpRewriter(rewriter)
}

trait Replacer {
  def expand(replacement: AnyRef): AnyRef
  def stop(replacement: AnyRef): AnyRef
}

case class replace(strategy: (Replacer => (AnyRef => AnyRef))) extends Rewriter {

  self =>

  private val cont = strategy(new Replacer {
    def expand(replacement: AnyRef) = {
      //this piece of code is used a lot and has been through profiling
      //please don't just remove it because it is ugly looking
      val children = replacement.children.toList
      val buffer = new Array[AnyRef](children.size)
      val it = children.iterator
      var index = 0
      while (it.hasNext) {
        buffer(index) = apply(it.next())
        index += 1
      }

      replacement.dup(buffer)
    }

    def stop(replacement: AnyRef) = replacement
  })

  def apply(that: AnyRef): AnyRef = cont(that)
}

case class repeat(rewriter: Rewriter) extends Rewriter {
  @tailrec
  final def apply(that: AnyRef): AnyRef = {
    val t = rewriter.apply(that)
    if (t == that) {
      t
    } else {
      apply(t)
    }
  }
}
