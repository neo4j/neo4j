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

import java.lang.reflect.Method
import scala.collection.mutable.{HashMap => MutableHashMap}
import scala.collection.{GenIterable, IterableLike}

object Rewriter {
  implicit class LiftedRewriter(f: (AnyRef => Option[AnyRef])) extends Rewriter {
    def apply(that: AnyRef): Option[AnyRef] = f.apply(that)
  }
  def lift(f: PartialFunction[AnyRef, AnyRef]): Rewriter = f.lift
}

trait Rewriter extends (AnyRef => Option[AnyRef])


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
    def dup(rewriter: AnyRef => AnyRef): AnyRef = that match {
      case p: Product with AnyRef =>
        val rewrittenChildren = p.productIterator.asInstanceOf[Iterator[AnyRef]].map(rewriter).toList
        if (p.productIterator.asInstanceOf[Iterator[AnyRef]] eqElements rewrittenChildren.iterator)
          p
        else
          p.dup(rewrittenChildren).asInstanceOf[AnyRef]
      case s: IndexedSeq[_] =>
        s.asInstanceOf[IndexedSeq[AnyRef]].map(rewriter)
      case s: Seq[_] =>
        s.asInstanceOf[Seq[AnyRef]].map(rewriter)
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
        copyConstructor.invoke(product, children.toSeq: _*).asInstanceOf[Product]
    }

    def copyConstructor: Method = {
      val productClass = product.getClass
      productCopyConstructors.get.getOrElseUpdate(productClass, productClass.getMethods.find(_.getName == "copy").get)
    }
  }

  implicit class RewritableAny(val that: AnyRef) extends AnyVal {
    def rewrite(rewriter: Rewriter): AnyRef = rewriter.apply(that).getOrElse(that)
  }
}

trait Rewritable {
  def dup(children: Seq[AnyRef]): this.type
}

case class topDown(rewriters: Rewriter*) extends Rewriter {
  import Rewritable._
  def apply(that: AnyRef): Some[AnyRef] = {
    val rewrittenThat = rewriters.foldLeft(that) {
      (t, r) => t.rewrite(r)
    }
    Some(rewrittenThat.dup(t => this.apply(t).get))
  }
}

case class untilMatched(rewriter: Rewriter) extends Rewriter {
  import Rewritable._
  def apply(that: AnyRef): Some[AnyRef] =
    Some(rewriter.apply(that).getOrElse(that.dup(t => this.apply(t).get)))
}

case class bottomUp(rewriters: Rewriter*) extends Rewriter {
  import Rewritable._
  def apply(that: AnyRef): Some[AnyRef] =
    Some(rewriters.foldLeft(that.dup(t => this.apply(t).get)) {
      (t, r) => t.rewrite(r)
    })
}

case class bottomUpRepeated(rewriter: Rewriter) extends Rewriter {
  import Rewritable._
  def apply(that: AnyRef): Some[AnyRef] = {
    val rewrittenThat = that.dup(t => this.apply(t).get)
    rewriter.apply(rewrittenThat).fold(Some(rewrittenThat)) {
      t => if (t eq that)
        Some(t)
      else
        Some(t.rewrite(this))
    }
  }
}

case class repeat(rewriter: Rewriter) extends Rewriter {
  import Rewritable._
  def apply(that: AnyRef): Option[AnyRef] =
    rewriter.apply(that).map {
      t => if (t eq that)
        that
      else
        t.rewrite(this)
    }
}
